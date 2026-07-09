# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Workflow-aware worker (R3 redesign).

Provides :class:`WorkflowShellCommandProcessor`, a drop-in replacement for
:class:`ai4s.jobq.work.ShellCommandProcessor` that:

* Recognises workflow metadata kwargs injected by the new coordinator
  (``__workflow_id`` / ``__workflow_task`` / ``__attempt_no`` /
  ``__upstream_outputs_compact``).  The legacy uncompacted form
  ``__upstream_output_refs`` is still accepted for back-compat with
  in-flight messages from older coordinators.
* Runs the shell command, captures the task's structured output, and
  publishes a :class:`~ai4s.jobq.workflow.entities.WorkflowCompletion`
  to the workflow's completion queue.
* Polls the persistence layer's cancel flag while the subprocess is
  running and SIGTERM-s the pool on a cancel request.
* Leaves retry decisions to the coordinator.  A completion (success or
  failure) is published on every attempt; the coordinator rearms the
  task or marks it FAILED based on ``WorkflowCompletion.attempt_no``.

The shape is intentionally minimal compared to the legacy worker
(``worker_legacy.py``):

* No per-task Table reads / writes.
* No CAS decrement of a retries-remaining counter.
* No ``RetryableTaskFailure`` raised on user-task failure — we always
  publish a completion.  ``RetryableTaskFailure`` *is* still raised
  when the completion push itself fails, so jobq redelivers the task
  message and the next attempt republishes.  The coordinator
  idempotency-filters duplicate completions via ``attempt_no``.

For scripts running inside the subprocess::

    from ai4s.jobq.workflow.worker import get_workflow_context

    ctx = get_workflow_context()
    async with ctx:
        upstream = await ctx.get_upstream_output("preprocess")

Or use the synchronous helpers in
:mod:`ai4s.jobq.workflow.context`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any

from ai4s.jobq.work import ShellCommandProcessor
from ai4s.jobq.workflow._compact_refs import pop_upstream_refs as _pop_upstream_refs

if TYPE_CHECKING:
    from ai4s.jobq.jobq import JobQ
    from ai4s.jobq.workflow.env import WorkflowEnv
    from ai4s.jobq.workflow.persistence import WorkflowPersistence

LOG = logging.getLogger(__name__)


class WorkflowShellCommandProcessor(ShellCommandProcessor):
    """ShellCommandProcessor that emits workflow completions automatically.

    Constructed by the worker entry point when ``JOBQ_WORKFLOW_PREFIX`` is
    set in the environment.  Reads workflow-routing kwargs from each
    incoming task message, runs the shell command, then publishes a
    completion to the workflow's completion queue.
    """

    _env: WorkflowEnv
    _completion_queue: str
    _completion_jobq: JobQ | None
    _persistence: WorkflowPersistence | None

    def __init__(
        self,
        num_workers: int = 1,
        emulate_tty: bool = False,
        *,
        completion_queue: str | None = None,
    ) -> None:
        super().__init__(num_workers=num_workers, emulate_tty=emulate_tty)
        from ai4s.jobq.workflow.env import WorkflowEnv, WorkflowEnvError
        from ai4s.jobq.workflow.ids import completion_queue_name

        try:
            self._env = WorkflowEnv.from_environ()
        except WorkflowEnvError as exc:
            raise ValueError(
                f"Cannot initialise WorkflowShellCommandProcessor: {exc}\n"
                "Set JOBQ_WORKFLOW_PREFIX=<account>/<prefix> before launching the worker."
            ) from exc
        self._completion_queue = (
            completion_queue
            if completion_queue is not None
            else completion_queue_name(self._env.prefix)
        )
        self._completion_jobq = None
        self._persistence = None
        LOG.info(
            "WorkflowShellCommandProcessor ready (prefix=%s completion_queue=%s)",
            self._env.prefix,
            self._completion_queue,
        )

    # ------------------------------------------------------------------
    # Lazy backend handles (one per worker process, cached for the
    # process lifetime via ``self.stack``)
    # ------------------------------------------------------------------

    async def _get_completion_jobq(self) -> JobQ:
        if self._completion_jobq is None:
            from ai4s.jobq.workflow._queues import open_jobq

            self._completion_jobq = await self.stack.enter_async_context(
                open_jobq(self._completion_queue, queues_account=self._env.queues)
            )
        return self._completion_jobq

    async def _get_persistence(self) -> WorkflowPersistence:
        if self._persistence is None:
            from ai4s.jobq.workflow.persistence import WorkflowPersistence

            self._persistence = await self.stack.enter_async_context(
                await WorkflowPersistence.from_account(
                    self._env.state_account, prefix=self._env.prefix
                )
            )
        return self._persistence

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def __call__(
        self,
        cmd: str,
        _job_id: str,
        bg_dirsync_to: str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        _log_dimensions: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> int:
        workflow_id = kwargs.pop("__workflow_id", None)
        task_name = kwargs.pop("__workflow_task", None)
        attempt_no = kwargs.pop("__attempt_no", None)
        # Accept either the compact or legacy upstream-refs payload.
        # ``pop_upstream_refs`` consumes both keys so they never leak
        # into the kwargs forwarded to the subprocess wrapper.
        upstream_refs: dict[str, str | None] = _pop_upstream_refs(workflow_id or "", kwargs)

        # Non-workflow tasks: pass-through to plain shell.
        if not (workflow_id and task_name):
            return await super().__call__(
                cmd=cmd,
                _job_id=_job_id,
                bg_dirsync_to=bg_dirsync_to,
                env=env,
                cwd=cwd,
                _log_dimensions=_log_dimensions,
                **kwargs,
            )

        return await self._process_workflow_task(
            cmd=cmd,
            job_id=_job_id,
            bg_dirsync_to=bg_dirsync_to,
            env=env,
            cwd=cwd,
            workflow_id=workflow_id,
            task_name=task_name,
            attempt_no=attempt_no,
            upstream_refs=upstream_refs,
        )

    async def _process_workflow_task(
        self,
        *,
        cmd: str,
        job_id: str,
        bg_dirsync_to: str | None,
        env: dict[str, str] | None,
        cwd: str | None,
        workflow_id: str,
        task_name: str,
        attempt_no: int | None,
        upstream_refs: dict[str, str | None],
    ) -> int:
        from ai4s.jobq.workflow.entities import WorkflowCompletion

        # Build subprocess env and per-task sidecar files.
        sub_env = self._prepare_workflow_env(env, workflow_id, task_name, attempt_no)
        upstream_refs_file = self._write_upstream_refs(sub_env, upstream_refs)
        output_file = self._create_output_file(sub_env)

        # Background cancel poller — terminates the subprocess pool on
        # ``cancel_requested``.
        cancel_task = asyncio.create_task(
            self._poll_cancellation(workflow_id),
            name=f"cancel-poll-{workflow_id}/{task_name}",
        )

        success = True
        error: str | None = None
        ret_code = 0
        try:
            ret_code = await super().__call__(
                cmd=cmd,
                _job_id=job_id,
                bg_dirsync_to=bg_dirsync_to,
                env=sub_env,
                cwd=cwd,
                _log_dimensions={
                    "workflow_id": workflow_id,
                    "task_name": task_name,
                    "attempt_no": str(attempt_no) if attempt_no is not None else "?",
                },
            )
        except RuntimeError as exc:
            # ShellCommandProcessor raises RuntimeError on non-zero
            # ret code — translate to a structured failure.
            success, error, ret_code = False, str(exc), 1
        except Exception as exc:
            success, error, ret_code = False, f"{type(exc).__name__}: {exc}", 1
            LOG.exception("Unexpected exception running %s/%s", workflow_id, task_name)
        finally:
            if not cancel_task.done():
                cancel_task.cancel()
            with contextlib.suppress(BaseException):
                await cancel_task
            # Upstream-refs file is only used by the subprocess; output
            # file is consumed by ``_collect_output`` below (it deletes
            # the file itself after reading).
            if upstream_refs_file:
                with contextlib.suppress(OSError):
                    os.unlink(upstream_refs_file)

        # Note: we re-open output_file via _collect_output (it cleans
        # up after itself) — pass the path it was created with.
        output_ref = await self._collect_output(output_file, workflow_id, task_name)

        completion = WorkflowCompletion(
            workflow_id=workflow_id,
            task_name=task_name,
            success=success,
            output_ref=output_ref,
            error=error,
            attempt_no=attempt_no,
        )
        await self._publish_completion(completion)
        # Always return 0 so jobq deletes the task message — the
        # completion is on the wire, retries are the coordinator's job.
        # (Logging the original ret_code for observability.)
        if ret_code != 0:
            LOG.info(
                "Workflow task %s/%s failed (ret_code=%d, attempt=%s); "
                "completion published, coordinator decides retry",
                workflow_id,
                task_name,
                ret_code,
                attempt_no,
            )
        return 0

    # ------------------------------------------------------------------
    # Subprocess env preparation
    # ------------------------------------------------------------------

    @staticmethod
    def _prepare_workflow_env(
        env: dict[str, str] | None,
        workflow_id: str,
        task_name: str,
        attempt_no: int | None,
    ) -> dict[str, str]:
        sub_env: dict[str, str] = dict(env) if env else {}
        sub_env["JOBQ_WORKFLOW_ID"] = workflow_id
        sub_env["JOBQ_WORKFLOW_TASK"] = task_name
        if attempt_no is not None:
            sub_env["JOBQ_WORKFLOW_ATTEMPT"] = str(attempt_no)
        for var in ("JOBQ_WORKFLOW_PREFIX", "JOBQ_WORKFLOW_QUEUES", "JOBQ_WORKFLOW_BLOBS"):
            val = os.environ.get(var)
            if val and var not in sub_env:
                sub_env[var] = val
        return sub_env

    @staticmethod
    def _write_upstream_refs(
        env: dict[str, str],
        upstream_refs: dict[str, str | None],
    ) -> str | None:
        """Stash upstream output refs in a tempfile readable by user scripts.

        Always written (even when empty) so user scripts can
        unconditionally read the file via the env var.
        """
        fd, path = tempfile.mkstemp(suffix=".json", prefix="jobq_upstream_refs_")
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(upstream_refs, fh)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(path)
            raise
        env["JOBQ_WORKFLOW_UPSTREAM_REFS"] = path
        return path

    @staticmethod
    def _create_output_file(env: dict[str, str]) -> str:
        fd, path = tempfile.mkstemp(suffix=".json", prefix="jobq_output_")
        os.close(fd)
        env["JOBQ_OUTPUT_FILE"] = path
        return path

    # ------------------------------------------------------------------
    # Output collection
    # ------------------------------------------------------------------

    async def _collect_output(
        self,
        output_file: str | None,
        workflow_id: str,
        task_name: str,
    ) -> str | None:
        if not output_file:
            return None
        output_ref: str | None = None
        try:
            if not await asyncio.to_thread(os.path.isfile, output_file):
                return None
            size = await asyncio.to_thread(os.path.getsize, output_file)
            if size == 0:
                return None
            output_ref = await asyncio.to_thread(self._read_output_file, output_file)
            output_ref = await self._materialise_pending_stashes(workflow_id, task_name, output_ref)
            from ai4s.jobq.workflow.entities import output_needs_blob

            if output_needs_blob(output_ref):
                try:
                    output_ref = await self._stash_output_to_blob(
                        workflow_id, task_name, output_ref
                    )
                except Exception:
                    LOG.exception(
                        "Blob upload of output for %s/%s failed; discarding",
                        workflow_id,
                        task_name,
                    )
                    output_ref = None
        except (OSError, json.JSONDecodeError) as exc:
            LOG.warning("Could not read output file %s: %s", output_file, exc)
            output_ref = None
        finally:
            with contextlib.suppress(OSError):
                os.unlink(output_file)
        return output_ref

    @staticmethod
    def _read_output_file(path: str) -> str:
        with open(path) as f:
            data = f.read()
        json.loads(data)
        return data

    async def _stash_output_to_blob(
        self,
        workflow_id: str,
        task_name: str,
        data: str,
    ) -> str:
        persistence = await self._get_persistence()
        ref = await persistence.stash_output(workflow_id, task_name, data.encode())
        LOG.info(
            "Stashed output (%d bytes) for %s/%s -> %s",
            len(data),
            workflow_id,
            task_name,
            ref,
        )
        return ref

    async def _materialise_pending_stashes(
        self,
        workflow_id: str,
        task_name: str,
        output_ref: str,
    ) -> str:
        """Walk *output_ref*, upload any pending ``BlobStasher.from_file`` markers.

        Files are uploaded to the *legacy* blob container resolved from
        :class:`WorkflowEnv` (defaults to ``jobq-workflow-data``).
        Downstream :class:`~ai4s.jobq.blob.BlobStash` resolves the same
        container, preserving the user-visible file-stash contract.
        """
        from ai4s.jobq.workflow.stash import STASH_VERSION

        try:
            payload = json.loads(output_ref)
        except json.JSONDecodeError:
            return output_ref

        pending: list[dict[str, Any]] = []
        _collect_pending_stashes(payload, pending)
        if not pending:
            return output_ref

        if not self._env.blob_account:
            raise RuntimeError(
                f"Task output uses BlobStasher.from_file ({len(pending)} file(s)) but "
                "no Blob Storage account is configured. Set JOBQ_WORKFLOW_BLOBS or "
                "ensure JOBQ_WORKFLOW_PREFIX resolves to a usable account."
            )

        from ai4s.jobq.workflow.context import _blob_service_client

        prefix = f"workflow-files/{workflow_id}/{task_name}"
        async with _blob_service_client(self._env.blob_account) as svc:
            container = svc.get_container_client(self._env.blob_container)
            # ``BlobStash`` resolves the container the first time the
            # downstream task reads it; the container is created here
            # on first use so a fresh prefix works out of the box.
            with contextlib.suppress(Exception):
                await container.create_container()
            for marker in pending:
                local_path = marker.get("local_path")
                filename = marker.get("filename") or "file"
                if not local_path or not await asyncio.to_thread(os.path.isfile, local_path):
                    raise FileNotFoundError(
                        f"BlobStasher.from_file({local_path!r}) refers to a missing file"
                    )
                blob_name = f"{prefix}/{filename}"
                md5_hex, size = await asyncio.to_thread(_md5_and_size, local_path)
                blob = container.get_blob_client(blob_name)
                fh = await asyncio.to_thread(open, local_path, "rb")
                try:
                    await blob.upload_blob(fh, overwrite=True, max_concurrency=4)
                finally:
                    await asyncio.to_thread(fh.close)
                LOG.info(
                    "Stashed file output %s (%d bytes) -> blob: %s",
                    local_path,
                    size,
                    blob_name,
                )
                marker.clear()
                marker.update(
                    {
                        "v": STASH_VERSION,
                        "state": "ready",
                        "blob_name": blob_name,
                        "md5": md5_hex,
                        "size": size,
                    }
                )

        leftover: list[dict[str, Any]] = []
        _collect_pending_stashes(payload, leftover)
        if leftover:
            raise RuntimeError(
                f"Internal error: {len(leftover)} pending stash marker(s) remained "
                f"after upload for {workflow_id}/{task_name}"
            )
        rewritten = json.dumps(payload)
        return rewritten

    # ------------------------------------------------------------------
    # Cancel polling
    # ------------------------------------------------------------------

    async def _poll_cancellation(self, workflow_id: str) -> None:
        """Background poller that kills subprocesses on workflow cancel.

        Reads :meth:`WorkflowPersistence.get_cancel_requested` on a
        jittered interval (default 30 s, configurable via
        ``JOBQ_CANCEL_POLL_INTERVAL``). Transient errors back off
        exponentially up to 5 minutes; a successful poll resets to the
        base interval.
        """
        import random

        base_interval = max(1, int(os.environ.get("JOBQ_CANCEL_POLL_INTERVAL", "30")))
        max_interval = max(base_interval * 10, 300)
        current_interval = base_interval

        try:
            persistence = await self._get_persistence()
            while True:
                jitter = current_interval * random.uniform(-0.25, 0.25)  # noqa: S311
                await asyncio.sleep(current_interval + jitter)
                try:
                    cancelled = await asyncio.wait_for(
                        persistence.get_cancel_requested(workflow_id),
                        timeout=min(current_interval, 30),
                    )
                except TimeoutError:
                    LOG.debug("Cancel poll for %s timed out, backing off", workflow_id)
                    current_interval = min(current_interval * 2, max_interval)
                    continue
                except Exception:
                    LOG.debug(
                        "Cancel poll for %s failed, backing off",
                        workflow_id,
                        exc_info=True,
                    )
                    current_interval = min(current_interval * 2, max_interval)
                    continue
                current_interval = base_interval
                if cancelled:
                    LOG.info(
                        "Workflow %s cancelled — sending SIGTERM to subprocesses",
                        workflow_id,
                    )
                    await self.pool.kill_all_subprocesses()
                    return
        except asyncio.CancelledError:
            return
        except Exception:
            LOG.warning(
                "Cancellation poller for %s stopped — cancellations may go unnoticed",
                workflow_id,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Completion publishing
    # ------------------------------------------------------------------

    async def _publish_completion(self, completion: Any) -> None:
        """Push *completion* to the coordinator's completion queue.

        Pushes are tiny and idempotent (coordinator ignores stale
        completions via ``attempt_no``).  We retry with exponential
        back-off (cap 60 s).  If the configured attempt cap is
        exceeded, raise :class:`RetryableTaskFailure` so jobq
        redelivers the task message and a future attempt republishes.
        """
        from tenacity import (
            AsyncRetrying,
            before_sleep_log,
            stop_after_attempt,
            stop_never,
            wait_exponential,
        )

        from ai4s.jobq.entities import RetryableTaskFailure

        body = completion.serialize()
        try:
            max_attempts = int(os.environ.get("JOBQ_COMPLETION_SEND_MAX_ATTEMPTS", "0"))
        except ValueError:
            max_attempts = 0
        stop = stop_after_attempt(max_attempts) if max_attempts > 0 else stop_never

        try:
            async for attempt in AsyncRetrying(
                stop=stop,
                wait=wait_exponential(multiplier=1, min=1, max=60),
                reraise=True,
                before_sleep=before_sleep_log(LOG, logging.WARNING, exc_info=True),
            ):
                with attempt:
                    q = await self._get_completion_jobq()
                    await q.push({"__completion_body": body}, num_retries=0)
        except Exception as exc:
            raise RetryableTaskFailure(
                f"failed to publish workflow completion for "
                f"{completion.workflow_id}/{completion.task_name}: {exc}"
            ) from exc

        n = attempt.retry_state.attempt_number
        if n > 1:
            LOG.info(
                "Published completion %s/%s (success=%s) after %d attempts",
                completion.workflow_id,
                completion.task_name,
                completion.success,
                n,
            )
        else:
            LOG.debug(
                "Published completion %s/%s (success=%s)",
                completion.workflow_id,
                completion.task_name,
                completion.success,
            )


# ---------------------------------------------------------------------------
# Helpers reused from legacy worker
# ---------------------------------------------------------------------------


def _md5_and_size(path: str) -> tuple[str, int]:
    """Stream *path* once to compute its md5 and byte size."""
    import hashlib

    h = hashlib.md5()  # noqa: S324 — integrity check, not security
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _collect_pending_stashes(node: Any, out: list[dict[str, Any]]) -> None:
    """Walk *node*, append every pending ``__jobq_stash__`` marker dict to *out*."""
    from ai4s.jobq.workflow.stash import STASH_MARKER

    if isinstance(node, dict):
        if STASH_MARKER in node and len(node) == 1:
            inner = node[STASH_MARKER]
            if isinstance(inner, dict) and inner.get("state") == "pending":
                out.append(inner)
                return
        for value in node.values():
            _collect_pending_stashes(value, out)
    elif isinstance(node, list):
        for item in node:
            _collect_pending_stashes(item, out)


# ---------------------------------------------------------------------------
# User-script context (lazy)
# ---------------------------------------------------------------------------


def get_workflow_context() -> _LazyWorkflowContext:
    """Return a lazy workflow context for the current task subprocess.

    Reads ``JOBQ_WORKFLOW_ID`` / ``JOBQ_WORKFLOW_TASK`` (set by
    :class:`WorkflowShellCommandProcessor`) and prepares a context
    that lazily opens a :class:`WorkflowPersistence` handle on first
    network call.  Upstream output refs are read from the JSON
    sidecar pointed at by ``JOBQ_WORKFLOW_UPSTREAM_REFS``.

    Typical usage in a Python task script::

        import asyncio
        from ai4s.jobq.workflow.worker import get_workflow_context

        async def main():
            ctx = get_workflow_context()
            async with ctx:
                features = await ctx.get_upstream_output("featurize")
                ...

        asyncio.run(main())

    Raises:
        ValueError: if the workflow env vars are not set.
    """
    workflow_id = os.environ.get("JOBQ_WORKFLOW_ID", "")
    task_name = os.environ.get("JOBQ_WORKFLOW_TASK", "")
    if not workflow_id or not task_name:
        raise ValueError(
            "JOBQ_WORKFLOW_ID and JOBQ_WORKFLOW_TASK must be set. "
            "Are you running inside a WorkflowShellCommandProcessor?"
        )
    return _LazyWorkflowContext(workflow_id, task_name)


class _LazyWorkflowContext:
    """Lazy workflow context backed by :class:`WorkflowPersistence`.

    Opens persistence on first network-using call and caches it for
    subsequent calls.  Closed via ``async with`` / :meth:`close`.
    """

    def __init__(self, workflow_id: str, task_name: str) -> None:
        self.workflow_id = workflow_id
        self.task_name = task_name
        self._persistence: WorkflowPersistence | None = None
        self._upstream_refs_cache: dict[str, str | None] | None = None

    async def _ensure_persistence(self) -> WorkflowPersistence:
        if self._persistence is None:
            from ai4s.jobq.workflow.env import WorkflowEnv
            from ai4s.jobq.workflow.persistence import WorkflowPersistence

            env = WorkflowEnv.from_environ()
            self._persistence = await WorkflowPersistence.from_account(
                env.state_account, prefix=env.prefix
            )
            await self._persistence.__aenter__()
        return self._persistence

    def _load_upstream_refs(self) -> dict[str, str | None]:
        if self._upstream_refs_cache is not None:
            return self._upstream_refs_cache
        path = os.environ.get("JOBQ_WORKFLOW_UPSTREAM_REFS", "")
        if not path or not os.path.isfile(path):
            self._upstream_refs_cache = {}
            return self._upstream_refs_cache
        with open(path) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            self._upstream_refs_cache = {}
            return self._upstream_refs_cache
        self._upstream_refs_cache = data
        return self._upstream_refs_cache

    async def get_upstream_output(self, task_name: str) -> Any:
        """Resolve a direct upstream task's output.

        Returns ``None`` if the upstream produced no output or was
        skipped.  Raises ``KeyError`` if *task_name* is not a direct
        dependency of the current task.
        """
        refs = self._load_upstream_refs()
        if task_name not in refs:
            raise KeyError(
                f"{task_name!r} is not a direct upstream of "
                f"{self.task_name!r} (or refs file missing)"
            )
        ref = refs[task_name]
        if not ref:
            return None
        persistence = await self._ensure_persistence()
        return await persistence.fetch_output(ref)

    async def get_available_upstream_outputs(self) -> dict[str, Any]:
        """Resolve every available direct upstream output.

        Useful with ``dep_policy="any"``: upstream entries with a
        ``None`` ref are skipped (parent failed or was skipped).
        """
        refs = self._load_upstream_refs()
        out: dict[str, Any] = {}
        for name, ref in refs.items():
            if not ref:
                continue
            persistence = await self._ensure_persistence()
            out[name] = await persistence.fetch_output(ref)
        return out

    async def is_cancelled(self) -> bool:
        persistence = await self._ensure_persistence()
        return await persistence.get_cancel_requested(self.workflow_id)

    async def get_real_upstream_tasks(self) -> list[str]:
        """Return the original (non-synthetic) upstream task names.

        See :meth:`ai4s.jobq.workflow.WorkflowContext.get_real_upstream_tasks`
        for full semantics; this is the runtime-load implementation used
        from inside a worker subprocess.
        """
        persistence = await self._ensure_persistence()
        loaded = await persistence.load(self.workflow_id)
        if loaded is None:
            return []
        runtime, _etag = loaded
        ch = runtime.tasks.get(self.task_name)
        if ch is None:
            return []
        real: set[str] = set()
        seen: set[str] = set()
        stack: list[str] = list(ch.parents)
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            task = runtime.tasks.get(n)
            if task is not None and bool(task.kwargs.get("__batch_merge")):
                stack.extend(task.parents)
            else:
                real.add(n)
        return sorted(real)

    async def close(self) -> None:
        if self._persistence is not None:
            with contextlib.suppress(Exception):
                await self._persistence.__aexit__(None, None, None)
            self._persistence = None

    async def __aenter__(self) -> _LazyWorkflowContext:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()


__all__ = [
    "WorkflowShellCommandProcessor",
    "get_workflow_context",
]
