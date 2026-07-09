# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""WorkflowContext — task-side API for upstream outputs and cancellation.

Designed for two call-sites:

* **Custom Processors** — inside ``Processor.__call__``, call
  :func:`ai4s.jobq.workflow.get_workflow_context` (re-exported from
  :mod:`ai4s.jobq.workflow.worker`).  That returns a lazy context
  backed by :class:`WorkflowPersistence` that uses the upstream-ref
  sidecar the worker stashed before invoking the processor — no extra
  load of the full workflow runtime.
* **Shell scripts** — the worker sets ``JOBQ_WORKFLOW_ID``,
  ``JOBQ_WORKFLOW_TASK``, ``JOBQ_WORKFLOW_UPSTREAM_REFS`` and
  ``JOBQ_OUTPUT_FILE`` in the subprocess environment.  Scripts use the
  sync helpers in this module (:func:`get_upstream_output`,
  :func:`get_upstream_outputs`, :func:`is_cancelled`,
  :func:`set_output`) which open a short-lived
  :class:`WorkflowContext` underneath.

The async :class:`WorkflowContext` API exists for long-lived contexts
that want to share a single persistence handle across multiple calls.

Env vars used:

* ``JOBQ_WORKFLOW_PREFIX`` — ``<account>/<prefix>`` workflow project (required
  for any persistence-backed call).
* ``JOBQ_WORKFLOW_BLOBS`` — ``<account>/<container>`` for large
  outputs (optional; defaults to the ``JOBQ_WORKFLOW_PREFIX`` account,
  container ``jobq-workflow-data``).
* ``JOBQ_WORKFLOW_ID`` / ``JOBQ_WORKFLOW_TASK`` — workflow + task
  identifiers (set by :class:`WorkflowShellCommandProcessor`).
* ``JOBQ_WORKFLOW_UPSTREAM_REFS`` — sidecar JSON file with this task's
  direct upstream outputs (set by the worker).
* ``JOBQ_OUTPUT_FILE`` — destination file for :func:`set_output`
  (set by the worker; the worker reads the file after the script
  exits and stages the output via the coordinator).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure.storage.blob.aio import BlobServiceClient

    from ai4s.jobq.workflow.persistence import WorkflowPersistence

LOG = logging.getLogger(__name__)


def _blob_service_client(account: str) -> BlobServiceClient:
    """Build a :class:`BlobServiceClient` for the given account.

    Accepts:

    * ``"devstoreaccount1"`` — Azurite shorthand (uses the dev connection
      string with the well-known account key).
    * A bare account name — uses :class:`DefaultAzureCredential`.
    """
    from azure.storage.blob.aio import BlobServiceClient

    if account == "devstoreaccount1":
        from ai4s.jobq.backend.storage_queue import azurite_conn_str

        return BlobServiceClient.from_connection_string(azurite_conn_str(service="blob"))

    from ai4s.jobq.auth import get_token_credential

    account_url = f"https://{account}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=get_token_credential())


class WorkflowContext:
    """User-facing context for reading upstream outputs + cancellation.

    Lazy: opens a :class:`WorkflowPersistence` handle on first network
    call, caches it, and closes it on :meth:`close` /
    ``__aexit__``.  Use the ``async with`` protocol or the sync helpers
    below — never let the persistence handle leak.
    """

    def __init__(self, workflow_id: str, task_name: str) -> None:
        self.workflow_id = workflow_id
        self.task_name = task_name
        self._persistence: WorkflowPersistence | None = None
        self._upstream_refs_cache: dict[str, str | None] | None = None

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    async def from_kwargs(
        cls,
        kwargs: dict[str, Any],
        *,
        prefix: str | None = None,
    ) -> WorkflowContext:
        """Build from task kwargs that contain workflow metadata.

        Reads ``__workflow_id`` / ``__workflow_task`` (preferred) or
        ``__task_name`` (legacy synonym) from *kwargs*.  Raises
        :class:`ValueError` if neither set is present.
        """
        wf_id = kwargs.get("__workflow_id")
        task_name = kwargs.get("__workflow_task") or kwargs.get("__task_name")
        if not wf_id or not task_name:
            raise ValueError(
                "kwargs must contain __workflow_id and __workflow_task "
                "(are you running inside a workflow task?)"
            )
        LOG.debug("WorkflowContext created for %s/%s", wf_id, task_name)
        return cls(wf_id, task_name)

    @classmethod
    async def from_environment(cls) -> WorkflowContext:
        """Build from ``JOBQ_WORKFLOW_ID`` / ``JOBQ_WORKFLOW_TASK``.

        Both env vars are set automatically by
        :class:`WorkflowShellCommandProcessor` when launching the task
        subprocess.
        """
        wf_id = os.environ.get("JOBQ_WORKFLOW_ID", "")
        task_name = os.environ.get("JOBQ_WORKFLOW_TASK", "")
        if not wf_id or not task_name:
            raise ValueError(
                "JOBQ_WORKFLOW_ID and JOBQ_WORKFLOW_TASK must be set. "
                "Are you running inside a WorkflowShellCommandProcessor?"
            )
        LOG.debug("WorkflowContext from_environment for %s/%s", wf_id, task_name)
        return cls(wf_id, task_name)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the underlying persistence handle if open."""
        if self._persistence is not None:
            with contextlib.suppress(Exception):
                await self._persistence.__aexit__(None, None, None)
            self._persistence = None

    async def __aenter__(self) -> WorkflowContext:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Persistence handle (lazy)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Upstream-refs sidecar
    # ------------------------------------------------------------------

    def _load_upstream_refs(self) -> dict[str, str | None]:
        """Return the worker-stashed ``{upstream_name: output_ref}`` map.

        Returns an empty dict if the sidecar isn't present (e.g. a
        custom Processor that didn't go through the shell entrypoint).
        Callers should fall back to a full runtime load in that case.
        """
        if self._upstream_refs_cache is not None:
            return self._upstream_refs_cache
        path = os.environ.get("JOBQ_WORKFLOW_UPSTREAM_REFS", "")
        if not path or not os.path.isfile(path):
            self._upstream_refs_cache = {}
            return self._upstream_refs_cache
        try:
            with open(path) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            LOG.warning("upstream refs sidecar %s unreadable: %s", path, exc)
            self._upstream_refs_cache = {}
            return self._upstream_refs_cache
        if not isinstance(data, dict):
            self._upstream_refs_cache = {}
            return self._upstream_refs_cache
        self._upstream_refs_cache = data
        return self._upstream_refs_cache

    async def _resolve_upstream_refs(self) -> dict[str, str | None]:
        """Return upstream refs, falling back to a runtime load if needed."""
        refs = self._load_upstream_refs()
        if refs:
            return refs
        persistence = await self._ensure_persistence()
        loaded = await persistence.load(self.workflow_id)
        if loaded is None:
            return {}
        runtime, _etag = loaded
        return runtime.parent_output_refs(self.task_name)

    # ------------------------------------------------------------------
    # Upstream data
    # ------------------------------------------------------------------

    async def get_upstream_output(self, task_name: str) -> Any:
        """Fetch the output of a direct upstream task.

        Returns ``None`` if the upstream produced no output or was
        skipped.  Raises :class:`KeyError` if *task_name* isn't a
        direct dependency of the current task.
        """
        refs = await self._resolve_upstream_refs()
        if task_name not in refs:
            raise KeyError(f"{task_name!r} is not a direct upstream of {self.task_name!r}")
        ref = refs[task_name]
        if not ref:
            return None
        persistence = await self._ensure_persistence()
        return await persistence.fetch_output(ref)

    async def get_available_upstream_outputs(self) -> dict[str, Any]:
        """Fetch every direct upstream output that's available.

        Useful with ``dep_policy="any"``: entries with a ``None`` ref
        (parent skipped or failed) are omitted from the returned dict.
        """
        refs = await self._resolve_upstream_refs()
        out: dict[str, Any] = {}
        for name, ref in refs.items():
            if not ref:
                continue
            persistence = await self._ensure_persistence()
            out[name] = await persistence.fetch_output(ref)
        return out

    async def get_real_upstream_tasks(self) -> list[str]:
        """Return the original (non-synthetic) upstream task names.

        Workflows submitted via ``ai4s-jobq workflow submit
        --max-fan-in N`` (or the Python helper
        :func:`ai4s.jobq.workflow.transforms.sequentialize_fan_in`)
        are restructured: a wide fan-in is broken into sequential
        batches separated by lightweight ``__batch_merge`` nodes, and
        the original leaf depends only on the *last* merge node.
        Scripts running on that leaf typically still want to see every
        original parent — e.g. to fetch their outputs.

        This helper walks the runtime DAG, follows any merge ancestor
        chains, and returns the names of all real (non-merge) tasks
        that contributed to the current task's fan-in.  Names are
        returned in topological-then-lexicographic order.

        The walk only touches the workflow's runtime blob (no per-task
        IO), so it scales linearly in the number of merge nodes plus
        original parents, not in workflow size.
        """
        persistence = await self._ensure_persistence()
        loaded = await persistence.load(self.workflow_id)
        if loaded is None:
            return []
        runtime, _etag = loaded

        def _is_merge(name: str) -> bool:
            task = runtime.tasks.get(name)
            return task is not None and bool(task.kwargs.get("__batch_merge"))

        real: set[str] = set()
        seen: set[str] = set()
        ch = runtime.tasks.get(self.task_name)
        if ch is None:
            return []
        stack: list[str] = list(ch.parents)
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            if _is_merge(n):
                parent = runtime.tasks.get(n)
                if parent is not None:
                    stack.extend(parent.parents)
            else:
                real.add(n)
        return sorted(real)

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def is_cancelled(self) -> bool:
        """Return ``True`` if the workflow has been cancelled.

        Long-running tasks can poll this to bail out early.
        """
        persistence = await self._ensure_persistence()
        return await persistence.get_cancel_requested(self.workflow_id)


# ---------------------------------------------------------------------------
# Output helper (no persistence needed)
# ---------------------------------------------------------------------------


def set_output(data: Any) -> None:
    """Write task output for downstream tasks to consume.

    Call this from any script running inside a workflow task to pass
    structured data to downstream dependents.  The data must be
    JSON-serializable, with one extension: values produced by
    :meth:`ai4s.jobq.workflow.BlobStasher.from_file` are accepted and
    upload the referenced local file to Blob Storage automatically (the
    worker materialises them after the script exits).

    Usage::

        from ai4s.jobq.workflow.context import set_output
        from ai4s.jobq.workflow import BlobStasher

        # Plain JSON output
        set_output({"model_path": "abfs://models/v2.pt", "mae": 0.03})

        # File output (worker materialises the upload after the script)
        set_output({
            "checkpoint": BlobStasher.from_file("model.pt"),
            "mae": 0.03,
        })

    Raises:
        RuntimeError: If ``JOBQ_OUTPUT_FILE`` is not set (i.e. not
            running inside a workflow task subprocess).
        TypeError: If *data* contains a value that is neither JSON-
            serializable nor a recognised stash marker.
    """
    output_file = os.environ.get("JOBQ_OUTPUT_FILE", "")
    if not output_file:
        raise RuntimeError(
            "JOBQ_OUTPUT_FILE is not set. Are you running inside a WorkflowShellCommandProcessor?"
        )
    from ai4s.jobq.workflow.stash import stash_json_default

    serialized = json.dumps(data, default=stash_json_default)
    with open(output_file, "w") as f:
        f.write(serialized)
    LOG.debug("Wrote output (%d bytes) to %s", len(serialized), output_file)


# ---------------------------------------------------------------------------
# Sync convenience functions for user scripts
# ---------------------------------------------------------------------------


def get_upstream_output(task_name: str) -> Any:
    """Fetch output of a direct upstream task (sync wrapper).

    Convenience wrapper that opens a context from environment
    variables, fetches the output, and cleans up.  Ideal for simple
    scripts that don't need to hold a long-lived context.

    Requires ``JOBQ_WORKFLOW_PREFIX``, ``JOBQ_WORKFLOW_ID``, and
    ``JOBQ_WORKFLOW_TASK`` (set automatically by
    :class:`WorkflowShellCommandProcessor`).
    """

    async def _fetch() -> Any:
        async with await WorkflowContext.from_environment() as ctx:
            return await ctx.get_upstream_output(task_name)

    return asyncio.run(_fetch())


def get_upstream_outputs() -> dict[str, Any]:
    """Fetch every available direct upstream output (sync wrapper)."""

    async def _fetch() -> dict[str, Any]:
        async with await WorkflowContext.from_environment() as ctx:
            return await ctx.get_available_upstream_outputs()

    return asyncio.run(_fetch())


def get_real_upstream_tasks() -> list[str]:
    """Return the original (non-synthetic) upstream task names (sync wrapper).

    Use this on a task whose direct parents include synthetic
    ``__batch_merge`` nodes inserted by ``--max-fan-in`` /
    :func:`ai4s.jobq.workflow.transforms.sequentialize_fan_in`.  The
    walk recovers every real ancestor of the current task and lets
    downstream code call :func:`get_upstream_output` against each
    one — bypassing the merge chain.

    Example::

        from ai4s.jobq.workflow.context import (
            get_real_upstream_tasks,
            get_upstream_output,
        )

        all_roots = get_real_upstream_tasks()
        results = {name: get_upstream_output(name) for name in all_roots}
    """

    async def _walk() -> list[str]:
        async with await WorkflowContext.from_environment() as ctx:
            return await ctx.get_real_upstream_tasks()

    return asyncio.run(_walk())


def is_cancelled() -> bool:
    """Check if the current workflow has been cancelled (sync wrapper)."""

    async def _check() -> bool:
        async with await WorkflowContext.from_environment() as ctx:
            return await ctx.is_cancelled()

    return asyncio.run(_check())
