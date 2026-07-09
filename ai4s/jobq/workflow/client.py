# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""High-level client for submitting and managing workflows.

This is the user-facing surface of the workflow layer.  It wraps:

* :class:`~ai4s.jobq.workflow.persistence.WorkflowPersistence` — durable
  state in Blob Storage + an Azure Table index.
* :class:`~ai4s.jobq.workflow._queues.JobQPool` — lazy per-queue JobQ
  handles for pushing root tasks during ``submit`` / ``retry``.

The client owns workflow bootstrap (submit) and user-driven control
operations (cancel, retry, status, list, summary, purge, queue
drain).  The coordinator (run separately as a daemon) owns the event
loop that advances tasks as completions arrive.

Backwards compatibility
-----------------------

The legacy implementation backed by :class:`WorkflowStore` lives in
``client_legacy.py`` for the duration of the R3 transition and is
slated for deletion in R5.  The public surface (class names,
method signatures) of this module matches the legacy module so
callers (CLI, dashboards, examples) keep working unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from ai4s.jobq.workflow._compact_refs import COMPACT_KWARG
from ai4s.jobq.workflow._compact_refs import encode as _encode_refs
from ai4s.jobq.workflow._queues import JobQPool, open_jobq
from ai4s.jobq.workflow.entities import (
    TaskStatus,
    WorkflowDefinition,
    WorkflowStatus,
    generate_workflow_id,
)
from ai4s.jobq.workflow.ids import completion_queue_name, task_message_id
from ai4s.jobq.workflow.persistence import (
    WorkflowConflictError,
    WorkflowNotFoundError,
    WorkflowPersistence,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ai4s.jobq.workflow.state import WorkflowRuntime

LOG = logging.getLogger(__name__)


@dataclass
class SubmitResult:
    """Result of a single workflow submission."""

    workflow_id: str
    task_count: int
    elapsed_s: float
    error: str | None = None


@dataclass
class AggregateStatus:
    """Aggregated counts across all workflows."""

    workflows: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    total_tasks: int = 0
    completed_tasks: int = 0
    running_tasks: int = 0
    failed_tasks: int = 0
    pending_tasks: int = 0
    skipped_tasks: int = 0

    @property
    def total_workflows(self) -> int:
        return sum(self.workflows.values())


class WorkflowClient:
    """High-level API for submitting and managing workflows.

    Usage::

        async with WorkflowClient.from_environment() as client:
            wf_id = await client.submit(definition)
            status = await client.status(wf_id)

    The client opens queue connections lazily — the first ``submit``
    or ``retry`` brings up a :class:`JobQPool` that's reused for the
    lifetime of the client and closed on ``__aexit__`` / ``close``.
    """

    def __init__(
        self,
        persistence: WorkflowPersistence,
        *,
        queues_account: str,
        prefix: str,
    ) -> None:
        self._persistence = persistence
        self._queues_account = queues_account
        self._prefix = prefix
        self._pool: JobQPool | None = None
        self._pool_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @classmethod
    async def from_connection_string(
        cls,
        conn_str: str,
        *,
        prefix: str = "JobQ",
        queues_account: str | None = None,
    ) -> WorkflowClient:
        """Build a client backed by a single Azure Storage connection string.

        *queues_account* defaults to the same connection string when
        omitted, which is the right choice for Azurite (one endpoint
        serves blob + table + queue) and real Storage accounts that
        host the queues alongside the workflow state.
        """
        persistence = await WorkflowPersistence.from_connection_string(conn_str, prefix=prefix)
        return cls(
            persistence,
            queues_account=queues_account or conn_str,
            prefix=prefix,
        )

    @classmethod
    async def from_environment(cls, *, prefix: str | None = None) -> WorkflowClient:
        """Create a client from ``JOBQ_WORKFLOW_PREFIX`` and related env vars.

        See :class:`~ai4s.jobq.workflow.env.WorkflowEnv` for the
        accepted variable names.
        """
        from ai4s.jobq.workflow.env import WorkflowEnv

        env = WorkflowEnv.from_environ(prefix=prefix)
        persistence = await WorkflowPersistence.from_account(env.state_account, prefix=env.prefix)
        return cls(
            persistence,
            queues_account=env.queues,
            prefix=env.prefix,
        )

    async def close(self) -> None:
        if self._pool is not None:
            with contextlib.suppress(Exception):
                await self._pool.__aexit__(None, None, None)
            self._pool = None
        await self._persistence.close()

    async def __aenter__(self) -> WorkflowClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal — queue pool
    # ------------------------------------------------------------------

    async def _ensure_pool(self) -> JobQPool:
        if self._pool is not None:
            return self._pool
        async with self._pool_lock:
            if self._pool is None:
                pool = JobQPool(self._queues_account)
                await pool.__aenter__()
                self._pool = pool
        assert self._pool is not None
        return self._pool

    async def _dispatch(
        self,
        runtime: WorkflowRuntime,
        names: list[str],
        *,
        concurrency: int = 64,
    ) -> None:
        """Push *names* to their queues with idempotent message ids.

        Caller is responsible for having called ``runtime.mark_dispatched``
        first so each task's ``attempt_no`` is the version the worker
        will see.  All pushes run concurrently (up to *concurrency* at once)
        so large root-task fans (thousands of roots) complete in seconds
        rather than minutes.
        """
        if not names:
            return
        pool = await self._ensure_pool()
        sem = asyncio.Semaphore(concurrency)

        async def _push_one(name: str) -> None:
            t = runtime.tasks[name]
            kwargs: dict[str, Any] = dict(t.kwargs)
            kwargs["__workflow_id"] = runtime.workflow_id
            kwargs["__workflow_task"] = name
            kwargs["__attempt_no"] = t.attempt_no
            upstream_refs = runtime.parent_output_refs(name)
            if upstream_refs:
                compact = _encode_refs(runtime.workflow_id, upstream_refs)
                if compact:
                    kwargs[COMPACT_KWARG] = compact
            jobq = await pool.get(t.queue)
            async with sem:
                await jobq.push(
                    kwargs,
                    num_retries=0,
                    id=task_message_id(runtime.workflow_id, name, t.attempt_no),
                )

        await asyncio.gather(*(_push_one(n) for n in names))

    # ------------------------------------------------------------------
    # Submit
    # ------------------------------------------------------------------

    async def submit(
        self,
        definition: WorkflowDefinition,
        *,
        workflow_id: str | None = None,
    ) -> str:
        """Submit a workflow for execution.

        Writes durable state, marks the root tasks as dispatched, pushes
        them to their queues with idempotent message ids, then flushes
        the runtime back.  If push succeeds but flush fails the
        coordinator's apply_completion handles the resulting
        attempt-no skew via push-before-flush recovery.
        """
        definition.validate()
        wf_id = workflow_id or generate_workflow_id()

        await self._persistence.submit(wf_id, definition)
        loaded = await self._persistence.load(wf_id)
        assert loaded is not None, "submit() succeeded but load() returned None"
        runtime, etag = loaded

        roots = runtime.ready_tasks()
        if roots:
            runtime.mark_dispatched(roots)
            await self._dispatch(runtime, roots)
            try:
                await self._persistence.flush(runtime, etag)
            except WorkflowConflictError:
                # Coordinator beat us to the flush (e.g. a sweeper).
                # The push already happened with the correct attempt_no
                # and our message id is idempotent, so this is safe.
                LOG.warning("submit(%s): flush conflict ignored", wf_id)

        LOG.info(
            "Workflow %s submitted: %d tasks, %d root(s) dispatched",
            wf_id,
            len(definition.tasks),
            len(roots),
        )
        return wf_id

    async def submit_batch(
        self,
        definitions: list[WorkflowDefinition],
        *,
        concurrency: int = 20,
        on_progress: Callable[[SubmitResult], Any] | None = None,
    ) -> list[SubmitResult]:
        """Submit many workflows in parallel, returning per-workflow results."""
        semaphore = asyncio.Semaphore(max(1, concurrency))
        results: list[SubmitResult | None] = [None] * len(definitions)

        async def _submit(idx: int, defn: WorkflowDefinition) -> None:
            async with semaphore:
                t0 = time.monotonic()
                try:
                    wf_id = await self.submit(defn)
                    result = SubmitResult(
                        workflow_id=wf_id,
                        task_count=len(defn.tasks),
                        elapsed_s=time.monotonic() - t0,
                    )
                except Exception as exc:
                    result = SubmitResult(
                        workflow_id="",
                        task_count=len(defn.tasks),
                        elapsed_s=time.monotonic() - t0,
                        error=str(exc),
                    )
                results[idx] = result
                if on_progress is not None:
                    on_progress(result)

        await asyncio.gather(*[_submit(i, d) for i, d in enumerate(definitions)])
        final = [r for r in results if r is not None]
        errors = sum(1 for r in final if r.error)
        LOG.info(
            "Batch submission done: %d workflows, %d failed",
            len(final),
            errors,
        )
        return final

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def status(
        self,
        workflow_id: str,
        *,
        include_tasks: bool = True,
    ) -> WorkflowStatus:
        """Get the current status of *workflow_id*.

        Raises :class:`KeyError` if the workflow doesn't exist.
        """
        result = await self._persistence.get_workflow_status(
            workflow_id, include_tasks=include_tasks
        )
        if result is None:
            raise KeyError(f"workflow not found: {workflow_id}")
        return result

    async def list_workflows(
        self,
        *,
        status: str | None = None,
    ) -> list[WorkflowStatus]:
        """List workflows, optionally filtered by state string."""
        return await self._persistence.list_workflow_statuses(status=status)

    async def list_recent_terminal(
        self,
        *,
        limit: int = 50,
        status: str | None = None,
    ) -> list[WorkflowStatus]:
        """Newest-first list of terminal workflows."""
        return await self._persistence.list_recent_terminal(limit=limit, status=status)

    async def list_tasks(
        self,
        workflow_id: str,
        *,
        status: str | None = None,
        queue: str | None = None,
        name_prefix: str | None = None,
    ) -> list[TaskStatus]:
        """List tasks in a workflow with optional filters."""
        return await self._persistence.list_tasks(
            workflow_id,
            status=status,
            queue=queue,
            name_prefix=name_prefix,
        )

    async def summary(self) -> AggregateStatus:
        """Aggregate status counts across all workflows."""
        agg = AggregateStatus()
        s = await self._persistence.summary()
        wf_counts: dict[str, int] = s.get("workflows", {}) or {}
        agg.workflows = defaultdict(int, wf_counts)
        agg.total_tasks = int(s.get("total_tasks", 0))
        agg.completed_tasks = int(s.get("completed_tasks", 0))
        agg.running_tasks = int(s.get("running_tasks", 0))
        agg.failed_tasks = int(s.get("failed_tasks", 0))
        agg.pending_tasks = int(s.get("pending_tasks", 0))
        agg.skipped_tasks = int(s.get("skipped_tasks", 0))
        return agg

    # ------------------------------------------------------------------
    # Control
    # ------------------------------------------------------------------

    async def cancel(self, workflow_id: str) -> None:
        """Request cancellation of *workflow_id*.

        Returns immediately after flipping the index flag; the
        coordinator's cancel-poll loop applies the cancel cascade to
        the durable state and the workers poll the flag to stop their
        subprocesses.  Use :meth:`status` to observe the actual
        transition to ``CANCELLED``.

        Raises :class:`KeyError` if the workflow doesn't exist.
        Idempotent — calling on an already-cancelled workflow is a
        no-op.
        """
        if await self._persistence.get_index_row(workflow_id) is None:
            raise KeyError(f"workflow not found: {workflow_id}")
        await self._persistence.request_cancel(workflow_id)
        LOG.info("Workflow %s cancel requested", workflow_id)

    async def retry(self, workflow_id: str) -> dict[str, int]:
        """Reset failed / upstream-failed / cancelled tasks and dispatch them.

        Returns counters ``{"reset": N, "now_ready": M, "still_pending": K}``.

        Raises :class:`KeyError` if the workflow doesn't exist.
        """
        try:
            counters = await self._persistence.reset_failed_tasks(workflow_id)
        except WorkflowNotFoundError as exc:
            raise KeyError(f"workflow not found: {workflow_id}") from exc

        # Dispatch any now-READY tasks.  reset_failed_tasks already
        # flushed the state, so reload to get the post-reset etag.
        loaded = await self._persistence.load(workflow_id)
        if loaded is None:
            return counters
        runtime, etag = loaded
        ready = runtime.ready_tasks()
        if ready:
            runtime.mark_dispatched(ready)
            await self._dispatch(runtime, ready)
            with contextlib.suppress(WorkflowConflictError):
                await self._persistence.flush(runtime, etag)

        LOG.info("Workflow %s retried: %s", workflow_id, counters)
        return counters

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def purge(
        self,
        *,
        drop_tables: bool = False,
        terminal_only: bool = True,
        concurrency: int = 20,
        on_progress: Callable[[str, int], None] | None = None,
    ) -> dict[str, int]:
        """Delete workflows and (optionally) drop the backing resources.

        Args:
            drop_tables: Also drop the underlying Table Storage tables.
            terminal_only: When ``True`` (default) only completed, failed,
                and cancelled workflows are removed.  Pass ``False`` to
                also delete workflows that are still in a running state —
                useful for resetting between test runs.
            concurrency: Maximum number of parallel delete operations.
            on_progress: Optional callback invoked once with the total
                workflow count after deletion completes.

        The return shape is ``{"workflows": n, "tasks": 0}`` (the new
        single-blob layout has no separate task table).
        """
        n = await self._persistence.purge(terminal_only=terminal_only, concurrency=concurrency)
        result = {"workflows": n, "tasks": 0}
        if on_progress is not None:
            on_progress("workflows", n)
        if drop_tables:
            await self._persistence.drop_resources()
            # Re-create the empty containers + index table so subsequent
            # client operations (submit, list, ...) work without the
            # caller having to rebuild the client.
            await self._persistence._ensure_resources()
        LOG.info(
            "Purged %d workflows (drop_tables=%s)",
            n,
            drop_tables,
        )
        return result

    async def discover_queues(self, *, extra_queues: Iterable[str] = ()) -> list[str]:
        """Return distinct queue names referenced by stored workflows.

        Includes the coordinator's completion queue and any *extra_queues*.
        """
        rows = await self._persistence.list_workflows()
        queues: set[str] = {completion_queue_name(self._prefix), *extra_queues}
        for r in rows:
            queues.update(r.queues_used)
            if r.default_queue:
                queues.add(r.default_queue)
        return sorted(queues)

    async def drain_queues(
        self,
        *,
        queues_account: str | None = None,
        queue_names: Iterable[str] | None = None,
        extra_queues: Iterable[str] = (),
        concurrency: int = 5,
        on_progress: Callable[[str, int], None] | None = None,
    ) -> dict[str, int]:
        """Drain all messages from queues used by this workflow setup.

        Discovers queue names from stored workflows (plus the
        completion queue and *extra_queues*), or uses *queue_names*
        when given.  Each queue is opened via
        :func:`~ai4s.jobq.workflow._queues.open_jobq` and cleared.

        *queues_account* defaults to the client's own
        ``queues_account`` setting; pass an explicit value to drain
        from a different account.
        """
        backend = queues_account or self._queues_account

        if queue_names is None:
            queues = await self.discover_queues(extra_queues=extra_queues)
        else:
            queues = sorted(set(queue_names))

        if not queues:
            return {}

        sem = asyncio.Semaphore(max(1, concurrency))
        counts: dict[str, int] = {}

        async def _drain_one(queue_name: str) -> None:
            async with sem:
                drained = -1
                try:
                    async with open_jobq(queue_name, backend) as jq:
                        try:
                            drained = await jq._client.__len__()
                        except Exception:
                            drained = 0
                        await jq.clear()
                except Exception as exc:
                    LOG.warning("Failed to drain queue %s: %s", queue_name, exc)
                    drained = -1
                counts[queue_name] = drained
                if on_progress is not None:
                    on_progress(queue_name, drained)

        await asyncio.gather(*[_drain_one(q) for q in queues])
        LOG.info(
            "Drained %d queues (%d messages total)",
            len(counts),
            sum(c for c in counts.values() if c > 0),
        )
        return counts
