# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Workflow coordinator — the event loop that advances DAGs.

The coordinator runs as a long-lived process (one per environment in
v1).  It consumes completion messages workers publish to the
completion queue, advances the in-memory :class:`WorkflowRuntime` for
each affected workflow, pushes newly-ready child tasks to their
queues, and flushes state back to the durable store.

Loop structure
--------------

Two cooperating coroutines run concurrently for the lifetime of the
coordinator:

* ``_main_loop`` — receive a batch from the completion queue, group
  the messages by workflow, then for each affected workflow do a
  single ``load → apply each completion → push newly-ready children →
  flush → ack`` cycle.  This is the throughput-critical path.

* ``_cancel_poll_loop`` — periodically scan the persistence index for
  workflows with ``cancel_requested=True``, load each, mark
  PENDING/READY tasks as CANCELLED in memory, and flush.

The main loop owns *every* mutation of state blobs.  The cancel
loop and the public API (``WorkflowClient.cancel``) only set the
``cancel_requested`` flag on the index Table row — the main loop
applies that flag to the state blob.  This single-writer invariant
keeps ETag conflicts rare in v1's single-coordinator deployment.

At-least-once delivery
----------------------

Storage Queue delivers each message at least once.  Two consequences:

* :meth:`WorkflowRuntime.apply_completion` is idempotent — a duplicate
  completion is a no-op and does not corrupt counters.
* Child tasks pushed before a crash may be pushed again on restart if
  the state-blob flush did not complete.  The worker is expected to
  be idempotent (typical pattern: workers write their result before
  emitting the completion).

Concurrency model
-----------------

v1 is **single-coordinator per environment**.  A sentinel blob lease
(:class:`~ai4s.jobq.workflow._lease.CoordinatorLease`) enforces this
at startup — a second coordinator against the same prefix will be
rejected with a clear error.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import logging
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from ai4s.jobq.backend.storage_queue import StorageQueueBackend
from ai4s.jobq.entities import EmptyQueue, Task
from ai4s.jobq.workflow._compact_refs import COMPACT_KWARG
from ai4s.jobq.workflow._compact_refs import encode as _encode_refs
from ai4s.jobq.workflow._lease import CoordinatorLease
from ai4s.jobq.workflow._queues import JobQPool as _JobQPool
from ai4s.jobq.workflow._queues import open_jobq as _open_jobq
from ai4s.jobq.workflow.condition import evaluate_condition
from ai4s.jobq.workflow.entities import (
    TaskState,
    WorkflowCompletion,
    WorkflowState,
)
from ai4s.jobq.workflow.ids import task_message_id
from ai4s.jobq.workflow.persistence import (
    WorkflowConflictError,
    WorkflowPersistence,
)

if TYPE_CHECKING:
    from types import TracebackType

    from azure.storage.queue import QueueMessage

    from ai4s.jobq import JobQ
    from ai4s.jobq.workflow.state import WorkflowRuntime

LOG = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 32
DEFAULT_VISIBILITY_TIMEOUT_S = 60.0
DEFAULT_IDLE_SLEEP_S = 0.5
DEFAULT_CANCEL_POLL_INTERVAL_S = 1.0
DEFAULT_FLUSH_RETRY_LIMIT = 2
DEFAULT_READY_SWEEP_INTERVAL_S = 60.0
DEFAULT_READY_REPAIR_THRESHOLD_S = 300.0
DEFAULT_RUNNING_SWEEP_INTERVAL_S = 60.0
DEFAULT_STATS_LOG_INTERVAL_S = 10.0
DEFAULT_CACHE_MAX_WORKFLOWS = 128
# Maximum number of back-to-back receive calls in one _process_one_batch
# cycle.  With deferred flush, each receive's completions are applied
# immediately to in-memory state and ready tasks pushed, so a larger cap
# lets the coordinator drain a full wave (e.g. 2108 root tasks at 32/receive
# = 66 calls) before writing the blob once.
_MAX_RECEIVE_LOOPS = 64


# ---------------------------------------------------------------------------
# Parsed completion + grouped batch
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ParsedCompletion:
    raw: QueueMessage
    completion: WorkflowCompletion


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------


class Coordinator:
    """Single-coordinator-per-environment workflow event loop.

    Usage::

        async with WorkflowPersistence.from_environment() as store, \
                Coordinator(
                    store,
                    completion_queue="jobq-completion",
                    queues_account="myacct",
                ) as coord:
            await coord.run()
    """

    def __init__(
        self,
        persistence: WorkflowPersistence,
        *,
        completion_queue: str,
        queues_account: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        visibility_timeout_s: float = DEFAULT_VISIBILITY_TIMEOUT_S,
        idle_sleep_s: float = DEFAULT_IDLE_SLEEP_S,
        cancel_poll_interval_s: float = DEFAULT_CANCEL_POLL_INTERVAL_S,
        flush_retry_limit: int = DEFAULT_FLUSH_RETRY_LIMIT,
        ready_sweep_interval_s: float = DEFAULT_READY_SWEEP_INTERVAL_S,
        ready_repair_threshold_s: float = DEFAULT_READY_REPAIR_THRESHOLD_S,
        running_timeout_s: float | None = None,
        running_sweep_interval_s: float = DEFAULT_RUNNING_SWEEP_INTERVAL_S,
        cache_max_workflows: int = DEFAULT_CACHE_MAX_WORKFLOWS,
        _own_persistence: bool = False,
    ) -> None:
        self._persistence = persistence
        self._own_persistence = _own_persistence
        self._completion_queue = completion_queue
        self._queues_account = queues_account
        self._batch_size = batch_size
        self._visibility_timeout = timedelta(seconds=visibility_timeout_s)
        self._idle_sleep_s = idle_sleep_s
        self._cancel_poll_interval_s = cancel_poll_interval_s
        self._flush_retry_limit = max(1, flush_retry_limit)
        self._ready_sweep_interval_s = ready_sweep_interval_s
        self._ready_repair_threshold = timedelta(seconds=ready_repair_threshold_s)
        self._running_timeout_s = running_timeout_s
        self._running_sweep_interval_s = running_sweep_interval_s
        self._cache_max_workflows = max(1, cache_max_workflows)

        # Populated on __aenter__.
        self._stack = AsyncExitStack()
        self._completion_jobq: JobQ | None = None
        self._completion_backend: StorageQueueBackend | None = None
        self._task_pool: _JobQPool | None = None
        self._stop_event = asyncio.Event()

        # LRU cache of recently-flushed workflow runtimes.  Bounded to
        # ``cache_max_workflows`` entries; eldest evicted on insert.
        # Eliminates the blob download on the hot path: after flushing,
        # we already hold the authoritative state in memory.  On an ETag
        # conflict (another writer) we evict and reload from blob.
        self._runtime_cache: collections.OrderedDict[str, tuple[WorkflowRuntime, str]] = (
            collections.OrderedDict()
        )

        # Lightweight metrics for testing + observability.
        self.stats = _Stats()

    def _cache_put(self, wf_id: str, runtime: WorkflowRuntime, etag: str) -> None:
        """Insert into the LRU cache, evicting the eldest entry if full."""
        self._runtime_cache[wf_id] = (runtime, etag)
        self._runtime_cache.move_to_end(wf_id)
        while len(self._runtime_cache) > self._cache_max_workflows:
            self._runtime_cache.popitem(last=False)

    @classmethod
    async def from_environment(
        cls,
        *,
        state_account: str | None = None,
        prefix: str | None = None,
        queues: str | None = None,
        config: str | object | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        visibility_timeout_s: float = DEFAULT_VISIBILITY_TIMEOUT_S,
        idle_sleep_s: float = DEFAULT_IDLE_SLEEP_S,
        cancel_poll_interval_s: float = DEFAULT_CANCEL_POLL_INTERVAL_S,
        flush_retry_limit: int = DEFAULT_FLUSH_RETRY_LIMIT,
        ready_sweep_interval_s: float = DEFAULT_READY_SWEEP_INTERVAL_S,
        ready_repair_threshold_s: float = DEFAULT_READY_REPAIR_THRESHOLD_S,
        running_timeout_s: float | None = None,
        running_sweep_interval_s: float = DEFAULT_RUNNING_SWEEP_INTERVAL_S,
        cache_max_workflows: int = DEFAULT_CACHE_MAX_WORKFLOWS,
    ) -> Coordinator:
        """Build a Coordinator from ``JOBQ_WORKFLOW_PREFIX`` and related env vars.

        Opens a :class:`WorkflowPersistence` for the resolved
        ``state_account``/``prefix`` and registers it for cleanup so
        :meth:`__aexit__` closes it.  Reject Service Bus completion
        backends here — the coordinator hot path requires Storage
        Queue's batched receive/visibility timeout semantics.
        """
        from ai4s.jobq.workflow.env import WorkflowConfig, WorkflowEnv
        from ai4s.jobq.workflow.ids import completion_queue_name

        env = WorkflowEnv.from_environ(
            state_account=state_account,
            prefix=prefix,
            queues=queues,
            config=config if isinstance(config, (str, WorkflowConfig)) else None,
        )
        if env.queues.startswith("sb://"):
            raise ValueError(
                "Coordinator requires a Storage Queue backend; "
                f"got Service Bus ({env.queues}). Unset JOBQ_WORKFLOW_QUEUES "
                "(or the config-file queues override) or point it at an "
                "Azure Storage account."
            )
        persistence = await WorkflowPersistence.from_account(env.state_account, prefix=env.prefix)
        return cls(
            persistence,
            completion_queue=completion_queue_name(env.prefix),
            queues_account=env.queues,
            batch_size=batch_size,
            visibility_timeout_s=visibility_timeout_s,
            idle_sleep_s=idle_sleep_s,
            cancel_poll_interval_s=cancel_poll_interval_s,
            flush_retry_limit=flush_retry_limit,
            ready_sweep_interval_s=ready_sweep_interval_s,
            ready_repair_threshold_s=ready_repair_threshold_s,
            running_timeout_s=running_timeout_s,
            running_sweep_interval_s=running_sweep_interval_s,
            cache_max_workflows=cache_max_workflows,
            _own_persistence=True,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> Coordinator:
        await self._stack.__aenter__()
        if self._own_persistence:
            self._stack.push_async_callback(self._persistence.close)
        # Acquire the sentinel lease to enforce single-coordinator invariant.
        lease = CoordinatorLease(
            self._persistence._state_container,
            prefix=self._persistence._prefix,
        )
        await self._stack.enter_async_context(lease)
        cm = _open_jobq(self._completion_queue, self._queues_account)
        self._completion_jobq = await self._stack.enter_async_context(cm)
        backend = self._completion_jobq._client
        if not isinstance(backend, StorageQueueBackend):
            await self._stack.aclose()
            raise TypeError(
                "Coordinator requires a Storage Queue completion backend; "
                f"got {type(backend).__name__!r}"
            )
        self._completion_backend = backend
        self._task_pool = await self._stack.enter_async_context(_JobQPool(self._queues_account))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._stack.__aexit__(exc_type, exc, tb)

    def stop(self) -> None:
        """Request the run loop to exit at the next opportunity."""
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the main, cancel-poll, ready-repair, and running-timeout loops until :meth:`stop`."""
        cancel_task = asyncio.create_task(self._cancel_poll_loop(), name="coordinator-cancel-poll")
        repair_task = asyncio.create_task(
            self._sweep_stuck_ready_loop(),
            name="coordinator-ready-repair",
        )
        running_timeout_task = asyncio.create_task(
            self._sweep_stuck_running_loop(),
            name="coordinator-running-timeout",
        )
        stats_task = asyncio.create_task(self._stats_log_loop(), name="coordinator-stats-log")
        try:
            await self._main_loop()
        finally:
            for t in (cancel_task, repair_task, running_timeout_task, stats_task):
                t.cancel()
            await asyncio.gather(
                cancel_task,
                repair_task,
                running_timeout_task,
                stats_task,
                return_exceptions=True,
            )

    async def run_once(self) -> int:
        """Process a single batch of completions and apply any pending cancels.

        Returns the number of completion messages handled (0 means the
        completion queue was empty).  Test entry point — production
        deployments should use :meth:`run`.  Does *not* run the
        ready-repair or running-timeout sweeps; use
        :meth:`sweep_stuck_ready_once` / :meth:`sweep_stuck_running_once`
        for those.
        """
        await self._apply_pending_cancels()
        return await self._process_one_batch()

    async def sweep_stuck_ready_once(self) -> int:
        """Run the ready-repair sweep once.  Returns tasks re-dispatched.

        Test entry point — production deployments should use :meth:`run`,
        which schedules this sweep periodically alongside the main loop.
        """
        return await self._sweep_stuck_ready_once()

    async def sweep_stuck_running_once(self) -> int:
        """Time out RUNNING tasks that have exceeded their deadline.

        Returns the number of tasks that were timed out.  Applies only to
        tasks whose ``task.timeout_s`` is set, or when the coordinator was
        constructed with ``running_timeout_s`` as a per-workflow default.

        Test entry point — production deployments should use :meth:`run`,
        which schedules this sweep periodically alongside the main loop.
        """
        return await self._sweep_stuck_running_once()

    # ------------------------------------------------------------------
    # Stats logging loop
    # ------------------------------------------------------------------

    async def _stats_log_loop(self, interval_s: float = DEFAULT_STATS_LOG_INTERVAL_S) -> None:
        """Log a periodic one-line summary of coordinator throughput."""
        prev_completions = 0
        prev_time = time.monotonic()
        while True:
            await asyncio.sleep(interval_s)
            now = time.monotonic()
            elapsed = now - prev_time
            delta = self.stats.completions_handled - prev_completions
            cps = delta / elapsed if elapsed > 0 else 0.0
            prev_completions = self.stats.completions_handled
            prev_time = now
            LOG.info(
                "coordinator stats: "
                "completions=%d (+%d, %.1f/s)  pushed=%d  batches=%d  "
                "cached=%d  conflicts=%d  dupes=%d  latency=%.3fs",
                self.stats.completions_handled,
                delta,
                cps,
                self.stats.tasks_pushed,
                self.stats.batches,
                len(self._runtime_cache),
                self.stats.flush_conflicts,
                self.stats.duplicate_completions,
                self.stats.last_workflow_latency_s,
            )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _main_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                n = await self._process_one_batch()
            except Exception:
                LOG.exception("coordinator: unexpected error in batch loop")
                n = 0
            if n == 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._idle_sleep_s,
                    )

    async def _process_one_batch(self) -> int:
        """Drain the completion queue, advance workflows, then flush and ack.

        **Deferred-flush design**: completions are applied to the in-memory
        runtime and ready tasks are pushed to workers *immediately* in the
        inner receive loop, but the blob write (flush) is deferred until the
        queue drains or ``_MAX_RECEIVE_LOOPS`` is reached.  This is critical
        for barrier DAGs where downstream tasks only become ready after *all*
        upstream completions are applied — intermediate flushes would be wasted
        writes that don't unlock any new work.

        **Durability**: messages are acked only after their workflow's flush
        succeeds.  A crash after push-but-before-flush causes re-delivery;
        the coordinator's ``ignored_duplicate`` path handles the idempotent
        re-application.

        **Cache invariant**: a workflow is evicted from ``_runtime_cache`` when
        it enters the dirty dict so the cache always reflects persisted state.
        It is re-inserted after a successful flush.
        """
        assert self._completion_backend is not None

        # dirty[wf_id] = (runtime, etag, accumulated_messages)
        dirty: dict[str, tuple[WorkflowRuntime, str, list[_ParsedCompletion]]] = {}
        # Count of orphan-workflow completions acked inline (no flush needed).
        n_orphan_acked = 0

        # Time-bound the accumulation loop so early messages don't expire
        # before we ack them.  Leave 20 % of the visibility window for flushing.
        vis_s = self._visibility_timeout.total_seconds()
        deadline = time.monotonic() + vis_s * 0.8

        for _ in range(_MAX_RECEIVE_LOOPS):
            if time.monotonic() >= deadline:
                break
            try:
                batch = await self._completion_backend.receive_messages_batch(
                    max_messages=self._batch_size,
                    visibility_timeout=self._visibility_timeout,
                )
            except EmptyQueue:
                break  # queue drained → fall through to flush dirty

            # Group this receive batch by workflow.
            batch_grouped: dict[str, list[_ParsedCompletion]] = {}
            for raw in batch:
                comp = _parse_completion(raw)
                if comp is None:
                    LOG.warning(
                        "coordinator: dropping un-parseable completion message id=%s",
                        getattr(raw, "id", "?"),
                    )
                    self.stats.poison_messages += 1
                    with contextlib.suppress(Exception):
                        await self._completion_backend.delete(raw)
                    continue
                batch_grouped.setdefault(comp.completion.workflow_id, []).append(comp)

            # For each workflow: load state (once), apply completions, push
            # ready tasks immediately, accumulate in dirty for later flush.
            for wf_id, items in batch_grouped.items():
                if wf_id in dirty:
                    runtime, etag, msgs = dirty[wf_id]
                elif wf_id in self._runtime_cache:
                    # Evict from cache — runtime is now dirty; cache only
                    # reflects successfully-flushed state.
                    runtime, etag = self._runtime_cache.pop(wf_id)
                    msgs = []
                else:
                    loaded = await self._persistence.load(wf_id)
                    if loaded is None:
                        LOG.warning(
                            "coordinator: %d completion(s) for unknown workflow %s; dropping",
                            len(items),
                            wf_id,
                        )
                        self.stats.orphan_completions += len(items)
                        await self._ack_all(items)
                        n_orphan_acked += len(items)
                        continue
                    runtime, etag = loaded
                    msgs = []

                ready_names = self._apply_completions(runtime, items)
                if runtime.cancel_requested:
                    ready_names = []
                ready_to_dispatch = self._budgeted_ready(runtime, ready_names)
                runtime.mark_dispatched(ready_to_dispatch)
                await self._push_ready_tasks(runtime, ready_to_dispatch)
                msgs.extend(items)
                dirty[wf_id] = (runtime, etag, msgs)

            if len(batch) < self._batch_size:
                break  # queue is draining; no point receiving more

        if not dirty:
            self.stats.completions_handled += n_orphan_acked
            return n_orphan_acked

        # Flush all dirty workflows in parallel; ack each workflow's messages
        # right after its own flush so one slow/conflicted workflow doesn't
        # block unrelated workflows from acking.
        t_flush = time.monotonic()

        async def _flush_and_ack(
            wf_id: str,
            runtime: WorkflowRuntime,
            etag: str,
            items: list[_ParsedCompletion],
        ) -> tuple[int, int]:
            """Flush one workflow and ack its messages.

            Returns ``(n_received, n_acked)`` where ``n_received`` is always
            ``len(items)`` (drives the event loop) and ``n_acked`` is non-zero
            only when the flush committed and messages were deleted from the
            queue.  A failed flush defers ack so messages are redelivered.
            """
            for attempt in range(self._flush_retry_limit):
                try:
                    new_etag = await self._persistence.flush(
                        runtime, etag, update_index=runtime.is_terminal()
                    )
                except WorkflowConflictError:
                    self._runtime_cache.pop(wf_id, None)
                    self.stats.flush_conflicts += 1
                    if attempt + 1 >= self._flush_retry_limit:
                        LOG.exception(
                            "coordinator: flush conflict for %s after %d attempts; "
                            "deferring ack so messages are redelivered",
                            wf_id,
                            self._flush_retry_limit,
                        )
                        return len(items), 0  # don't ack; messages will be redelivered
                    LOG.info(
                        "coordinator: flush conflict for %s on attempt %d; reloading",
                        wf_id,
                        attempt + 1,
                    )
                    reloaded = await self._persistence.load(wf_id)
                    if reloaded is None:
                        LOG.warning(
                            "coordinator: workflow %s vanished mid-flush; dropping",
                            wf_id,
                        )
                        await self._ack_all(items)
                        return len(items), len(items)
                    runtime, etag = reloaded
                    # Re-apply all accumulated completions against the fresh state.
                    ready_names = self._apply_completions(runtime, items)
                    if runtime.cancel_requested:
                        ready_names = []
                    ready_to_dispatch = self._budgeted_ready(runtime, ready_names)
                    runtime.mark_dispatched(ready_to_dispatch)
                    await self._push_ready_tasks(runtime, ready_to_dispatch)
                    continue
                else:
                    if runtime.is_terminal():
                        self._runtime_cache.pop(wf_id, None)
                    else:
                        self._cache_put(wf_id, runtime, new_etag)
                    await self._ack_all(items)
                    return len(items), len(items)
            return len(items), 0

        results = await asyncio.gather(
            *(_flush_and_ack(wf_id, r, e, m) for wf_id, (r, e, m) in dirty.items()),
            return_exceptions=True,
        )

        n_received = 0
        n_acked = 0
        for res in results:
            if isinstance(res, tuple):
                rcv, acked = res
                n_received += rcv
                n_acked += acked
                if acked > 0:
                    self.stats.workflows_advanced += 1
            else:
                LOG.exception("coordinator: unexpected error in flush: %s", res)

        self.stats.last_workflow_latency_s = time.monotonic() - t_flush
        self.stats.batches += 1
        self.stats.completions_handled += n_acked + n_orphan_acked
        return n_received + n_orphan_acked

    # ------------------------------------------------------------------
    # Per-workflow processing helpers
    # ------------------------------------------------------------------

    def _apply_completions(
        self,
        runtime: WorkflowRuntime,
        items: list[_ParsedCompletion],
    ) -> list[str]:
        """Apply each completion and collect names that became READY.

        Handles condition evaluation for candidate children inline so
        the coordinator dispatches them in the same iteration.
        """
        ready_names: list[str] = []

        for parsed in items:
            comp = parsed.completion
            res = runtime.apply_completion(
                comp.task_name,
                success=bool(comp.success),
                output_ref=comp.output_ref,
                error=comp.error,
                attempt_no=comp.attempt_no,
            )
            if res.ignored_duplicate:
                self.stats.duplicate_completions += 1
                continue
            ready_names.extend(res.ready)

            # Evaluate condition gates for candidate children.
            for cand in res.candidates:
                self._resolve_candidate(runtime, cand, ready_names)

        # De-dup while preserving order — a child can become candidate
        # via one parent and ready via another in the same batch.
        return list(dict.fromkeys(ready_names))

    def _resolve_candidate(
        self,
        runtime: WorkflowRuntime,
        name: str,
        ready_names: list[str],
    ) -> None:
        """Evaluate *name*'s condition and either promote it to READY or skip it."""
        task = runtime.tasks.get(name)
        if task is None or task.state != TaskState.PENDING or task.condition is None:
            return

        inputs = runtime.parent_outputs_inline(name)
        if inputs is None:
            # Upstream output is blob-stashed — v1 limitation.  Skip
            # the child with a clear error.
            runtime.mark_skipped(name, reason="condition skipped: upstream output not inline")
            self.stats.condition_skipped_blob += 1
            return

        try:
            ok = evaluate_condition(task.condition, inputs)
        except Exception as exc:
            LOG.warning(
                "coordinator: condition evaluation failed for %s/%s: %s",
                runtime.workflow_id,
                name,
                exc,
            )
            runtime.mark_skipped(name, reason=f"condition error: {exc}")
            self.stats.condition_errors += 1
            return

        if ok:
            runtime.set_ready(name)
            ready_names.append(name)
        else:
            cascaded = runtime.mark_skipped(name, reason="condition false")
            # Skipped parents count as dep-satisfied for downstream
            # gating — surface any descendants that became READY.
            ready_names.extend(n for n in cascaded if runtime.tasks[n].state == TaskState.READY)

    def _budgeted_ready(self, runtime: WorkflowRuntime, candidates: list[str]) -> list[str]:
        """Return the subset of *candidates* the workflow's budget permits.

        Honours :attr:`WorkflowRuntime.max_parallelism` if set.  Any
        names that don't fit stay in :class:`TaskState.READY`; the
        coordinator picks them up in a later iteration (after some
        RUNNING tasks complete).
        """
        if runtime.max_parallelism is None:
            return candidates
        inflight = sum(1 for t in runtime.tasks.values() if t.state == TaskState.RUNNING)
        free = max(0, runtime.max_parallelism - inflight)
        return candidates[:free]

    async def _push_ready_tasks(
        self,
        runtime: WorkflowRuntime,
        names: list[str],
        *,
        concurrency: int = 64,
    ) -> None:
        assert self._task_pool is not None
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
            jobq = await self._task_pool.get(t.queue)  # type: ignore[union-attr]
            # Coordinator handles retries via state rearm; push with
            # jobq num_retries=0 so the broker doesn't try to retry
            # on its own.
            async with sem:
                await jobq.push(
                    kwargs,
                    num_retries=0,
                    id=task_message_id(runtime.workflow_id, name, t.attempt_no),
                )
            self.stats.tasks_pushed += 1

        await asyncio.gather(*(_push_one(n) for n in names))

    async def _ack_all(self, items: list[_ParsedCompletion], *, concurrency: int = 64) -> None:
        assert self._completion_backend is not None
        # Deferred-flush batches can be large (hundreds to thousands of
        # messages).  Parallelise the deletes to avoid O(N) sequential
        # round-trips dominating flush latency.
        sem = asyncio.Semaphore(concurrency)

        async def _delete_one(parsed: _ParsedCompletion) -> None:
            async with sem:
                with contextlib.suppress(Exception):
                    await self._completion_backend.delete(parsed.raw)  # type: ignore[union-attr]

        await asyncio.gather(*(_delete_one(p) for p in items))

    # ------------------------------------------------------------------
    # Cancellation loop
    # ------------------------------------------------------------------

    async def _cancel_poll_loop(self) -> None:
        """Periodically apply cancel-flag flips from the index to state blobs."""
        while not self._stop_event.is_set():
            try:
                await self._apply_pending_cancels()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("coordinator: cancel poll error (will retry)")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._cancel_poll_interval_s,
                )

    async def _apply_pending_cancels(self) -> None:
        wf_ids = await self._persistence.list_cancel_requested_active()
        for wf_id in wf_ids:
            try:
                await self._apply_single_cancel(wf_id)
            except Exception:
                LOG.exception("coordinator: failed to apply cancel for %s", wf_id)

    async def _apply_single_cancel(self, workflow_id: str) -> None:
        loaded = await self._persistence.load(workflow_id)
        if loaded is None:
            return
        runtime, etag = loaded
        if runtime.is_terminal():
            return
        # Apply the cancel in-memory.  ``request_cancel`` is idempotent
        # so this is safe even if the flag was already applied.
        runtime.request_cancel()
        try:
            await self._persistence.flush(runtime, etag)
            self._runtime_cache.pop(workflow_id, None)
            self.stats.workflows_cancelled += 1
        except WorkflowConflictError:
            LOG.info(
                "coordinator: cancel flush conflict for %s; main loop will retry",
                workflow_id,
            )

    # ------------------------------------------------------------------
    # Ready-repair sweep
    # ------------------------------------------------------------------

    async def _sweep_stuck_ready_loop(self) -> None:
        """Periodically re-dispatch READY tasks in workflows that look stuck.

        Closes the ``client.submit``/``client.retry`` race where the
        caller flipped tasks READY→RUNNING in memory, but crashed before
        pushing the task message (so durable state stays READY and no
        completion will ever wake the main loop).  Without this sweep,
        such workflows would never advance.
        """
        while not self._stop_event.is_set():
            try:
                await self._sweep_stuck_ready_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("coordinator: ready-repair sweep error (will retry)")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._ready_sweep_interval_s,
                )

    async def _sweep_stuck_ready_once(self) -> int:
        """Run one ready-repair sweep.  Returns the number of tasks re-pushed.

        Re-dispatch is **at-least-once**: if the original submit/retry
        actually pushed but failed to flush, the worker may now see two
        copies of the same task at different ``attempt_no`` values.  The
        coordinator's ``apply_completion`` deduplicates the resulting
        completions, but user task code must tolerate duplicate
        execution.  The ``ready_repair_threshold_s`` default of 5
        minutes keeps this rare in practice.
        """
        rows = await self._persistence.list_workflows(
            status_filter=[WorkflowState.PENDING, WorkflowState.RUNNING],
        )
        now = datetime.now(timezone.utc)
        repushed = 0
        for row in rows:
            if row.cancel_requested:
                continue
            if now - row.updated_at < self._ready_repair_threshold:
                continue
            try:
                repushed += await self._repair_single_workflow(row.workflow_id)
            except Exception:
                LOG.exception("coordinator: ready-repair failed for %s", row.workflow_id)
        self.stats.ready_sweeps += 1
        return repushed

    async def _repair_single_workflow(self, workflow_id: str) -> int:
        """Re-dispatch READY tasks for *workflow_id* if any are present.

        Returns the number of tasks re-pushed.  Returns 0 if the
        workflow is gone, terminal, cancel-requested, or has no READY
        tasks.  Catches :class:`WorkflowConflictError` so a race with
        the main/cancel loop doesn't kill the sweep.
        """
        loaded = await self._persistence.load(workflow_id)
        if loaded is None:
            return 0
        runtime, etag = loaded
        # Re-check post-load because list_workflows is a snapshot.
        if runtime.is_terminal() or runtime.cancel_requested:
            return 0
        ready_names = runtime.ready_tasks()
        if not ready_names:
            return 0
        LOG.warning(
            "coordinator: ready-repair re-dispatching %d task(s) in %s: %s",
            len(ready_names),
            workflow_id,
            ", ".join(ready_names),
        )
        runtime.mark_dispatched(ready_names)
        await self._push_ready_tasks(runtime, ready_names)
        try:
            await self._persistence.flush(runtime, etag)
            self._runtime_cache.pop(workflow_id, None)
        except WorkflowConflictError:
            LOG.info(
                "coordinator: ready-repair flush conflict for %s; main loop wins",
                workflow_id,
            )
            self.stats.flush_conflicts += 1
            return 0
        self.stats.ready_tasks_repushed += len(ready_names)
        return len(ready_names)

    # ------------------------------------------------------------------
    # Stuck-RUNNING timeout sweep
    # ------------------------------------------------------------------

    async def _sweep_stuck_running_loop(self) -> None:
        """Periodically time out RUNNING tasks that have exceeded their deadline.

        Only acts on tasks that have a per-task ``timeout_s`` or on
        workflows where the coordinator was started with a
        ``running_timeout_s`` default.  Tasks with no timeout are never
        expired by this sweep (they may still be cancelled via the cancel
        API).
        """
        if self._running_timeout_s is None:
            # Only scan per-task timeouts; still worth running the loop
            # so per-task timeout_s on individual tasks is honoured.
            pass
        while not self._stop_event.is_set():
            try:
                await self._sweep_stuck_running_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("coordinator: running-timeout sweep error (will retry)")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._running_sweep_interval_s,
                )

    async def _sweep_stuck_running_once(self) -> int:
        """Run one RUNNING-timeout sweep.  Returns the number of tasks timed out."""
        rows = await self._persistence.list_workflows(
            status_filter=[WorkflowState.PENDING, WorkflowState.RUNNING],
        )
        now = datetime.now(timezone.utc)
        timed_out = 0
        for row in rows:
            if row.cancel_requested:
                continue
            try:
                timed_out += await self._timeout_single_workflow(row.workflow_id, now)
            except Exception:
                LOG.exception("coordinator: running-timeout sweep failed for %s", row.workflow_id)
        self.stats.running_sweeps += 1
        return timed_out

    async def _timeout_single_workflow(self, workflow_id: str, now: datetime) -> int:
        """Fail RUNNING tasks in *workflow_id* that have exceeded their timeout.

        Returns the number of tasks timed out (0 if none, or if the
        workflow is gone / terminal / has no timedout tasks).
        """
        loaded = await self._persistence.load(workflow_id)
        if loaded is None:
            return 0
        runtime, etag = loaded
        if runtime.is_terminal() or runtime.cancel_requested:
            return 0

        to_timeout: list[str] = []
        for name, task in runtime.tasks.items():
            if task.state != TaskState.RUNNING:
                continue
            if task.started_at is None:
                continue
            effective_timeout = (
                task.timeout_s if task.timeout_s is not None else self._running_timeout_s
            )
            if effective_timeout is None:
                continue
            elapsed = (now - task.started_at).total_seconds()
            if elapsed > effective_timeout:
                to_timeout.append(name)

        if not to_timeout:
            return 0

        LOG.warning(
            "coordinator: timing out %d RUNNING task(s) in %s: %s",
            len(to_timeout),
            workflow_id,
            ", ".join(to_timeout),
        )
        for name in to_timeout:
            effective_timeout = (
                runtime.tasks[name].timeout_s
                if runtime.tasks[name].timeout_s is not None
                else self._running_timeout_s
            )
            runtime.apply_completion(
                name,
                success=False,
                error=f"task timed out after {effective_timeout}s",
            )

        try:
            await self._persistence.flush(runtime, etag)
            self._runtime_cache.pop(workflow_id, None)
        except WorkflowConflictError:
            LOG.info(
                "coordinator: running-timeout flush conflict for %s; main loop wins",
                workflow_id,
            )
            self.stats.flush_conflicts += 1
            return 0
        self.stats.tasks_timed_out += len(to_timeout)
        return len(to_timeout)


# ---------------------------------------------------------------------------
# Wire-format parsing
# ---------------------------------------------------------------------------


def _parse_completion(raw: QueueMessage) -> _ParsedCompletion | None:
    """Decode a worker-emitted completion message.

    Wire format: Storage Queue message whose content is a serialized
    :class:`Task` whose ``kwargs["__completion_body"]`` is a
    JSON-serialized :class:`WorkflowCompletion`.
    """
    try:
        task = Task.deserialize(raw["content"])
    except Exception:
        return None
    body = task.kwargs.get("__completion_body", "")
    if not body:
        try:
            body = json.dumps(task.kwargs)
        except Exception:
            return None
    try:
        completion = WorkflowCompletion.deserialize(body)
    except Exception:
        return None
    return _ParsedCompletion(raw=raw, completion=completion)


# ---------------------------------------------------------------------------
# Lightweight metrics
# ---------------------------------------------------------------------------


@dataclass
class _Stats:
    batches: int = 0
    completions_handled: int = 0
    workflows_advanced: int = 0
    workflows_cancelled: int = 0
    tasks_pushed: int = 0
    duplicate_completions: int = 0
    orphan_completions: int = 0
    poison_messages: int = 0
    flush_conflicts: int = 0
    condition_errors: int = 0
    condition_skipped_blob: int = 0
    ready_sweeps: int = 0
    ready_tasks_repushed: int = 0
    running_sweeps: int = 0
    tasks_timed_out: int = 0
    last_workflow_latency_s: float = 0.0
