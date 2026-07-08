# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Async-to-sync bridge for ``WorkflowStore`` in Dash callbacks.

Dash callbacks are synchronous.  ``WorkflowStore`` is fully async.
This module provides a module-level event loop and a ``run()`` helper
that schedules a coroutine on that loop and blocks until it completes.

Usage inside a Dash callback::

    from ai4s.jobq.track.utils.workflow_store import get_store, run

    store = get_store()
    if store is not None:
        workflows = run(store.list_workflows(status="running"))
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Coroutine

T = TypeVar("T")

_DEFAULT_CONCURRENCY = 8

LOG = logging.getLogger(__name__)

_lock = threading.Lock()
_store_lock = threading.Lock()
_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None
_store: Any | None = None  # WorkflowStore | None
_store_ready = False


def has_workflow_source() -> bool:
    """Return True when any workflow data source is configured."""
    return bool(_workflow_file_env() or _workflow_prefix_env())


def _workflow_prefix_env() -> str:
    return os.environ.get("JOBQ_WORKFLOW_PREFIX", "").strip()


def _workflow_file_env() -> str:
    return os.environ.get("JOBQ_WORKFLOW_FILE", "").strip()


def _ensure_loop() -> asyncio.AbstractEventLoop:
    """Return (and lazily start) the background event loop."""
    global _loop, _loop_thread  # noqa: PLW0603
    if _loop is not None and _loop.is_running():
        return _loop
    with _lock:
        if _loop is not None and _loop.is_running():
            return _loop
        _loop = asyncio.new_event_loop()

        def _run() -> None:
            assert _loop is not None
            asyncio.set_event_loop(_loop)
            _loop.run_forever()

        _loop_thread = threading.Thread(target=_run, daemon=True, name="wf-store-loop")
        _loop_thread.start()
        return _loop


def run(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine synchronously, blocking the caller."""
    loop = _ensure_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=30)


def run_many(*coros: Coroutine[Any, Any, T]) -> list[T]:
    """Run multiple coroutines concurrently, blocking until all complete.

    Equivalent to ``asyncio.gather(*coros)`` executed on the background
    event loop.  Raises the first exception encountered (fail-fast).
    """

    async def _gather() -> list[T]:
        return list(await asyncio.gather(*coros))

    return run(_gather())


def run_many_limited(
    *coros: Coroutine[Any, Any, T], concurrency: int = _DEFAULT_CONCURRENCY
) -> list[T]:
    """Like :func:`run_many` but with bounded concurrency.

    At most *concurrency* coroutines execute simultaneously, preventing
    large fan-outs from overwhelming Azure Table Storage.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _guarded(coro: Coroutine[Any, Any, T]) -> T:
        async with sem:
            return await coro

    async def _gather() -> list[T]:
        return list(await asyncio.gather(*(_guarded(c) for c in coros)))

    return run(_gather())


def get_store() -> Any | None:
    """Return a cached ``WorkflowStore``, or ``None`` if not configured.

    Reads ``JOBQ_WORKFLOW_PREFIX`` (``<account>/<prefix>``) from the environment.
    The store is created once and reused across callbacks.

    Must not be called from inside a coroutine running on the shared
    background loop — it bounces through :func:`run` which would
    deadlock against itself. Use :func:`_aget_store` instead.
    """
    global _store, _store_ready  # noqa: PLW0603
    if _store_ready:
        return _store
    with _store_lock:
        if _store_ready:
            return _store
        wf_file = _workflow_file_env()
        if wf_file:
            try:
                from ai4s.jobq.track.utils.local_workflow_store import LocalWorkflowStore

                _store = LocalWorkflowStore.from_file(wf_file)
                _store_ready = True
                LOG.info("Workflow store ready from local file: %s", wf_file)
                return _store
            except Exception:
                LOG.warning("Failed to initialise local workflow store", exc_info=True)
                _store_ready = True
                return None

        wf_env = _workflow_prefix_env()
        if not wf_env:
            LOG.info("No workflow source configured — workflow dashboard disabled")
            _store_ready = True
            return None
        try:
            _store = run(_build_store(wf_env))
            _store_ready = True
            return _store
        except Exception:
            LOG.warning("Failed to initialise workflow store", exc_info=True)
            _store_ready = True
            return None


async def _build_store(wf_env: str) -> Any | None:
    """Async constructor for the shared :class:`WorkflowPersistence` singleton."""
    from ai4s.jobq.workflow.env import parse_workflow_value
    from ai4s.jobq.workflow.persistence import WorkflowPersistence

    account, prefix = parse_workflow_value(wf_env)
    store = await WorkflowPersistence.from_account(account, prefix=prefix)
    LOG.info("Workflow store ready: %s/%s", account, prefix)
    return store


async def _aget_store() -> Any | None:
    """Async variant of :func:`get_store`, safe to call from the shared loop.

    Coroutines scheduled on the background event loop (e.g. the count
    refresher) cannot use :func:`get_store` because it relies on
    :func:`run`, which round-trips through the same loop and
    deadlocks. This helper builds the store directly with ``await``
    and updates the same cache so a later sync :func:`get_store` call
    returns the already-initialised instance.
    """
    global _store, _store_ready  # noqa: PLW0603
    if _store_ready:
        return _store
    wf_file = _workflow_file_env()
    if wf_file:
        try:
            from ai4s.jobq.track.utils.local_workflow_store import LocalWorkflowStore

            local_store: Any = LocalWorkflowStore.from_file(wf_file)
        except Exception:
            LOG.warning("Failed to initialise local workflow store", exc_info=True)
            with _store_lock:
                _store_ready = True
            return None
        with _store_lock:
            _store = local_store
            _store_ready = True
        return local_store

    wf_env = _workflow_prefix_env()
    if not wf_env:
        with _store_lock:
            _store_ready = True
        return None
    try:
        store = await _build_store(wf_env)
    except Exception:
        LOG.warning("Failed to initialise workflow store", exc_info=True)
        with _store_lock:
            _store_ready = True
        return None
    with _store_lock:
        _store = store
        _store_ready = True
    return store


# ---------------------------------------------------------------------------
# Background-refreshed workflow count cache.
#
# Dash callbacks are pull-based and synchronous, so they can't stream
# partial results from a multi-page Azure Table Storage scan. Instead
# we run a background coroutine on the shared event loop that scans
# each status bucket with ``WorkflowStore.count_workflows`` and
# updates a shared dict after every page. The dashboard callback reads
# the dict instantaneously and always sees the latest counts —
# accurate after a full scan completes, progressively converging
# while a scan is in flight.
# ---------------------------------------------------------------------------

_COUNT_STATUSES: tuple[str, ...] = ("running", "pending")
# NOTE: we deliberately do **not** scan terminal (completed/failed/
# cancelled) buckets here — terminal history is unbounded at scale
# (chaos runs can produce hundreds of thousands of finished
# workflows) and the dashboard only needs a live view of active work.
# If a future caller wants a full row count, see
# ``WorkflowStore.count_workflows(status=None)``.
_counts_lock = threading.Lock()
_counts: dict[str, int] = {}
_counts_started = False
_DEFAULT_COUNTS_PERIOD_S = 5.0


def _set_count(status: str, value: int) -> None:
    with _counts_lock:
        _counts[status] = value


def get_workflow_counts() -> dict[str, int]:
    """Return a snapshot of the latest cached workflow counts.

    Keys are status names (``"running"``, ``"pending"``) plus
    ``"total"`` (= ``running + pending``, i.e. *active* workflows
    only — terminal history is intentionally excluded so the
    dashboard stays useful at scale). Missing keys default to 0.
    During a refresh cycle the values converge page-by-page; treat
    them as a monotonically-improving estimate until the cycle
    completes.
    """
    with _counts_lock:
        snapshot = dict(_counts)
    snapshot["total"] = sum(snapshot.get(s, 0) for s in _COUNT_STATUSES)
    return snapshot


def start_counts_refresher(period_s: float = _DEFAULT_COUNTS_PERIOD_S) -> None:
    """Start the background refresher coroutine (idempotent)."""
    global _counts_started  # noqa: PLW0603
    if _counts_started:
        return
    with _counts_lock:
        if _counts_started:
            return
        _counts_started = True

    loop = _ensure_loop()

    async def _refresher() -> None:
        # Single-scan refresher: bucket pending/running in one pass over
        # the A- RowKey range so a workflow that transitions between
        # statuses can't be double-counted (which would have happened
        # with two parallel per-status scans). Page-level progress is
        # written into ``_counts`` so the dashboard sees increments
        # while the scan is mid-flight. Back-pressured by completion
        # rather than fixed cadence to avoid piling up overlapping
        # scans on a slow store.
        while True:
            store = await _aget_store()
            if store is None:
                await asyncio.sleep(period_s)
                continue
            try:
                with _counts_lock:
                    _counts.update(dict.fromkeys(_COUNT_STATUSES, 0))

                def _on_progress(snapshot: dict[str, int]) -> None:
                    for s in _COUNT_STATUSES:
                        _set_count(s, snapshot.get(s, 0))

                final = await store.count_active_workflows_by_status(on_progress=_on_progress)
                for s in _COUNT_STATUSES:
                    _set_count(s, final.get(s, 0))
            except Exception:
                LOG.warning("Workflow count refresher cycle failed", exc_info=True)
            await asyncio.sleep(period_s)

    asyncio.run_coroutine_threadsafe(_refresher(), loop)
    LOG.info("Workflow count refresher started (period=%.1fs)", period_s)
