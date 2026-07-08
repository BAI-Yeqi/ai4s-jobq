# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""End-to-end throughput validation for the workflow stack.

Submits a 1000-task fan-out/fan-in workflow against Azurite and asserts
the coordinator drains completion messages at the ≥50 cps target the
clean-cut design promises (see plan R4).  The single-workflow shape is
deliberately the worst case for state-blob churn:

* ``root`` — one task that fans out to N leaves.
* ``leaf_0001..leaf_NNN`` — N parallel leaf tasks all dependent on
  ``root``.
* ``terminal`` — one task that joins all leaves.

The test does not run real workers; it simulates them by draining the
task queue (so the leaf messages don't pile up) and pushing synthetic
completion messages directly to the coordinator's completion queue.
What is measured is the steady-state batch-flush-ack rate inside
:meth:`Coordinator.run_once`, which is exactly the metric the design
promises to deliver.

Marked ``@pytest.mark.stress`` because bulk-pushing and processing
~1000 messages against Azurite takes ~30-60 seconds.  Run with::

    pytest tests/test_workflow_perf.py --run-stress-tests -v
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.backend.storage_queue import azurite_conn_str
from ai4s.jobq.workflow.coordinator import Coordinator
from ai4s.jobq.workflow.entities import (
    WorkflowCompletion,
    WorkflowDefinition,
    WorkflowState,
    WorkflowTask,
)
from ai4s.jobq.workflow.persistence import WorkflowPersistence

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

LOG = logging.getLogger(__name__)

AZURITE_BLOB_TABLE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)

# Target: ≥50 completions per second on the coordinator hot path
# (see plan.md, "Throughput strategy").
THROUGHPUT_TARGET_CPS = 50.0

# Total task count — root + N leaves + terminal == TASK_COUNT.
TASK_COUNT = 1000
LEAF_COUNT = TASK_COUNT - 2  # 998 leaves
TASK_QUEUE_PREFIX = "perf-q"


def _azurite_up() -> bool:
    for port in (10000, 10001, 10002):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
        except OSError:
            return False
    return True


skip_without_azurite = pytest.mark.skipif(
    not _azurite_up(),
    reason="Azurite blob (10000) + queue (10001) + table (10002) must be running",
)


def _fan_out_in_workflow(leaf_count: int, task_queue: str) -> WorkflowDefinition:
    """Build a 1-root + N-leaf + 1-terminal workflow."""
    leaves = [WorkflowTask(name=f"leaf_{i:04d}", depends_on=["root"]) for i in range(leaf_count)]
    defn = WorkflowDefinition(
        name="perf",
        tasks=[
            WorkflowTask(name="root"),
            *leaves,
            WorkflowTask(
                name="terminal",
                depends_on=[f"leaf_{i:04d}" for i in range(leaf_count)],
            ),
        ],
        default_queue=task_queue,
    )
    defn.validate()
    return defn


@pytest.fixture
async def perf_env() -> AsyncIterator[dict[str, Any]]:
    """Per-test fresh persistence + completion queue + task queue."""
    suffix = uuid.uuid4().hex[:8]
    prefix = f"P{suffix}"
    completion_queue = f"compl-{suffix}"
    task_queue = f"{TASK_QUEUE_PREFIX}-{suffix}"

    store = await WorkflowPersistence.from_connection_string(
        AZURITE_BLOB_TABLE_CONN_STR, prefix=prefix
    )
    try:
        for name in (completion_queue, task_queue):
            async with JobQ.from_connection_string(
                name, connection_string=azurite_conn_str(), exist_ok=True
            ) as q:
                await q.clear()
        yield {
            "store": store,
            "completion_queue": completion_queue,
            "task_queue": task_queue,
            "queues_account": "devstoreaccount1",
        }
        for name in (completion_queue, task_queue):
            try:
                async with JobQ.from_connection_string(
                    name, connection_string=azurite_conn_str(), exist_ok=True
                ) as q:
                    await q.clear()
            except Exception:
                pass
    finally:
        try:
            await store.drop_resources()
        finally:
            await store.close()


async def _drain_queue(queue_name: str) -> int:
    """Pop every visible task on *queue_name*; return the count drained."""
    from datetime import timedelta

    from ai4s.jobq.entities import EmptyQueue

    drained = 0
    async with JobQ.from_connection_string(
        queue_name, connection_string=azurite_conn_str(), exist_ok=True
    ) as q:
        backend = q._client  # type: ignore[attr-defined]
        while True:
            try:
                messages = await backend.receive_messages_batch(
                    max_messages=32, visibility_timeout=timedelta(seconds=30)
                )
            except EmptyQueue:
                break
            if not messages:
                break
            for msg in messages:
                await backend.delete(msg)
                drained += 1
    return drained


async def _push_completion(
    completion_queue: str,
    *,
    workflow_id: str,
    task_name: str,
    attempt_no: int = 1,
    success: bool = True,
) -> None:
    body = WorkflowCompletion(
        workflow_id=workflow_id,
        task_name=task_name,
        attempt_no=attempt_no,
        success=success,
    ).serialize()
    async with JobQ.from_connection_string(
        completion_queue, connection_string=azurite_conn_str(), exist_ok=True
    ) as q:
        await q.push({"__completion_body": body}, num_retries=0)


async def _bulk_push_completions(
    completion_queue: str,
    *,
    workflow_id: str,
    task_names: list[str],
    concurrency: int = 16,
) -> None:
    """Push N completion messages concurrently; setup time, not measured."""
    sem = asyncio.Semaphore(concurrency)

    async with JobQ.from_connection_string(
        completion_queue, connection_string=azurite_conn_str(), exist_ok=True
    ) as q:

        async def _push_one(name: str) -> None:
            async with sem:
                body = WorkflowCompletion(
                    workflow_id=workflow_id,
                    task_name=name,
                    attempt_no=1,
                    success=True,
                ).serialize()
                await q.push({"__completion_body": body}, num_retries=0)

        await asyncio.gather(*[_push_one(n) for n in task_names])


@pytest.mark.stress
@skip_without_azurite
async def test_single_workflow_throughput_meets_target(perf_env: dict[str, Any]) -> None:
    """1000-task fan-out/fan-in workflow processes leaf completions at ≥50 cps."""
    store: WorkflowPersistence = perf_env["store"]
    cq: str = perf_env["completion_queue"]
    queues_account: str = perf_env["queues_account"]
    task_queue: str = perf_env["task_queue"]

    defn = _fan_out_in_workflow(LEAF_COUNT, task_queue=task_queue)
    await store.submit("wf-perf", defn)

    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=queues_account,
        batch_size=32,
        visibility_timeout_s=300.0,  # generous; we ack within seconds per batch.
        idle_sleep_s=0.01,
    ) as coord:
        # Bootstrap: dispatch root, fake its completion, then run_once
        # so the coordinator fans out all LEAF_COUNT leaves to the task
        # queue.  None of this counts toward the cps measurement below.
        loaded = await store.load("wf-perf")
        assert loaded is not None
        rt, etag = loaded
        rt.mark_dispatched(rt.ready_tasks())
        await coord._push_ready_tasks(rt, ["root"])  # type: ignore[attr-defined]
        await store.flush(rt, etag)
        await _drain_queue(task_queue)  # discard the root task message.

        await _push_completion(cq, workflow_id="wf-perf", task_name="root", success=True)
        t_fanout_0 = time.monotonic()
        n = await coord.run_once()
        t_fanout = time.monotonic() - t_fanout_0
        assert n == 1, f"root completion should drive exactly one batch, got {n}"
        LOG.info(
            "fan-out: dispatched %d child tasks in %.2fs (%.0f tasks/s)",
            LEAF_COUNT,
            t_fanout,
            LEAF_COUNT / max(t_fanout, 1e-6),
        )
        # Drain the simulated worker queue so it doesn't pile up.
        drained = await _drain_queue(task_queue)
        assert drained == LEAF_COUNT, (
            f"expected {LEAF_COUNT} leaf tasks on the task queue, got {drained}"
        )

        # Setup: bulk-push LEAF_COUNT leaf completions.  Not measured.
        leaf_names = [f"leaf_{i:04d}" for i in range(LEAF_COUNT)]
        t_push_0 = time.monotonic()
        await _bulk_push_completions(cq, workflow_id="wf-perf", task_names=leaf_names)
        t_push = time.monotonic() - t_push_0
        LOG.info("bulk-pushed %d completions in %.2fs", LEAF_COUNT, t_push)

        # Let Azurite make the messages visible.
        await asyncio.sleep(0.5)

        # ---- Measurement window: drain LEAF_COUNT leaf completions. ----
        start_handled = coord.stats.completions_handled
        target_handled = start_handled + LEAF_COUNT
        idle_ticks = 0
        t0 = time.monotonic()
        while coord.stats.completions_handled < target_handled:
            n = await coord.run_once()
            if n == 0:
                idle_ticks += 1
                if idle_ticks > 200:  # ~2s of idle polls => give up.
                    break
                await asyncio.sleep(0.01)
            else:
                idle_ticks = 0
        elapsed = time.monotonic() - t0
        handled = coord.stats.completions_handled - start_handled

        assert handled == LEAF_COUNT, (
            f"expected to handle {LEAF_COUNT} completions, got {handled} "
            f"(elapsed={elapsed:.2f}s, batches={coord.stats.batches})"
        )

        cps = handled / elapsed
        LOG.info(
            "throughput: %d completions in %.2fs = %.1f cps (batches=%d, flush_conflicts=%d)",
            handled,
            elapsed,
            cps,
            coord.stats.batches,
            coord.stats.flush_conflicts,
        )

        # The plan promises ≥50 cps with headroom; this is the hard gate.
        assert cps >= THROUGHPUT_TARGET_CPS, (
            f"single-workflow throughput {cps:.1f} cps < target "
            f"{THROUGHPUT_TARGET_CPS} cps (handled {handled} in {elapsed:.2f}s, "
            f"batches={coord.stats.batches})"
        )

        # Final sanity: the terminal task should have been queued, and
        # one more completion processes the workflow to COMPLETED.
        terminal_drained = await _drain_queue(task_queue)
        assert terminal_drained == 1, (
            f"expected exactly 1 terminal task on the task queue, got {terminal_drained}"
        )
        await _push_completion(cq, workflow_id="wf-perf", task_name="terminal", success=True)
        await coord.run_once()

    row = await store.get_index_row("wf-perf")
    assert row is not None
    assert row.workflow_state == WorkflowState.COMPLETED, (
        f"workflow should end in COMPLETED, got {row.workflow_state!r}"
    )
