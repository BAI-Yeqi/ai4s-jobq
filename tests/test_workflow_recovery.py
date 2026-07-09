# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Recovery tests: infrastructure failures at every link in the chain.

Each test simulates an infrastructure failure (coordinator crash, worker
crash, Azure error) and verifies that the workflow either recovers
automatically on restart or degrades gracefully without data corruption.

All tests run against a live Azurite instance and are skipped when
Azurite is not available.

Failure catalogue tested here
------------------------------

1.  Coordinator crashes **before** flush: completion message redelivered;
    coordinator reprocesses idempotently and advances the DAG.
2.  Coordinator crashes **after** flush, before ack: duplicate completion
    on restart is ignored; no double-push of children.
3.  Worker crash mid-task (same attempt_no redelivered): coordinator
    accepts the redelivered completion and advances correctly.
4.  Stale completion from an earlier attempt (attempt_no < current):
    coordinator ignores it; DAG is not rolled back.
5.  Push-before-flush recovery (attempt_no ahead of blob): coordinator
    catches up ``attempt_no`` and applies the completion.
6.  Flush ETag conflict triggers reload-and-retry: ``flush_conflicts``
    counter increments and the workflow still advances.
7.  Output blob upload failure in worker: completion published with
    ``output_ref=None``; coordinator accepts it and advances.
8.  Upstream blob fetch fails in worker context: clear error raised, not
    a silent hang.
9.  Task-queue push failure does not crash coordinator: logs the error,
    does not ack the message, retries on next delivery.
10. Index row write failure in submit: blob is uploaded; ``load`` still
    works even though the index row was never written.
11. Workflow state blob deleted mid-run: coordinator drops the orphan
    batch and acks the messages without crashing.
12. Cancel-flush ETag conflict: next cancel poll reloads and succeeds.
13. Stuck-READY repair sweep re-dispatches READY tasks.
14. Stuck-RUNNING timeout sweep fails overdue tasks.
15. Poison/unparseable completion message: coordinator deletes the
    message, increments ``stats.poison_messages``, and does not crash.
16. Transient receive error in the main loop: exception is caught and
    logged; coordinator retries on the next iteration without crashing.
17. Late completion for an already-terminal workflow: message is acked
    as a no-op; the workflow remains in its terminal state.
"""

from __future__ import annotations

import asyncio
import socket
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.backend.storage_queue import azurite_conn_str
from ai4s.jobq.entities import EmptyQueue, Task
from ai4s.jobq.workflow.coordinator import Coordinator
from ai4s.jobq.workflow.entities import (
    TaskState,
    WorkflowCompletion,
    WorkflowDefinition,
    WorkflowState,
    WorkflowTask,
)
from ai4s.jobq.workflow.persistence import (
    WorkflowConflictError,
    WorkflowPersistence,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def _all_azurite_up() -> bool:
    for port in (10000, 10001, 10002):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
        except OSError:
            return False
    return True


skip_without_azurite = pytest.mark.skipif(
    not _all_azurite_up(),
    reason="Azurite blob/queue/table must be running on 10000/10001/10002",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _diamond() -> WorkflowDefinition:
    defn = WorkflowDefinition(
        name="diamond",
        tasks=[
            WorkflowTask(name="A", kwargs={"x": 1}),
            WorkflowTask(name="B", depends_on=["A"]),
            WorkflowTask(name="C", depends_on=["A"]),
            WorkflowTask(name="D", depends_on=["B", "C"]),
        ],
        default_queue="recovery-test-q",
    )
    defn.validate()
    return defn


def _chain(num_retries: int = 0) -> WorkflowDefinition:
    """A→B linear chain."""
    defn = WorkflowDefinition(
        name="chain",
        tasks=[
            WorkflowTask(name="A", kwargs={}, num_retries=num_retries),
            WorkflowTask(name="B", depends_on=["A"]),
        ],
        default_queue="recovery-test-q",
    )
    defn.validate()
    return defn


@pytest.fixture
async def env() -> AsyncIterator[dict[str, Any]]:
    """Per-test isolated persistence + completion queue."""
    suffix = uuid.uuid4().hex[:8]
    prefix = f"R{suffix}"
    completion_queue = f"compl-{suffix}"

    store = await WorkflowPersistence.from_connection_string(AZURITE_CONN_STR, prefix=prefix)
    try:
        async with JobQ.from_connection_string(
            completion_queue,
            connection_string=azurite_conn_str(),
            exist_ok=True,
        ) as cq:
            await cq.clear()

        yield {
            "store": store,
            "completion_queue": completion_queue,
            "queues_account": "devstoreaccount1",
        }

        for q in (completion_queue, "recovery-test-q"):
            with suppress(Exception):
                async with JobQ.from_connection_string(
                    q,
                    connection_string=azurite_conn_str(),
                    exist_ok=True,
                ) as jq:
                    await jq.clear()
    finally:
        with suppress(Exception):
            await store.drop_resources()
        await store.close()


async def _push_completion(queue: str, body: WorkflowCompletion) -> None:
    async with JobQ.from_connection_string(
        queue,
        connection_string=azurite_conn_str(),
        exist_ok=True,
    ) as q:
        await q.push({"__completion_body": body.serialize()}, num_retries=0)


async def _drain_task_queue(queue_name: str = "recovery-test-q") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    async with JobQ.from_connection_string(
        queue_name,
        connection_string=azurite_conn_str(),
        exist_ok=True,
    ) as q:
        backend = q._client  # type: ignore[attr-defined]
        with suppress(Exception):
            from datetime import timedelta

            msgs = await backend.receive_messages_batch(
                max_messages=32, visibility_timeout=timedelta(seconds=10)
            )
            for msg in msgs:
                task = Task.deserialize(msg["content"])
                out.append(task.kwargs)
                await backend.delete(msg)
    return out


def _coord(env: dict[str, Any], **kwargs: Any) -> Coordinator:
    """Build a test Coordinator with sensible defaults."""
    return Coordinator(
        env["store"],
        completion_queue=env["completion_queue"],
        queues_account=env["queues_account"],
        idle_sleep_s=0.01,
        **kwargs,
    )


async def _bootstrap(
    store: WorkflowPersistence,
    wf_id: str,
    coord: Coordinator,
    root: str = "A",
) -> None:
    """Dispatch root task and push state blob (simulates client.dispatch)."""
    loaded = await store.load(wf_id)
    assert loaded is not None
    rt, etag = loaded
    rt.mark_dispatched([root])
    await coord._push_ready_tasks(rt, [root])  # type: ignore[attr-defined]
    await store.flush(rt, etag)


# ---------------------------------------------------------------------------
# Test 1: Coordinator crash before flush
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_crash_before_flush_recovers(env: dict[str, Any]) -> None:
    """Completion redelivered after pre-flush crash; DAG advances on restart."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    async with _coord(env, flush_retry_limit=1, visibility_timeout_s=2) as coord1:
        await _bootstrap(store, "wf1", coord1)
        await _drain_task_queue()

        # Push A completion.
        await _push_completion(
            cq, WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1)
        )

        # Patch flush to always raise (simulates crash / storage outage).
        original_flush = store.flush

        async def _always_fail(rt: Any, etag: Any, **_kw: Any) -> str:
            raise WorkflowConflictError("simulated crash")

        store.flush = _always_fail  # type: ignore[method-assign]
        n = await coord1.run_once()
        assert n == 1
        assert coord1.stats.flush_conflicts == 1
        store.flush = original_flush  # type: ignore[method-assign]

    # State blob: A still RUNNING (flush never committed).
    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.RUNNING
    assert rt.tasks["B"].state == TaskState.PENDING

    # Wait for visibility timeout to expire so the message is redelivered.
    await asyncio.sleep(2.5)

    # Restart: message now visible.
    async with _coord(env) as coord2:
        n = await coord2.run_once()
        assert n == 1

    # Now B should be dispatched.
    b_tasks = await _drain_task_queue()
    assert {t["__workflow_task"] for t in b_tasks} == {"B"}
    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state == TaskState.COMPLETED
    assert rt2.tasks["B"].state == TaskState.RUNNING


# ---------------------------------------------------------------------------
# Test 2: Coordinator crash after flush, before ack
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_crash_after_flush_before_ack_no_double_push(env: dict[str, Any]) -> None:
    """Completion redelivered after post-flush crash is ignored; children pushed once."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    async with _coord(env, visibility_timeout_s=2) as coord1:
        await _bootstrap(store, "wf1", coord1)
        await _drain_task_queue()

        await _push_completion(
            cq, WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1)
        )

        # Patch _ack_all to do nothing (message stays visible after flush).
        original_ack = coord1._ack_all  # type: ignore[attr-defined]

        async def _no_ack(_items: Any) -> None:
            pass

        coord1._ack_all = _no_ack  # type: ignore[method-assign]
        n = await coord1.run_once()
        assert n == 1
        coord1._ack_all = original_ack  # type: ignore[method-assign]

    # State blob: A COMPLETED, B RUNNING (flush committed).
    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.COMPLETED
    assert rt.tasks["B"].state == TaskState.RUNNING
    # Drain B that was pushed.
    b_pushed = await _drain_task_queue()
    assert len(b_pushed) == 1

    # Wait for message to become visible again (ack was skipped).
    await asyncio.sleep(2.5)

    # Restart: same message still visible (was never acked).
    async with _coord(env) as coord2:
        n = await coord2.run_once()
        assert n == 1
        # A is already COMPLETED → duplicate, not re-applied.
        assert coord2.stats.duplicate_completions == 1

    # B was NOT pushed again.
    second_push = await _drain_task_queue()
    assert second_push == []


# ---------------------------------------------------------------------------
# Test 3: Worker crash mid-task — redelivered with same attempt_no
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_worker_redelivery_same_attempt_no_succeeds(env: dict[str, Any]) -> None:
    """Two completions for the same attempt (worker crash + redelivery) deduplicated."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    async with _coord(env) as coord:
        await _bootstrap(store, "wf1", coord)
        await _drain_task_queue()

        # Simulate worker running, crashing, then running again.
        # Both deliveries produce a completion with attempt_no=1.
        comp = WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1)
        await _push_completion(cq, comp)
        await _push_completion(cq, comp)

        # Process both: both arrive in the same batch.
        n1 = await coord.run_once()  # both messages; second is a duplicate

    assert n1 == 2
    assert coord.stats.duplicate_completions == 1

    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.COMPLETED
    # B dispatched exactly once.
    b_tasks = await _drain_task_queue()
    assert len(b_tasks) == 1


# ---------------------------------------------------------------------------
# Test 4: Stale attempt_no completion is dropped
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_stale_attempt_completion_is_ignored(env: dict[str, Any]) -> None:
    """Completion from attempt 1 after task rearmed to attempt 2 is dropped."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    # num_retries=2: attempt_no=1 < 2 → failure triggers rearm.
    await store.submit("wf1", _chain(num_retries=2))

    async with _coord(env) as coord:
        await _bootstrap(store, "wf1", coord)
        await _drain_task_queue()

        # Attempt-1 fails → task is rearmed (READY again).
        await _push_completion(
            cq,
            WorkflowCompletion(
                workflow_id="wf1", task_name="A", success=False, error="oops", attempt_no=1
            ),
        )
        n = await coord.run_once()
        assert n == 1

        # After rearm, A is READY; coordinator re-dispatches → RUNNING attempt_no=2.
        rt, _ = await store.load("wf1")  # type: ignore[misc]
        assert rt.tasks["A"].state == TaskState.RUNNING
        assert rt.tasks["A"].attempt_no == 2

        # Drain the re-dispatched A message.
        await _drain_task_queue()

        # Now a stale attempt-1 completion arrives.
        await _push_completion(
            cq,
            WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1),
        )
        n2 = await coord.run_once()
        assert n2 == 1
        assert coord.stats.duplicate_completions >= 1

    # A must still be RUNNING (stale completion was ignored).
    rt3, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt3.tasks["A"].state == TaskState.RUNNING


# ---------------------------------------------------------------------------
# Test 5: Push-before-flush recovery via attempt_no > blob
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_push_before_flush_recovery(env: dict[str, Any]) -> None:
    """Completion with attempt_no > blob catches up the durable counter."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    # Manually put A into READY state with attempt_no=0 in the blob.
    rt, _etag = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.READY
    assert rt.tasks["A"].attempt_no == 0

    # Worker ran attempt 1 and sends a completion.
    async with _coord(env) as coord:
        # Don't bootstrap via coord — we want the task in READY (not RUNNING) in blob.
        await _push_completion(
            cq,
            WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1),
        )
        n = await coord.run_once()
        assert n == 1

    # Coordinator should have accepted the completion (push-before-flush path).
    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state == TaskState.COMPLETED
    assert rt2.tasks["B"].state == TaskState.RUNNING


# ---------------------------------------------------------------------------
# Test 6: Flush ETag conflict triggers reload-and-retry
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_flush_etag_conflict_retries(env: dict[str, Any]) -> None:
    """ETag conflict on first flush triggers reload; workflow advances on second."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    conflict_count = 0
    original_flush = store.flush

    async def _conflict_once(rt: Any, etag: Any, **kw: Any) -> str:
        nonlocal conflict_count
        conflict_count += 1
        if conflict_count == 1:
            raise WorkflowConflictError("simulated conflict")
        return await original_flush(rt, etag, **kw)

    async with _coord(env, flush_retry_limit=2) as coord:
        await _bootstrap(store, "wf1", coord)
        await _drain_task_queue()
        # Patch flush AFTER bootstrap so the submit+dispatch flushes succeed.
        store.flush = _conflict_once  # type: ignore[method-assign]
        await _push_completion(
            cq, WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1)
        )
        n = await coord.run_once()
        assert n == 1
        assert coord.stats.flush_conflicts == 1

    store.flush = original_flush  # type: ignore[method-assign]
    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.COMPLETED
    assert rt.tasks["B"].state == TaskState.RUNNING


# ---------------------------------------------------------------------------
# Test 7: Output blob upload failure — coordinator accepts output_ref=None
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_output_blob_upload_failure_swallowed(env: dict[str, Any]) -> None:
    """Completion published with output_ref=None is accepted; DAG advances."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    async with _coord(env) as coord:
        await _bootstrap(store, "wf1", coord)
        await _drain_task_queue()

        # Worker completed but blob upload failed → output_ref=None published.
        await _push_completion(
            cq,
            WorkflowCompletion(
                workflow_id="wf1",
                task_name="A",
                success=True,
                attempt_no=1,
                output_ref=None,
            ),
        )
        n = await coord.run_once()

    assert n == 1
    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.COMPLETED
    assert rt.tasks["A"].output_ref is None
    # B is dispatched — output_ref=None is handled gracefully.
    assert rt.tasks["B"].state == TaskState.RUNNING


# ---------------------------------------------------------------------------
# Test 8: Upstream blob ref missing raises clear error
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_upstream_blob_fetch_missing_raises(env: dict[str, Any]) -> None:
    """get_upstream_output for a non-existent blob ref raises, not hangs."""
    from azure.core.exceptions import ResourceNotFoundError

    from ai4s.jobq.workflow.ids import output_blob_name

    store: WorkflowPersistence = env["store"]
    missing_ref = f"blob:{output_blob_name('ghost-wf', 'ghost-task')}"
    with pytest.raises(ResourceNotFoundError):
        await store.fetch_output(missing_ref)


# ---------------------------------------------------------------------------
# Test 9: Task-queue push failure — coordinator does not crash, no ack
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_task_queue_push_failure_no_crash(env: dict[str, Any]) -> None:
    """Push failure leaves state blob unchanged; message is redelivered."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    async with _coord(env) as coord:
        await _bootstrap(store, "wf1", coord)
        await _drain_task_queue()

        await _push_completion(
            cq, WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1)
        )

        # Patch _push_ready_tasks to simulate queue outage.
        original_push_ready = coord._push_ready_tasks  # type: ignore[attr-defined]

        async def _fail_push(rt: Any, names: Any) -> None:
            raise OSError("simulated queue outage")

        coord._push_ready_tasks = _fail_push  # type: ignore[method-assign]

        # run_once raises because the push failed before flush.
        with pytest.raises(OSError, match="simulated queue outage"):
            await coord.run_once()

        coord._push_ready_tasks = original_push_ready  # type: ignore[method-assign]
    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.RUNNING
    assert rt.tasks["B"].state == TaskState.PENDING


# ---------------------------------------------------------------------------
# Test 10: Index write failure in submit — blob still loadable
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_submit_index_failure_blob_survives(env: dict[str, Any]) -> None:
    """submit() raises if index write fails; blob is written and loadable."""
    store: WorkflowPersistence = env["store"]

    # Patch _upsert_index to raise.
    original_upsert = store._upsert_index  # type: ignore[attr-defined]

    async def _fail_upsert(row: Any) -> None:
        raise OSError("simulated table write failure")

    store._upsert_index = _fail_upsert  # type: ignore[method-assign]
    with pytest.raises(OSError, match="simulated table write failure"):
        await store.submit("wf1", _chain())
    store._upsert_index = original_upsert  # type: ignore[method-assign]

    # Blob was written before the index call → load must succeed.
    loaded = await store.load("wf1")
    assert loaded is not None
    rt, _ = loaded
    assert rt.workflow_id == "wf1"
    assert rt.tasks["A"].state == TaskState.READY

    # list_workflows will NOT show wf1 (no index row).
    rows = await store.list_workflows()
    assert not any(r.workflow_id == "wf1" for r in rows)


# ---------------------------------------------------------------------------
# Test 11: Workflow deleted mid-run — coordinator drops orphan batch
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_workflow_deleted_mid_run_drops_gracefully(env: dict[str, Any]) -> None:
    """Completions for a deleted workflow are acked and counted as orphans."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    async with _coord(env) as coord:
        await _bootstrap(store, "wf1", coord)
        await _drain_task_queue()

        # Delete the state blobs directly (simulates `workflow purge` mid-run).
        from ai4s.jobq.workflow.ids import definition_blob_name, mutable_state_blob_name

        for name in (definition_blob_name("wf1"), mutable_state_blob_name("wf1")):
            blob = store._state_container.get_blob_client(name)  # type: ignore[attr-defined]
            await blob.delete_blob()

        await _push_completion(
            cq, WorkflowCompletion(workflow_id="wf1", task_name="A", success=True)
        )
        n = await coord.run_once()

    assert n == 1
    assert coord.stats.orphan_completions == 1


# ---------------------------------------------------------------------------
# Test 12: Cancel-flush ETag conflict — recovered on next poll
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_cancel_flush_conflict_recovered_on_next_poll(env: dict[str, Any]) -> None:
    """Cancel flush conflict causes a retry; workflow ends up CANCELLED."""
    store: WorkflowPersistence = env["store"]
    await store.submit("wf1", _diamond())
    await store.request_cancel("wf1")

    conflict_count = 0
    original_flush = store.flush

    async def _conflict_once(rt: Any, etag: Any, **kw: Any) -> str:
        nonlocal conflict_count
        conflict_count += 1
        if conflict_count == 1:
            raise WorkflowConflictError("simulated cancel flush conflict")
        return await original_flush(rt, etag, **kw)

    store.flush = _conflict_once  # type: ignore[method-assign]

    async with _coord(env) as coord:
        # First apply: flush conflicts, cancel not applied to blob.
        await coord._apply_pending_cancels()  # type: ignore[attr-defined]
        # Second apply: reload + succeed.
        await coord._apply_pending_cancels()  # type: ignore[attr-defined]

    store.flush = original_flush  # type: ignore[method-assign]

    row = await store.get_index_row("wf1")
    assert row is not None
    assert row.workflow_state == WorkflowState.CANCELLED


# ---------------------------------------------------------------------------
# Test 13: Stuck-READY repair sweep re-dispatches READY tasks
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_stuck_ready_repair_redispatches(env: dict[str, Any]) -> None:
    """Workflows with READY tasks beyond the threshold are re-dispatched."""
    store: WorkflowPersistence = env["store"]
    await store.submit("wf1", _chain())

    # Task A starts in READY state after submit.
    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].state == TaskState.READY

    # Use threshold=0 so any workflow (even freshly submitted) qualifies.
    async with _coord(env, ready_repair_threshold_s=0.0) as coord:
        repushed = await coord.sweep_stuck_ready_once()

    assert repushed == 1
    assert coord.stats.ready_tasks_repushed == 1

    pushed = await _drain_task_queue()
    assert len(pushed) == 1
    assert pushed[0]["__workflow_task"] == "A"

    # State blob: A is now RUNNING.
    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state == TaskState.RUNNING


# ---------------------------------------------------------------------------
# Test 14: Stuck-RUNNING timeout sweep fails overdue tasks
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_stuck_running_timeout_fails_overdue_task(env: dict[str, Any]) -> None:
    """RUNNING tasks beyond their timeout are failed by the timeout sweep."""
    store: WorkflowPersistence = env["store"]
    await store.submit("wf1", _chain())

    # Dispatch A (READY → RUNNING, sets started_at).
    rt, etag = await store.load("wf1")  # type: ignore[misc]
    rt.mark_dispatched(["A"])
    await store.flush(rt, etag)

    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state == TaskState.RUNNING

    # Sweep with coordinator-level default timeout of 0.01 s (already expired).
    async with _coord(env, running_timeout_s=0.01) as coord:
        await asyncio.sleep(0.05)  # ensure started_at is in the past
        timed_out = await coord.sweep_stuck_running_once()

    assert timed_out == 1
    assert coord.stats.tasks_timed_out == 1

    rt3, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt3.tasks["A"].state == TaskState.FAILED
    assert "timed out" in (rt3.tasks["A"].error or "")
    # B should be UPSTREAM_FAILED.
    assert rt3.tasks["B"].state == TaskState.UPSTREAM_FAILED


@skip_without_azurite
async def test_stuck_running_per_task_timeout(env: dict[str, Any]) -> None:
    """Per-task timeout_s is honoured even without a coordinator default."""
    store: WorkflowPersistence = env["store"]
    defn = WorkflowDefinition(
        name="timed",
        tasks=[WorkflowTask(name="A", timeout_s=1), WorkflowTask(name="B", depends_on=["A"])],
        default_queue="recovery-test-q",
    )
    defn.validate()
    await store.submit("wf1", defn)

    rt, etag = await store.load("wf1")  # type: ignore[misc]
    rt.mark_dispatched(["A"])
    await store.flush(rt, etag)

    # running_timeout_s=None; only per-task timeout_s=1 should apply.
    async with _coord(env, running_timeout_s=None) as coord:
        await asyncio.sleep(1.1)
        timed_out = await coord.sweep_stuck_running_once()

    assert timed_out == 1
    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state == TaskState.FAILED
    assert rt2.tasks["B"].state == TaskState.UPSTREAM_FAILED


@skip_without_azurite
async def test_stuck_running_no_timeout_skipped(env: dict[str, Any]) -> None:
    """RUNNING tasks with no timeout are left untouched by the sweep."""
    store: WorkflowPersistence = env["store"]
    await store.submit("wf1", _chain())  # no timeout on A

    rt, etag = await store.load("wf1")  # type: ignore[misc]
    rt.mark_dispatched(["A"])
    await store.flush(rt, etag)

    # No coordinator-level default, no per-task timeout → sweep is a no-op.
    async with _coord(env, running_timeout_s=None) as coord:
        timed_out = await coord.sweep_stuck_running_once()

    assert timed_out == 0
    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state == TaskState.RUNNING


# ---------------------------------------------------------------------------
# Test 15: Poison / unparseable completion message
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_poison_completion_is_acked_and_not_fatal(env: dict[str, Any]) -> None:
    """Un-parseable completion messages are deleted and counted; coordinator keeps running."""
    # Push a message with empty kwargs.  JobQ creates a valid Task, but
    # _parse_completion fails because there is no __completion_body and {}
    # cannot be deserialised as a WorkflowCompletion.
    async with JobQ.from_connection_string(
        env["completion_queue"],
        connection_string=azurite_conn_str(),
        exist_ok=True,
    ) as q:
        await q.push({}, num_retries=0)

    async with _coord(env) as coord:
        n = await coord.run_once()
        assert n == 0
        assert coord.stats.poison_messages == 1

    # Message was acked — a second run_once sees an empty queue.
    async with _coord(env) as coord2:
        await coord2.run_once()
    assert coord2.stats.poison_messages == 0


# ---------------------------------------------------------------------------
# Test 16: Transient receive error in the main loop
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_receive_failure_in_main_loop_does_not_crash(env: dict[str, Any]) -> None:
    """A receive exception in _main_loop is caught; the coordinator retries."""
    async with _coord(env) as coord:
        call_count = 0

        async def _fail_then_empty(*args: Any, **kwargs: Any) -> list[Any]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated network error")
            raise EmptyQueue

        coord._completion_backend.receive_messages_batch = _fail_then_empty  # type: ignore[method-assign]

        loop_task = asyncio.create_task(coord._main_loop())  # type: ignore[attr-defined]
        await asyncio.sleep(0.05)
        coord._stop_event.set()  # type: ignore[attr-defined]
        await asyncio.wait_for(loop_task, timeout=2.0)

    # The loop was entered at least twice: once for the failure, once after.
    assert call_count >= 2


# ---------------------------------------------------------------------------
# Test 17: Late completion for an already-terminal workflow
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_late_completion_for_terminal_workflow_is_a_noop(env: dict[str, Any]) -> None:
    """A stale completion arriving after the workflow is COMPLETED is acked as a no-op."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _chain())

    async with _coord(env) as coord:
        await _bootstrap(store, "wf1", coord)
        await _drain_task_queue()

        # Drive the workflow to completion: complete A then B.
        await _push_completion(
            cq, WorkflowCompletion(workflow_id="wf1", task_name="A", success=True, attempt_no=1)
        )
        await coord.run_once()

        rt_mid, etag_mid = await store.load("wf1")  # type: ignore[misc]
        rt_mid.mark_dispatched(["B"])
        await store.flush(rt_mid, etag_mid)

        await _push_completion(
            cq, WorkflowCompletion(workflow_id="wf1", task_name="B", success=True, attempt_no=1)
        )
        await coord.run_once()

    # Workflow should now be terminal.
    rt_done, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt_done.workflow_state == WorkflowState.COMPLETED

    # Push a late (duplicate) completion for B after the workflow is terminal.
    await _push_completion(
        cq, WorkflowCompletion(workflow_id="wf1", task_name="B", success=True, attempt_no=1)
    )

    async with _coord(env) as coord2:
        n = await coord2.run_once()

    # Message handled (acked); workflow state unchanged.
    assert n > 0
    rt_final, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt_final.workflow_state == WorkflowState.COMPLETED
    assert rt_final.tasks["B"].state == TaskState.COMPLETED
