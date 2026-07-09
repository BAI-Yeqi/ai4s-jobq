# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""End-to-end coordinator tests against Azurite.

Pushes synthetic worker completions to a real Storage Queue and asserts
that :class:`~ai4s.jobq.workflow.coordinator.Coordinator.run_once`
advances persistent state correctly.
"""

from __future__ import annotations

import logging
import socket
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.backend.storage_queue import azurite_conn_str
from ai4s.jobq.entities import Task
from ai4s.jobq.workflow._compact_refs import COMPACT_KWARG
from ai4s.jobq.workflow._compact_refs import decode as _decode_refs
from ai4s.jobq.workflow.coordinator import Coordinator
from ai4s.jobq.workflow.entities import (
    TaskState,
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


def _diamond() -> WorkflowDefinition:
    defn = WorkflowDefinition(
        name="diamond",
        tasks=[
            WorkflowTask(name="A", kwargs={"x": 1}),
            WorkflowTask(name="B", kwargs={"x": 2}, depends_on=["A"]),
            WorkflowTask(name="C", kwargs={"x": 3}, depends_on=["A"]),
            WorkflowTask(name="D", kwargs={"x": 4}, depends_on=["B", "C"]),
        ],
        default_queue="diamond-q",
    )
    defn.validate()
    return defn


async def _push_completion(completion_queue: str, body: WorkflowCompletion) -> None:
    """Push a worker-style completion message to the completion queue.

    Mirrors the worker contract: a serialized ``Task`` whose
    ``kwargs["__completion_body"]`` carries the JSON-serialized
    :class:`WorkflowCompletion`.
    """
    async with JobQ.from_connection_string(
        completion_queue,
        connection_string=azurite_conn_str(),
        exist_ok=True,
    ) as q:
        await q.push({"__completion_body": body.serialize()}, num_retries=0)


@pytest.fixture
async def env() -> AsyncIterator[dict[str, Any]]:
    """Per-test fresh persistence + completion queue with unique names."""
    suffix = uuid.uuid4().hex[:8]
    prefix = f"T{suffix}"
    completion_queue = f"compl-{suffix}"

    store = await WorkflowPersistence.from_connection_string(
        AZURITE_BLOB_TABLE_CONN_STR, prefix=prefix
    )
    try:
        # Pre-create the completion queue so the coordinator's open is a no-op.
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

        # Cleanup queues (best effort).
        try:
            async with JobQ.from_connection_string(
                completion_queue,
                connection_string=azurite_conn_str(),
                exist_ok=True,
            ) as cq:
                await cq.clear()
        except Exception:
            pass
    finally:
        try:
            await store.drop_resources()
        finally:
            await store.close()


async def _drain_queue(queue_name: str) -> list[dict[str, Any]]:
    """Pop every task currently visible on *queue_name*; return their kwargs."""
    out: list[dict[str, Any]] = []
    async with JobQ.from_connection_string(
        queue_name,
        connection_string=azurite_conn_str(),
        exist_ok=True,
    ) as q:
        backend = q._client  # type: ignore[attr-defined]
        try:
            from datetime import timedelta

            messages = await backend.receive_messages_batch(
                max_messages=32, visibility_timeout=timedelta(seconds=10)
            )
        except Exception:
            return out
        for msg in messages:
            try:
                task = Task.deserialize(msg["content"])
                out.append(task.kwargs)
            except Exception:
                LOG.exception("test helper: failed to deserialize drained message")
                continue
            await backend.delete(msg)
    return out


@skip_without_azurite
async def test_diamond_end_to_end(env: dict[str, Any]) -> None:
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]

    # Submit the diamond workflow.
    await store.submit("wf1", _diamond())

    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
        idle_sleep_s=0.01,
    ) as coord:
        # On first tick, the completion queue is empty; coordinator
        # has nothing to do because no completions have arrived yet.
        # We need to manually push A first (since persistence.submit
        # marks root tasks READY but doesn't push them — that's the
        # coordinator's job on a "wake" signal).  For v1, the natural
        # trigger is a completion message; for the very first task we
        # bootstrap by pushing it ourselves below.  In production, the
        # coordinator's run() loop has a "submit-detected" hook (R3);
        # for this test we'll just dispatch root tasks via the
        # coordinator's internal helper to mirror what R3 wires up.

        # Bootstrap: load the runtime, push root tasks, flush.
        loaded = await store.load("wf1")
        assert loaded is not None
        rt, etag = loaded
        root_names = rt.ready_tasks()
        rt.mark_dispatched(root_names)
        await coord._push_ready_tasks(rt, root_names)  # type: ignore[attr-defined]
        await store.flush(rt, etag)

        # A is now in the diamond-q.  Simulate worker pulling A and
        # emitting a completion.
        a_tasks = await _drain_queue("diamond-q")
        assert {t["__workflow_task"] for t in a_tasks} == {"A"}

        await _push_completion(
            cq,
            WorkflowCompletion(
                workflow_id="wf1",
                task_name="A",
                success=True,
                output_ref='{"x":1}',
            ),
        )

        n = await coord.run_once()
        assert n == 1

        # B and C should now be in the queue.
        bc_tasks = await _drain_queue("diamond-q")
        assert {t["__workflow_task"] for t in bc_tasks} == {"B", "C"}
        for kw in bc_tasks:
            assert kw["__workflow_id"] == "wf1"
            # Coordinator emits the compact form; decoding restores
            # the original {parent: ref} dict.
            assert _decode_refs("wf1", kw[COMPACT_KWARG]) == {"A": '{"x":1}'}

        # Complete B and C.
        for name in ("B", "C"):
            await _push_completion(
                cq,
                WorkflowCompletion(
                    workflow_id="wf1",
                    task_name=name,
                    success=True,
                    output_ref=f'{{"name":"{name}"}}',
                ),
            )
        n = await coord.run_once()
        assert n == 2

        d_tasks = await _drain_queue("diamond-q")
        assert {t["__workflow_task"] for t in d_tasks} == {"D"}

        # Complete D.
        await _push_completion(
            cq,
            WorkflowCompletion(
                workflow_id="wf1",
                task_name="D",
                success=True,
            ),
        )
        n = await coord.run_once()
        assert n == 1

    # Final state.
    row = await store.get_index_row("wf1")
    assert row is not None
    assert row.workflow_state == WorkflowState.COMPLETED


@skip_without_azurite
async def test_unknown_workflow_completion_is_acked(env: dict[str, Any]) -> None:
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await _push_completion(
        cq,
        WorkflowCompletion(workflow_id="ghost", task_name="A", success=True),
    )
    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
    ) as coord:
        n = await coord.run_once()
        # Even though the workflow is unknown, the message is parsed
        # and counted (then dropped + acked).
        assert n == 1
        assert coord.stats.orphan_completions == 1


@skip_without_azurite
async def test_failure_cascades_to_workflow_failed(env: dict[str, Any]) -> None:
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _diamond())

    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
    ) as coord:
        # Bootstrap A.
        loaded = await store.load("wf1")
        assert loaded is not None
        rt, etag = loaded
        rt.mark_dispatched(rt.ready_tasks())
        await coord._push_ready_tasks(rt, ["A"])  # type: ignore[attr-defined]
        await store.flush(rt, etag)
        await _drain_queue("diamond-q")

        await _push_completion(
            cq,
            WorkflowCompletion(workflow_id="wf1", task_name="A", success=False, error="boom"),
        )
        await coord.run_once()

    row = await store.get_index_row("wf1")
    assert row is not None
    assert row.workflow_state == WorkflowState.FAILED
    assert row.failed_tasks == 4  # A failed + B/C/D upstream-failed.


@skip_without_azurite
async def test_cancel_loop_marks_active_workflow_cancelled(
    env: dict[str, Any],
) -> None:
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _diamond())

    # Flip the cancel flag via the public API.
    assert await store.request_cancel("wf1") is True

    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
        cancel_poll_interval_s=0.01,
    ) as coord:
        # Hand-drive the cancel application instead of starting the loop.
        await coord._apply_pending_cancels()  # type: ignore[attr-defined]

    row = await store.get_index_row("wf1")
    assert row is not None
    assert row.workflow_state == WorkflowState.CANCELLED


@skip_without_azurite
async def test_duplicate_completion_is_idempotent(env: dict[str, Any]) -> None:
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    await store.submit("wf1", _diamond())

    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
    ) as coord:
        # Bootstrap A.
        loaded = await store.load("wf1")
        assert loaded is not None
        rt, etag = loaded
        rt.mark_dispatched(rt.ready_tasks())
        await coord._push_ready_tasks(rt, ["A"])  # type: ignore[attr-defined]
        await store.flush(rt, etag)
        await _drain_queue("diamond-q")

        # Push the same completion twice.
        for _ in range(2):
            await _push_completion(
                cq,
                WorkflowCompletion(workflow_id="wf1", task_name="A", success=True),
            )
        await coord.run_once()

        # Both messages processed; the second was a no-op.
        assert coord.stats.completions_handled == 2
        assert coord.stats.duplicate_completions == 1

    # Only one set of (B, C) child tasks should have been pushed.
    children = await _drain_queue("diamond-q")
    assert sorted(t["__workflow_task"] for t in children) == ["B", "C"]


@skip_without_azurite
async def test_condition_evaluation_routes_to_skipped(
    env: dict[str, Any],
) -> None:
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]
    defn = WorkflowDefinition(
        name="cond",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(name="B", depends_on=["A"], condition="inputs.A.go == True"),
            WorkflowTask(name="C", depends_on=["A"], condition="inputs.A.go == False"),
        ],
        default_queue="cond-q",
    )
    defn.validate()
    await store.submit("wf1", defn)

    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
    ) as coord:
        loaded = await store.load("wf1")
        assert loaded is not None
        rt, etag = loaded
        rt.mark_dispatched(rt.ready_tasks())
        await coord._push_ready_tasks(rt, ["A"])  # type: ignore[attr-defined]
        await store.flush(rt, etag)
        await _drain_queue("cond-q")

        await _push_completion(
            cq,
            WorkflowCompletion(
                workflow_id="wf1",
                task_name="A",
                success=True,
                output_ref='{"go": true}',
            ),
        )
        await coord.run_once()

    # B should be ready (queued), C should be skipped (NOT queued).
    loaded = await store.load("wf1")
    assert loaded is not None
    rt, _ = loaded
    assert rt.tasks["B"].state in (TaskState.READY, TaskState.RUNNING)
    assert rt.tasks["C"].state == TaskState.SKIPPED


# ---------------------------------------------------------------------------
# Ready-repair sweep
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_ready_repair_sweep_redispatches_stuck_roots(
    env: dict[str, Any],
) -> None:
    """Stuck root tasks (READY past the threshold) are re-pushed by the sweep."""
    import asyncio as _asyncio

    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]

    # Submit, but deliberately skip the push step — this models the
    # crash window where client.submit() flushed the persistence record
    # but never managed to push the queue message.  Root tasks remain
    # READY on disk; without the sweep they would never be picked up.
    await store.submit("wf1", _diamond())

    # Threshold of 0.05s, sleep 0.2s to be comfortably past it.
    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
        ready_repair_threshold_s=0.05,
    ) as coord:
        await _asyncio.sleep(0.2)
        repushed = await coord.sweep_stuck_ready_once()
        assert repushed == 1
        assert coord.stats.ready_tasks_repushed == 1
        assert coord.stats.ready_sweeps == 1

    # The sweep should have pushed root A to the diamond queue.
    pushed = await _drain_queue("diamond-q")
    assert {t["__workflow_task"] for t in pushed} == {"A"}, (
        f"sweep should have re-dispatched A; got {pushed!r}"
    )

    # Durable state should now reflect the dispatch (READY → RUNNING).
    loaded = await store.load("wf1")
    assert loaded is not None
    rt, _ = loaded
    assert rt.tasks["A"].state == TaskState.RUNNING


@skip_without_azurite
async def test_ready_repair_sweep_skips_fresh_workflows(
    env: dict[str, Any],
) -> None:
    """Workflows updated within the threshold are not touched by the sweep."""
    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]

    await store.submit("wf1", _diamond())

    # Generous threshold — the freshly-submitted workflow is well
    # within it, so the sweep should be a no-op.
    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
        ready_repair_threshold_s=3600.0,
    ) as coord:
        repushed = await coord.sweep_stuck_ready_once()
        assert repushed == 0
        assert coord.stats.ready_tasks_repushed == 0

    pushed = await _drain_queue("diamond-q")
    assert pushed == [], f"fresh workflow should not have been swept; got {pushed!r}"


@skip_without_azurite
async def test_ready_repair_sweep_skips_cancel_requested(
    env: dict[str, Any],
) -> None:
    """Cancel-requested workflows are left for the cancel loop, not the sweep."""
    import asyncio as _asyncio

    store: WorkflowPersistence = env["store"]
    cq = env["completion_queue"]

    await store.submit("wf1", _diamond())
    assert await store.request_cancel("wf1") is True

    async with Coordinator(
        store,
        completion_queue=cq,
        queues_account=env["queues_account"],
        ready_repair_threshold_s=0.05,
    ) as coord:
        await _asyncio.sleep(0.2)
        repushed = await coord.sweep_stuck_ready_once()
        assert repushed == 0
        assert coord.stats.ready_tasks_repushed == 0

    pushed = await _drain_queue("diamond-q")
    assert pushed == [], f"cancelled workflow should not be swept; got {pushed!r}"
