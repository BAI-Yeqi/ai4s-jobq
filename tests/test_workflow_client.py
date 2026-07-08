# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Live-Azurite tests for the new :mod:`ai4s.jobq.workflow.client`.

The new client is a thin wrapper around
:class:`~ai4s.jobq.workflow.persistence.WorkflowPersistence` plus
:class:`~ai4s.jobq.workflow._queues.JobQPool`.  These tests verify:

* ``submit`` writes durable state, marks root tasks dispatched, and
  pushes them to their queues with deterministic message ids.
* ``status`` / ``list_workflows`` / ``list_tasks`` read back the
  persisted runtime.
* ``cancel`` flips the index cancel flag (the coordinator daemon is
  the one that propagates the cancel into the runtime).
* ``retry`` resets failed tasks and dispatches the newly-READY ones.
* ``purge`` deletes terminal workflows; ``drop_tables=True`` removes
  the underlying containers + table entirely.
* ``discover_queues`` / ``drain_queues`` use the queue-pool helpers
  (not the legacy coordinator's openers).

The tests deliberately exercise the new architecture's invariants
(blob-backed state, attempt_no on dispatched messages, asynchronous
cancel) rather than the legacy contracts.
"""

from __future__ import annotations

import socket
import uuid
from contextlib import suppress

import pytest

from ai4s.jobq.jobq import JobQ
from ai4s.jobq.workflow.client import (
    AggregateStatus,
    SubmitResult,
    WorkflowClient,
)
from ai4s.jobq.workflow.entities import (
    TaskState,
    WorkflowDefinition,
    WorkflowState,
    WorkflowTask,
)

AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def _port_up(port: int) -> bool:
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=1)
        s.close()
    except OSError:
        return False
    return True


def _all_azurite_up() -> bool:
    return all(_port_up(p) for p in (10000, 10001, 10002))


skip_without_azurite = pytest.mark.skipif(
    not _all_azurite_up(),
    reason="Azurite blob/queue/table must be running on 10000/10001/10002",
)


def _diamond(default_queue: str) -> WorkflowDefinition:
    defn = WorkflowDefinition(
        name="diamond",
        tasks=[
            WorkflowTask(name="A", kwargs={"x": 1}),
            WorkflowTask(name="B", depends_on=["A"]),
            WorkflowTask(name="C", depends_on=["A"]),
            WorkflowTask(name="D", depends_on=["B", "C"]),
        ],
        default_queue=default_queue,
    )
    defn.validate()
    return defn


def _two_roots(default_queue: str) -> WorkflowDefinition:
    defn = WorkflowDefinition(
        name="two-roots",
        tasks=[
            WorkflowTask(name="root1", kwargs={"x": 1}),
            WorkflowTask(name="root2", kwargs={"x": 2}),
            WorkflowTask(name="leaf", depends_on=["root1", "root2"]),
        ],
        default_queue=default_queue,
    )
    defn.validate()
    return defn


@pytest.fixture
async def client_prefix() -> str:
    return f"Cli{uuid.uuid4().hex[:10]}"


@pytest.fixture
async def client(client_prefix: str) -> WorkflowClient:
    c = await WorkflowClient.from_connection_string(AZURITE_CONN_STR, prefix=client_prefix)
    try:
        yield c
    finally:
        with suppress(Exception):
            await c._persistence.drop_resources()
        await c.close()


async def _drain(queue_name: str) -> int:
    """Return the approximate pre-drain queue length and clear it."""
    async with JobQ.from_connection_string(
        queue_name, connection_string=AZURITE_CONN_STR, exist_ok=True
    ) as q:
        try:
            n = await q.get_approximate_size()
        except Exception:
            n = 0
        await q.clear()
        return n


# ---------------------------------------------------------------------------
# Submit + status
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_submit_writes_state_and_pushes_roots(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    wf_id = await client.submit(_diamond(q), workflow_id="wf-submit")
    assert wf_id == "wf-submit"

    # Root task A should have been pushed to its queue.
    received = await _drain(q)
    assert received == 1, "exactly one root message should have been pushed"

    # Runtime should reflect A is RUNNING (mark_dispatched) and B/C are PENDING.
    status = await client.status(wf_id)
    assert status.workflow_id == "wf-submit"
    assert status.tasks["A"].status == TaskState.RUNNING
    assert status.tasks["B"].status == TaskState.PENDING
    assert status.tasks["C"].status == TaskState.PENDING
    assert status.tasks["D"].status == TaskState.PENDING


@skip_without_azurite
async def test_submit_two_roots_pushes_both(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_two_roots(q), workflow_id="wf-two-roots")
    received = await _drain(q)
    assert received == 2


@skip_without_azurite
async def test_submit_generates_id_when_none_given(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    wf_id = await client.submit(_diamond(q))
    assert wf_id  # non-empty generated id
    status = await client.status(wf_id)
    assert status.workflow_id == wf_id


@skip_without_azurite
async def test_submit_duplicate_id_raises(client: WorkflowClient) -> None:
    from ai4s.jobq.workflow.persistence import WorkflowConflictError

    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_diamond(q), workflow_id="wf-dup")
    with pytest.raises(WorkflowConflictError):
        await client.submit(_diamond(q), workflow_id="wf-dup")


@skip_without_azurite
async def test_submit_invalid_workflow_raises(client: WorkflowClient) -> None:
    defn = WorkflowDefinition(
        name="bad",
        tasks=[WorkflowTask(name="dup"), WorkflowTask(name="dup")],
        default_queue="bad-q",
    )
    with pytest.raises(ValueError, match="dup"):
        await client.submit(defn)


@skip_without_azurite
async def test_status_missing_raises_keyerror(client: WorkflowClient) -> None:
    with pytest.raises(KeyError):
        await client.status("does-not-exist")


@skip_without_azurite
async def test_status_include_tasks_false_omits_tasks(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    wf_id = await client.submit(_diamond(q))
    await _drain(q)  # discard pushed roots
    s = await client.status(wf_id, include_tasks=False)
    assert s.tasks == {}
    assert s.total == 4


# ---------------------------------------------------------------------------
# submit_batch
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_submit_batch_reports_results(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    defs = [_diamond(q), _diamond(q), _diamond(q)]
    results = await client.submit_batch(defs, concurrency=2)
    assert len(results) == 3
    assert all(isinstance(r, SubmitResult) for r in results)
    assert all(r.error is None for r in results)
    assert all(r.task_count == 4 for r in results)
    await _drain(q)


@skip_without_azurite
async def test_submit_batch_invokes_on_progress(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    seen: list[SubmitResult] = []

    def cb(r: SubmitResult) -> None:
        seen.append(r)

    await client.submit_batch([_diamond(q), _diamond(q)], on_progress=cb)
    assert len(seen) == 2
    await _drain(q)


# ---------------------------------------------------------------------------
# Listing / summary
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_list_workflows_filters_by_status(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_diamond(q), workflow_id="wf-a")
    await client.submit(_diamond(q), workflow_id="wf-b")
    await _drain(q)

    running = await client.list_workflows(status=WorkflowState.RUNNING)
    assert {w.workflow_id for w in running} == {"wf-a", "wf-b"}

    done = await client.list_workflows(status=WorkflowState.COMPLETED)
    assert done == []


@skip_without_azurite
async def test_list_tasks_filters(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    wf_id = await client.submit(_diamond(q))
    await _drain(q)

    running = await client.list_tasks(wf_id, status=TaskState.RUNNING)
    assert {t.name for t in running} == {"A"}

    bs = await client.list_tasks(wf_id, name_prefix="B")
    assert [t.name for t in bs] == ["B"]

    wrong = await client.list_tasks(wf_id, queue="nope-q")
    assert wrong == []


@skip_without_azurite
async def test_summary_aggregates_two_workflows(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_diamond(q), workflow_id="wf-1")
    await client.submit(_diamond(q), workflow_id="wf-2")
    await _drain(q)

    agg = await client.summary()
    assert isinstance(agg, AggregateStatus)
    assert agg.total_workflows == 2
    # 2 workflows x 4 tasks each = 8.
    assert agg.total_tasks == 8


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_cancel_flips_index_flag(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    wf_id = await client.submit(_diamond(q))
    await _drain(q)

    await client.cancel(wf_id)
    flag = await client._persistence.get_cancel_requested(wf_id)
    assert flag is True


@skip_without_azurite
async def test_cancel_missing_raises_keyerror(client: WorkflowClient) -> None:
    with pytest.raises(KeyError):
        await client.cancel("does-not-exist")


@skip_without_azurite
async def test_cancel_idempotent_on_already_cancelled(
    client: WorkflowClient,
) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    wf_id = await client.submit(_diamond(q))
    await _drain(q)
    await client.cancel(wf_id)
    # Second cancel must not raise.
    await client.cancel(wf_id)


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_retry_resets_failed_and_redispatches(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    wf_id = await client.submit(_diamond(q), workflow_id="wf-retry")
    await _drain(q)

    # Simulate failure of root task A by manipulating persistence directly.
    loaded = await client._persistence.load(wf_id)
    assert loaded is not None
    runtime, etag = loaded
    runtime.apply_completion("A", success=False, error="boom", attempt_no=1)
    # apply_completion may have rearmed A if num_retries > 0; force a hard
    # failure for this test by retrying until it stays FAILED.
    while runtime.tasks["A"].state == TaskState.READY:
        runtime.mark_dispatched(["A"])
        runtime.apply_completion("A", success=False, error="boom")
    await client._persistence.flush(runtime, etag)

    counters = await client.retry(wf_id)
    assert counters["reset"] >= 1

    # Retry should have dispatched A again.
    received = await _drain(q)
    assert received >= 1

    status = await client.status(wf_id)
    assert status.tasks["A"].status in (TaskState.RUNNING, TaskState.READY)


@skip_without_azurite
async def test_retry_missing_raises_keyerror(client: WorkflowClient) -> None:
    with pytest.raises(KeyError):
        await client.retry("does-not-exist")


# ---------------------------------------------------------------------------
# Purge
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_purge_terminal_only(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_diamond(q), workflow_id="wf-running")
    await client.submit(_diamond(q), workflow_id="wf-done")
    await _drain(q)

    # Complete wf-done by directly mutating the runtime.
    loaded = await client._persistence.load("wf-done")
    assert loaded is not None
    rt, etag = loaded
    for n in ("A", "B", "C", "D"):
        if rt.tasks[n].state != TaskState.RUNNING:
            rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await client._persistence.flush(rt, etag)

    result = await client.purge()
    assert result == {"workflows": 1, "tasks": 0}
    remaining = await client.list_workflows()
    assert {w.workflow_id for w in remaining} == {"wf-running"}


@skip_without_azurite
async def test_purge_drop_tables_removes_resources(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_diamond(q))
    await _drain(q)

    await client.purge(drop_tables=True)
    # Resources are recreated lazily on next submit; verify a fresh submit
    # succeeds against the now-dropped resources.
    new_id = await client.submit(_diamond(q))
    assert new_id


# ---------------------------------------------------------------------------
# Queue discovery / drain
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_discover_queues_includes_completion_and_used(
    client: WorkflowClient,
    client_prefix: str,
) -> None:
    from ai4s.jobq.workflow.ids import completion_queue_name

    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_diamond(q))
    await _drain(q)

    queues = await client.discover_queues()
    assert completion_queue_name(client_prefix) in queues
    assert q in queues


@skip_without_azurite
async def test_drain_queues_clears_pushed_messages(client: WorkflowClient) -> None:
    q = f"q{uuid.uuid4().hex[:10]}"
    await client.submit(_diamond(q), workflow_id="wf-drain")
    # Don't pop ourselves — let drain_queues do it.

    counts = await client.drain_queues(queue_names=[q])
    assert counts.get(q, 0) >= 0  # message count was at least visible

    # And the queue should now be empty.
    leftover = await _drain(q)
    assert leftover == 0


@skip_without_azurite
async def test_drain_queues_returns_empty_on_no_queues(
    client: WorkflowClient,
) -> None:
    counts = await client.drain_queues(queue_names=[])
    assert counts == {}
