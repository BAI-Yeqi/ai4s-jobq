# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Live-Azurite tests for :mod:`ai4s.jobq.workflow.persistence`.

Skipped if Azurite isn't running on the standard ports.
"""

from __future__ import annotations

import json
import socket
import uuid

import pytest

from ai4s.jobq.workflow.entities import (
    WorkflowDefinition,
    WorkflowState,
    WorkflowTask,
)
from ai4s.jobq.workflow.persistence import (
    WorkflowConflictError,
    WorkflowPersistence,
)

AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def _azurite_up() -> bool:
    for port in (10000, 10002):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
        except OSError:
            return False
    return True


skip_without_azurite = pytest.mark.skipif(
    not _azurite_up(),
    reason="Azurite blob (10000) + table (10002) must be running",
)


def _diamond() -> WorkflowDefinition:
    defn = WorkflowDefinition(
        name="diamond",
        tasks=[
            WorkflowTask(name="A", kwargs={"x": 1}),
            WorkflowTask(name="B", depends_on=["A"]),
            WorkflowTask(name="C", depends_on=["A"]),
            WorkflowTask(name="D", depends_on=["B", "C"]),
        ],
        default_queue="diamond-q",
    )
    defn.validate()
    return defn


@pytest.fixture
async def store() -> WorkflowPersistence:
    """Per-test isolated WorkflowPersistence with a unique prefix.

    The prefix is short (≤ 12 chars to keep blob container names within
    Azure's 63-char limit) and unique per test, so concurrent test runs
    do not collide.
    """
    prefix = f"T{uuid.uuid4().hex[:10]}"
    p = await WorkflowPersistence.from_connection_string(AZURITE_CONN_STR, prefix=prefix)
    try:
        yield p
    finally:
        try:
            await p.drop_resources()
        finally:
            await p.close()


@skip_without_azurite
async def test_submit_creates_blob_and_index_row(store: WorkflowPersistence) -> None:
    runtime = await store.submit("wf1", _diamond())
    assert runtime.workflow_id == "wf1"
    # Index row visible to list_workflows.
    rows = await store.list_workflows()
    assert {r.workflow_id for r in rows} == {"wf1"}
    assert rows[0].total_tasks == 4
    assert rows[0].workflow_state == WorkflowState.RUNNING


@skip_without_azurite
async def test_submit_then_load_round_trip(store: WorkflowPersistence) -> None:
    submitted = await store.submit("wf1", _diamond())
    loaded = await store.load("wf1")
    assert loaded is not None
    rt, etag = loaded
    assert etag
    assert rt.workflow_id == submitted.workflow_id
    assert rt.tasks["A"].state == submitted.tasks["A"].state
    assert rt.workflow_state == submitted.workflow_state


@skip_without_azurite
async def test_submit_duplicate_raises(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    with pytest.raises(WorkflowConflictError):
        await store.submit("wf1", _diamond())


@skip_without_azurite
async def test_load_missing_returns_none(store: WorkflowPersistence) -> None:
    assert await store.load("does-not-exist") is None


@skip_without_azurite
async def test_flush_with_correct_etag_succeeds(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    rt, etag = await store.load("wf1")  # type: ignore[misc]
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=True, output_ref='{"x":1}')
    new_etag = await store.flush(rt, etag)
    assert new_etag
    assert new_etag != etag

    # Reload and confirm state advanced.
    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state.value == "completed"
    assert rt2.tasks["B"].state.value == "ready"


@skip_without_azurite
async def test_flush_with_stale_etag_raises(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    rt_a, etag_a = await store.load("wf1")  # type: ignore[misc]
    rt_b, etag_b = await store.load("wf1")  # type: ignore[misc]
    rt_a.mark_dispatched(["A"])
    new_etag = await store.flush(rt_a, etag_a)
    assert new_etag != etag_a

    rt_b.mark_dispatched(["A"])
    with pytest.raises(WorkflowConflictError):
        await store.flush(rt_b, etag_b)


@skip_without_azurite
async def test_request_cancel_sets_index_flag(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    assert await store.request_cancel("wf1") is True
    assert await store.get_cancel_requested("wf1") is True
    # Idempotent.
    assert await store.request_cancel("wf1") is False


@skip_without_azurite
async def test_request_cancel_missing_workflow_returns_false(
    store: WorkflowPersistence,
) -> None:
    assert await store.request_cancel("ghost") is False
    assert await store.get_cancel_requested("ghost") is False


@skip_without_azurite
async def test_list_cancel_requested_active(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    await store.submit("wf2", _diamond())
    await store.request_cancel("wf1")
    ids = await store.list_cancel_requested_active()
    assert ids == ["wf1"]


@skip_without_azurite
async def test_load_inherits_cancel_flag_from_index(
    store: WorkflowPersistence,
) -> None:
    await store.submit("wf1", _diamond())
    await store.request_cancel("wf1")
    rt, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt.cancel_requested is True


@skip_without_azurite
async def test_list_workflows_filter_by_status(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    await store.submit("wf2", _diamond())

    # Mark wf2 terminal by completing all tasks.
    rt, etag = await store.load("wf2")  # type: ignore[misc]
    for n in ("A", "B", "C", "D"):
        rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await store.flush(rt, etag)

    active = await store.list_workflows(status_filter=[WorkflowState.RUNNING])
    assert {r.workflow_id for r in active} == {"wf1"}

    terminal = await store.list_workflows(status_filter=[WorkflowState.COMPLETED])
    assert {r.workflow_id for r in terminal} == {"wf2"}


@skip_without_azurite
async def test_total_workflows(store: WorkflowPersistence) -> None:
    assert await store.total_workflows() == 0
    await store.submit("wf1", _diamond())
    await store.submit("wf2", _diamond())
    assert await store.total_workflows() == 2


@skip_without_azurite
async def test_stash_and_fetch_output(store: WorkflowPersistence) -> None:
    payload = json.dumps({"big": "x" * 100}).encode()
    ref = await store.stash_output("wf1", "task-A", payload)
    assert ref.startswith("blob:wf1/")
    fetched = await store.fetch_output(ref)
    assert fetched == {"big": "x" * 100}


@skip_without_azurite
async def test_fetch_output_inline_path(store: WorkflowPersistence) -> None:
    fetched = await store.fetch_output('{"hello": "world"}')
    assert fetched == {"hello": "world"}


@skip_without_azurite
async def test_delete_removes_blob_and_index_row(
    store: WorkflowPersistence,
) -> None:
    await store.submit("wf1", _diamond())
    await store.stash_output("wf1", "task-A", b'{"x":1}')
    assert await store.delete("wf1") is True
    assert await store.load("wf1") is None
    assert await store.get_index_row("wf1") is None
    # Idempotent.
    assert await store.delete("wf1") is False


@skip_without_azurite
async def test_purge_terminal_only(store: WorkflowPersistence) -> None:
    await store.submit("wf-running", _diamond())
    await store.submit("wf-done", _diamond())

    # Complete wf-done.
    rt, etag = await store.load("wf-done")  # type: ignore[misc]
    for n in ("A", "B", "C", "D"):
        rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await store.flush(rt, etag)

    n = await store.purge(terminal_only=True)
    assert n == 1
    rows = await store.list_workflows()
    assert {r.workflow_id for r in rows} == {"wf-running"}


@skip_without_azurite
async def test_large_workflow_round_trip(store: WorkflowPersistence) -> None:
    """A 200-task linear pipeline serializes to ~50 KB; round-trip through
    the blob and verify task ordering / counts are preserved.
    """
    tasks = [WorkflowTask(name="t0")]
    tasks.extend(WorkflowTask(name=f"t{i}", depends_on=[f"t{i - 1}"]) for i in range(1, 200))
    defn = WorkflowDefinition(name="pipeline", tasks=tasks)
    defn.validate()
    await store.submit("wf-big", defn)
    rt, _ = await store.load("wf-big")  # type: ignore[misc]
    assert len(rt.tasks) == 200
    assert rt.tasks["t199"].parents == ("t198",)


# ---------------------------------------------------------------------------
# R3.0 — user-facing client conveniences
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_get_workflow_status_no_tasks(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    status = await store.get_workflow_status("wf1", include_tasks=False)
    assert status is not None
    assert status.workflow_id == "wf1"
    assert status.name == "diamond"
    assert status.total == 4
    assert status.tasks == {}


@skip_without_azurite
async def test_get_workflow_status_with_tasks(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    status = await store.get_workflow_status("wf1", include_tasks=True)
    assert status is not None
    assert set(status.tasks.keys()) == {"A", "B", "C", "D"}
    assert status.tasks["A"].depends_on == []
    assert set(status.tasks["D"].depends_on) == {"B", "C"}


@skip_without_azurite
async def test_get_workflow_status_missing_returns_none(
    store: WorkflowPersistence,
) -> None:
    assert await store.get_workflow_status("nope") is None
    assert await store.get_workflow_status("nope", include_tasks=False) is None


@skip_without_azurite
async def test_list_workflow_statuses(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    await store.submit("wf2", _diamond())

    rt, etag = await store.load("wf2")  # type: ignore[misc]
    for n in ("A", "B", "C", "D"):
        rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await store.flush(rt, etag)

    all_statuses = await store.list_workflow_statuses()
    assert {s.workflow_id for s in all_statuses} == {"wf1", "wf2"}
    for s in all_statuses:
        assert s.tasks == {}

    only_done = await store.list_workflow_statuses(status=WorkflowState.COMPLETED)
    assert [s.workflow_id for s in only_done] == ["wf2"]


@skip_without_azurite
async def test_list_recent_terminal_orders_newest_first(
    store: WorkflowPersistence,
) -> None:
    import asyncio as _asyncio

    await store.submit("wf-old", _diamond())
    rt, etag = await store.load("wf-old")  # type: ignore[misc]
    for n in ("A", "B", "C", "D"):
        rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await store.flush(rt, etag)

    await _asyncio.sleep(0.05)

    await store.submit("wf-new", _diamond())
    rt, etag = await store.load("wf-new")  # type: ignore[misc]
    for n in ("A", "B", "C", "D"):
        rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await store.flush(rt, etag)

    recent = await store.list_recent_terminal(limit=10)
    assert [s.workflow_id for s in recent] == ["wf-new", "wf-old"]


@skip_without_azurite
async def test_list_recent_terminal_filters_non_terminal(
    store: WorkflowPersistence,
) -> None:
    await store.submit("wf-running", _diamond())
    await store.submit("wf-done", _diamond())
    rt, etag = await store.load("wf-done")  # type: ignore[misc]
    for n in ("A", "B", "C", "D"):
        rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await store.flush(rt, etag)

    recent = await store.list_recent_terminal(limit=10)
    assert [s.workflow_id for s in recent] == ["wf-done"]


@skip_without_azurite
async def test_list_recent_terminal_respects_limit(
    store: WorkflowPersistence,
) -> None:
    for i in range(3):
        wf = f"wf{i}"
        await store.submit(wf, _diamond())
        rt, etag = await store.load(wf)  # type: ignore[misc]
        for n in ("A", "B", "C", "D"):
            rt.mark_dispatched([n])
            rt.apply_completion(n, success=True)
        await store.flush(rt, etag)

    recent = await store.list_recent_terminal(limit=2)
    assert len(recent) == 2


@skip_without_azurite
async def test_list_tasks_filters(store: WorkflowPersistence) -> None:
    from ai4s.jobq.workflow.entities import TaskState

    await store.submit("wf1", _diamond())
    rt, etag = await store.load("wf1")  # type: ignore[misc]
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=True)
    await store.flush(rt, etag)

    all_tasks = await store.list_tasks("wf1")
    assert {t.name for t in all_tasks} == {"A", "B", "C", "D"}

    completed = await store.list_tasks("wf1", status=TaskState.COMPLETED)
    assert [t.name for t in completed] == ["A"]

    ready = await store.list_tasks("wf1", status=TaskState.READY)
    assert {t.name for t in ready} == {"B", "C"}

    diamond_q = await store.list_tasks("wf1", queue="diamond-q")
    assert {t.name for t in diamond_q} == {"A", "B", "C", "D"}

    other = await store.list_tasks("wf1", queue="other-q")
    assert other == []

    b_prefix = await store.list_tasks("wf1", name_prefix="B")
    assert [t.name for t in b_prefix] == ["B"]


@skip_without_azurite
async def test_list_tasks_missing_workflow_returns_empty(
    store: WorkflowPersistence,
) -> None:
    assert await store.list_tasks("nope") == []


@skip_without_azurite
async def test_reset_failed_tasks(store: WorkflowPersistence) -> None:
    await store.submit("wf1", _diamond())
    rt, etag = await store.load("wf1")  # type: ignore[misc]
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=False, error="boom")
    await store.flush(rt, etag)

    counters = await store.reset_failed_tasks("wf1")
    assert counters["reset"] >= 1
    assert counters["now_ready"] >= 1

    rt2, _ = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].state.value == "ready"
    assert rt2.tasks["A"].error is None


@skip_without_azurite
async def test_reset_failed_tasks_missing_raises(
    store: WorkflowPersistence,
) -> None:
    from ai4s.jobq.workflow.persistence import WorkflowNotFoundError

    with pytest.raises(WorkflowNotFoundError):
        await store.reset_failed_tasks("nope")


@skip_without_azurite
async def test_summary_aggregates(store: WorkflowPersistence) -> None:
    await store.submit("wf-running", _diamond())
    await store.submit("wf-done", _diamond())
    rt, etag = await store.load("wf-done")  # type: ignore[misc]
    for n in ("A", "B", "C", "D"):
        rt.mark_dispatched([n])
        rt.apply_completion(n, success=True)
    await store.flush(rt, etag)

    summary = await store.summary()
    assert summary["total_workflows"] == 2
    assert summary["completed_tasks"] >= 4
    assert "completed" in summary["workflows"]


@skip_without_azurite
async def test_split_blob_format_roundtrip(store: WorkflowPersistence) -> None:
    """Verify the split-blob format preserves kwargs and topology across load/flush."""
    submitted = await store.submit("wf1", _diamond())
    assert submitted.tasks["A"].kwargs == {"x": 1}

    # Load, mutate, flush, reload — kwargs must survive.
    rt, etag = await store.load("wf1")  # type: ignore[misc]
    assert rt.tasks["A"].kwargs == {"x": 1}
    assert rt.tasks["D"].parents == ("B", "C")

    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=True, output_ref='{"v":1}')
    new_etag = await store.flush(rt, etag)

    rt2, etag2 = await store.load("wf1")  # type: ignore[misc]
    assert rt2.tasks["A"].kwargs == {"x": 1}  # static survived
    assert rt2.tasks["A"].output_ref == '{"v":1}'  # mutable updated
    assert rt2.tasks["B"].state.value == "ready"
    assert etag2 == new_etag


@skip_without_azurite
async def test_definition_blob_not_rewritten_on_flush(store: WorkflowPersistence) -> None:
    """Flush only writes the state blob; definition blob stays untouched."""
    from ai4s.jobq.workflow.ids import definition_blob_name

    await store.submit("wf1", _diamond())
    def_client = store._state_container.get_blob_client(  # type: ignore[attr-defined]
        definition_blob_name("wf1")
    )
    props_before = await def_client.get_blob_properties()

    # Flush state multiple times.
    rt, etag = await store.load("wf1")  # type: ignore[misc]
    rt.mark_dispatched(["A"])
    etag = await store.flush(rt, etag)
    rt.apply_completion("A", success=True)
    await store.flush(rt, etag)

    props_after = await def_client.get_blob_properties()
    # Definition blob's ETag must not change across flushes.
    assert props_before.etag == props_after.etag
