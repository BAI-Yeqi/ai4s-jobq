# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Unit tests for :mod:`ai4s.jobq.workflow.state`.

Pure in-memory tests — no Azure dependency.
"""

from __future__ import annotations

import json

from ai4s.jobq.workflow.entities import (
    DepPolicy,
    ResultPolicy,
    TaskState,
    WorkflowDefinition,
    WorkflowState,
    WorkflowTask,
)
from ai4s.jobq.workflow.state import WorkflowRuntime


def _diamond() -> WorkflowDefinition:
    defn = WorkflowDefinition(
        name="diamond",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(name="B", depends_on=["A"]),
            WorkflowTask(name="C", depends_on=["A"]),
            WorkflowTask(name="D", depends_on=["B", "C"]),
        ],
    )
    defn.validate()
    return defn


def test_from_definition_sets_roots_to_ready() -> None:
    rt = WorkflowRuntime.from_definition("wf1", _diamond())
    assert rt.tasks["A"].state == TaskState.READY
    assert rt.tasks["B"].state == TaskState.PENDING
    assert rt.tasks["C"].state == TaskState.PENDING
    assert rt.tasks["D"].state == TaskState.PENDING
    assert rt.workflow_state == WorkflowState.RUNNING
    assert rt.ready_tasks() == ["A"]


def test_full_happy_path_diamond() -> None:
    rt = WorkflowRuntime.from_definition("wf1", _diamond())
    rt.mark_dispatched(["A"])
    assert rt.tasks["A"].state == TaskState.RUNNING

    res = rt.apply_completion("A", success=True, output_ref='{"x":1}')
    assert sorted(res.ready) == ["B", "C"]
    assert not res.candidates
    assert not res.ignored_duplicate

    rt.mark_dispatched(["B", "C"])
    rt.apply_completion("B", success=True, output_ref='{"y":2}')
    res = rt.apply_completion("C", success=True, output_ref='{"z":3}')
    assert res.ready == ["D"]
    assert rt.tasks["D"].state == TaskState.READY

    rt.mark_dispatched(["D"])
    rt.apply_completion("D", success=True)
    assert rt.is_terminal()
    assert rt.workflow_state == WorkflowState.COMPLETED


def test_duplicate_completion_is_noop() -> None:
    rt = WorkflowRuntime.from_definition("wf1", _diamond())
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=True, output_ref='{"x":1}')
    # B and C are now READY.  A duplicate completion for A must not
    # corrupt counters.
    res = rt.apply_completion("A", success=True, output_ref='{"x":2}')
    assert res.ignored_duplicate
    assert res.ready == []
    assert rt.tasks["B"].state == TaskState.READY
    assert rt.tasks["B"].completed_parents == 1


def test_failure_cascades_upstream_failed() -> None:
    rt = WorkflowRuntime.from_definition("wf1", _diamond())
    rt.mark_dispatched(["A"])
    res = rt.apply_completion("A", success=False, error="boom")
    # B, C, and D all become UPSTREAM_FAILED in cascade.
    assert sorted(res.skipped_cascade) == ["B", "C", "D"]
    for name in ("B", "C", "D"):
        assert rt.tasks[name].state == TaskState.UPSTREAM_FAILED
    assert rt.workflow_state == WorkflowState.FAILED


def test_any_policy_eager_satisfaction() -> None:
    defn = WorkflowDefinition(
        name="any",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(name="B"),
            WorkflowTask(name="C", depends_on=["A", "B"], dep_policy=DepPolicy.ANY),
        ],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A", "B"])
    res = rt.apply_completion("A", success=True)
    assert "C" in res.ready
    assert rt.tasks["C"].state == TaskState.READY
    # Late completion of B must not re-enqueue C or corrupt counters.
    res2 = rt.apply_completion("B", success=True)
    assert res2.ready == []


def test_all_settled_waits_for_all_parents() -> None:
    defn = WorkflowDefinition(
        name="settled",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(name="B"),
            WorkflowTask(
                name="C",
                depends_on=["A", "B"],
                dep_policy=DepPolicy.ALL_SETTLED,
            ),
        ],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A", "B"])
    res = rt.apply_completion("A", success=True)
    assert "C" not in res.ready  # Still waiting for B.
    res = rt.apply_completion("B", success=False, error="x")
    # All settled, at least one succeeded → C ready.
    assert "C" in res.ready


def test_int_dep_policy() -> None:
    defn = WorkflowDefinition(
        name="int",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(name="B"),
            WorkflowTask(name="C"),
            WorkflowTask(name="D", depends_on=["A", "B", "C"], dep_policy=2),
        ],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A", "B", "C"])
    res = rt.apply_completion("A", success=True)
    assert "D" not in res.ready
    res = rt.apply_completion("B", success=True)
    assert "D" in res.ready


def test_condition_creates_pending_candidate() -> None:
    defn = WorkflowDefinition(
        name="cond",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(
                name="B",
                depends_on=["A"],
                condition="inputs.A.go == True",
            ),
        ],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])
    res = rt.apply_completion("A", success=True, output_ref='{"go": true}')
    # Dep policy is met but condition not yet evaluated → candidate, not ready.
    assert res.candidates == ["B"]
    assert res.ready == []
    assert rt.tasks["B"].state == TaskState.PENDING

    # Coordinator then evaluates and promotes:
    rt.set_ready("B")
    assert rt.tasks["B"].state == TaskState.READY


def test_mark_skipped_cascades_downstream() -> None:
    defn = WorkflowDefinition(
        name="cond",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(name="B", depends_on=["A"], condition="inputs.A.go == True"),
            WorkflowTask(name="C", depends_on=["B"]),
        ],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=True, output_ref='{"go": false}')
    cascaded = rt.mark_skipped("B", reason="condition false")
    assert "C" in cascaded
    assert rt.tasks["B"].state == TaskState.SKIPPED
    # C has an UPSTREAM_FAILED parent (B SKIPPED is dep-satisfied, but
    # then dep policy ALL with no failures should make C ready).
    # Actually: SKIPPED counts as DEP_SATISFIED for downstream gating,
    # so C should become READY in this graph.
    # Re-read intent: SKIPPED parents satisfy the dep, so C is READY.
    assert rt.tasks["C"].state == TaskState.READY


def test_cancel_marks_pending_and_running_remains() -> None:
    rt = WorkflowRuntime.from_definition("wf", _diamond())
    rt.mark_dispatched(["A"])
    running = rt.request_cancel()
    assert running == ["A"]
    assert rt.tasks["B"].state == TaskState.CANCELLED
    assert rt.tasks["C"].state == TaskState.CANCELLED
    assert rt.tasks["D"].state == TaskState.CANCELLED
    assert rt.tasks["A"].state == TaskState.RUNNING

    # Worker eventually completes A; workflow freezes as CANCELLED.
    rt.apply_completion("A", success=True)
    assert rt.workflow_state == WorkflowState.CANCELLED


def test_retry_resets_failed_to_pending() -> None:
    rt = WorkflowRuntime.from_definition("wf", _diamond())
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=False, error="boom")
    assert rt.workflow_state == WorkflowState.FAILED
    counts = rt.reset_failed_tasks()
    assert counts["reset"] == 4  # A failed + B/C/D upstream_failed.
    # A is root → now READY again.  B/C/D still PENDING.
    assert rt.tasks["A"].state == TaskState.READY
    assert rt.tasks["B"].state == TaskState.PENDING
    assert rt.workflow_state == WorkflowState.RUNNING


def test_max_parallelism_limits_ready_dispatch() -> None:
    defn = WorkflowDefinition(
        name="mp",
        tasks=[WorkflowTask(name=f"t{i}") for i in range(5)],
        max_parallelism=2,
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    # 5 root tasks, all READY.  With max_parallelism=2 and 0 in flight,
    # ready_tasks() returns at most 2.
    ready = rt.ready_tasks()
    assert len(ready) == 2
    rt.mark_dispatched(ready)
    # Now 2 in-flight, max_parallelism=2 → 0 more can be dispatched.
    assert rt.ready_tasks() == []
    # Complete one → one more slot frees up.
    rt.apply_completion(ready[0], success=True)
    assert len(rt.ready_tasks()) == 1


def test_to_json_round_trip_preserves_state() -> None:
    rt = WorkflowRuntime.from_definition("wf", _diamond())
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=True, output_ref='{"x":1}')

    js = rt.to_json()
    # Must be JSON-serializable.
    s = json.dumps(js)
    rt2 = WorkflowRuntime.from_json(json.loads(s))
    assert rt2.workflow_id == rt.workflow_id
    assert rt2.workflow_state == rt.workflow_state
    assert set(rt2.tasks) == set(rt.tasks)
    for name in rt.tasks:
        a = rt.tasks[name]
        b = rt2.tasks[name]
        assert a.state == b.state
        assert a.output_ref == b.output_ref
        assert a.completed_parents == b.completed_parents
        assert a.failed_parents == b.failed_parents


def test_to_status_counts_match() -> None:
    rt = WorkflowRuntime.from_definition("wf", _diamond())
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=True)
    rt.mark_dispatched(["B", "C"])
    status = rt.to_status()
    assert status.total == 4
    assert status.completed == 1  # A
    assert status.running == 2  # B, C
    assert status.pending == 1  # D
    assert status.status == WorkflowState.RUNNING


def test_workflow_failed_all_tasks_succeed_policy() -> None:
    defn = WorkflowDefinition(
        name="strict",
        tasks=[
            WorkflowTask(name="A"),
            WorkflowTask(name="B", depends_on=["A"], dep_policy=DepPolicy.ALL_SETTLED),
        ],
        result_policy=ResultPolicy.ALL_TASKS_SUCCEED,
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])
    rt.apply_completion("A", success=False, error="x")
    # Under ALL_SETTLED, B is still candidate; coordinator sees it.
    # But the parent failed → B should still run (all settled, A failed
    # but B has no other parent so settled count == 1, completed == 0
    # so dep_satisfied is False — under ALL_SETTLED with 1 parent that
    # failed, _is_dep_satisfied returns False, and unsatisfiable is True
    # → UPSTREAM_FAILED).
    assert rt.tasks["B"].state == TaskState.UPSTREAM_FAILED
    # ALL_TASKS_SUCCEED + a single failed task → workflow FAILED.
    assert rt.workflow_state == WorkflowState.FAILED


def test_recovery_path_pending_to_completed_direct() -> None:
    """If coordinator crashes after pushing a task but before flushing,
    the task is still PENDING in state.  When the completion arrives,
    apply_completion should accept PENDING → COMPLETED directly.
    """
    rt = WorkflowRuntime.from_definition("wf", _diamond())
    # No mark_dispatched call — simulating "crash before flush".
    res = rt.apply_completion("A", success=True, output_ref='{"x":1}')
    assert not res.ignored_duplicate
    assert rt.tasks["A"].state == TaskState.COMPLETED
    assert sorted(res.ready) == ["B", "C"]


def test_attempt_no_stale_completion_ignored() -> None:
    """Completion from an earlier attempt arriving late must be ignored."""
    defn = WorkflowDefinition(
        name="wf",
        tasks=[WorkflowTask(name="A", num_retries=3)],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])  # attempt_no=1, RUNNING
    rt.apply_completion("A", success=False, error="boom", attempt_no=1)
    # Re-armed to READY because num_retries=3 > attempt_no=1.
    assert rt.tasks["A"].state == TaskState.READY
    rt.mark_dispatched(["A"])  # attempt_no=2, RUNNING

    # A late attempt-1 completion arrives — must be ignored.
    res = rt.apply_completion("A", success=True, output_ref='{"x":1}', attempt_no=1)
    assert res.ignored_duplicate
    assert rt.tasks["A"].state == TaskState.RUNNING


def test_attempt_no_future_recovery_bumps_durable() -> None:
    """Push-before-flush: completion.attempt_no > task.attempt_no → bump + apply."""
    rt = WorkflowRuntime.from_definition("wf", _diamond())
    # Simulate the coordinator pushing without flushing: state stays
    # READY in durable storage even though the worker is running
    # attempt_no=1.  When the completion lands, durable attempt_no is 0.
    assert rt.tasks["A"].state == TaskState.READY
    assert rt.tasks["A"].attempt_no == 0

    res = rt.apply_completion("A", success=True, output_ref='{"x":1}', attempt_no=1)
    assert not res.ignored_duplicate
    assert rt.tasks["A"].state == TaskState.COMPLETED
    assert rt.tasks["A"].attempt_no == 1
    assert sorted(res.ready) == ["B", "C"]


def test_retry_rearm_failure_with_budget() -> None:
    """Failure with retries remaining → state goes back to READY."""
    defn = WorkflowDefinition(
        name="wf",
        tasks=[WorkflowTask(name="A", num_retries=2)],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])
    assert rt.tasks["A"].attempt_no == 1

    res = rt.apply_completion("A", success=False, error="transient", attempt_no=1)
    assert res.retry_rearmed is True
    assert res.ready == ["A"]
    assert rt.tasks["A"].state == TaskState.READY
    assert rt.tasks["A"].attempt_no == 1  # not yet bumped — only re-dispatch bumps

    rt.mark_dispatched(["A"])
    assert rt.tasks["A"].attempt_no == 2

    res = rt.apply_completion("A", success=True, output_ref='{"r":1}', attempt_no=2)
    assert rt.tasks["A"].state == TaskState.COMPLETED


def test_retry_rearm_failure_without_budget_fails() -> None:
    """Last attempt fails → terminal FAILED, no rearm."""
    defn = WorkflowDefinition(
        name="wf",
        tasks=[WorkflowTask(name="A", num_retries=1)],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])
    res = rt.apply_completion("A", success=False, error="dead", attempt_no=1)
    assert not res.retry_rearmed
    assert rt.tasks["A"].state == TaskState.FAILED
    assert rt.workflow_state == WorkflowState.FAILED


def test_retry_rearm_suppressed_when_cancelled() -> None:
    """Cancel-requested workflow does not rearm failed tasks for retry."""
    defn = WorkflowDefinition(
        name="wf",
        tasks=[WorkflowTask(name="A", num_retries=3)],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])
    rt.cancel_requested = True
    res = rt.apply_completion("A", success=False, error="x", attempt_no=1)
    assert not res.retry_rearmed
    assert rt.tasks["A"].state == TaskState.FAILED


def test_duplicate_completion_during_rearm_window_ignored() -> None:
    """Same-attempt completion arriving twice during the rearm window is ignored."""
    defn = WorkflowDefinition(
        name="wf",
        tasks=[WorkflowTask(name="A", num_retries=3)],
    )
    defn.validate()
    rt = WorkflowRuntime.from_definition("wf", defn)
    rt.mark_dispatched(["A"])
    res1 = rt.apply_completion("A", success=False, error="x", attempt_no=1)
    assert res1.retry_rearmed
    assert rt.tasks["A"].state == TaskState.READY

    # A duplicate attempt-1 completion arrives before redispatch.  The
    # task is in READY but attempt_no still 1, so this is a same-attempt
    # duplicate — ignore.
    res2 = rt.apply_completion("A", success=True, output_ref='{"x":2}', attempt_no=1)
    assert res2.ignored_duplicate
    assert rt.tasks["A"].state == TaskState.READY  # untouched
