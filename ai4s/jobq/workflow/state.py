# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""In-memory state for an active workflow.

This module is pure data — it does no IO and has no Azure dependency.
:class:`WorkflowRuntime` is mutated by the coordinator (and, narrowly,
by the client during retry) and persisted by
``ai4s.jobq.workflow.persistence``.

Invariants
----------

1. ``apply_completion`` is idempotent: a duplicate completion message
   for a task already in a terminal state is a no-op (returns ``[]``).
2. ``apply_completion`` accepts the "skip the RUNNING step" transition
   from PENDING/READY/READY_PENDING_BUDGET directly to a terminal
   state — this happens when a coordinator restarts after pushing a
   task but before flushing its state.
3. Cascading dependency resolution (parent succeeds → child becomes
   candidate; parent fails → child becomes UPSTREAM_FAILED; etc.) is
   driven entirely from per-task ``completed_parents`` /
   ``failed_parents`` counters that are recomputable from the set of
   terminal parent states.
4. Condition evaluation is *not* done here; the coordinator calls
   :meth:`WorkflowRuntime.candidate_children` to learn which children
   have their dep-policy satisfied, evaluates each candidate's
   ``condition`` expression with parent outputs, then calls
   :meth:`set_ready` or :meth:`mark_skipped` accordingly.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ai4s.jobq.workflow.entities import (
    DepPolicy,
    ResultPolicy,
    TaskState,
    TaskStatus,
    WorkflowState,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from ai4s.jobq.workflow.entities import WorkflowDefinition

__all__ = [
    "AdvanceResult",
    "TaskRuntime",
    "WorkflowRuntime",
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class TaskRuntime:
    """In-memory state of a single task within a workflow.

    ``kwargs_raw`` stores kwargs as pre-serialized JSON bytes to avoid
    keeping decoded dicts in memory for all 30k+ tasks.  Use
    :attr:`kwargs` to decode on demand (only needed at dispatch time).
    Once a task reaches a terminal state it can no longer be dispatched,
    so ``kwargs_raw`` is freed (set to ``b""``) to cut coordinator
    memory; the immutable definition blob retains the original kwargs.
    """

    name: str
    parents: tuple[str, ...]
    children: tuple[str, ...]
    queue: str
    num_retries: int
    dep_policy: DepPolicy | int
    timeout_s: int | None
    condition: str | None
    kwargs_raw: bytes

    state: TaskState = TaskState.PENDING
    completed_parents: int = 0
    failed_parents: int = 0
    output_ref: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    attempt_no: int = 0

    @property
    def kwargs(self) -> dict[str, Any]:
        """Decode kwargs from pre-serialized bytes (cached per access)."""
        return json.loads(self.kwargs_raw) if self.kwargs_raw else {}

    @property
    def is_terminal(self) -> bool:
        return TaskState.is_terminal(self.state)

    def to_status(self) -> TaskStatus:
        """Build the user-facing :class:`TaskStatus`."""
        retries_remaining = max(0, self.num_retries - self.attempt_no)
        dep_policy_s = str(self.dep_policy)
        return TaskStatus(
            name=self.name,
            status=self.state,
            depends_on=list(self.parents),
            depended_by=list(self.children),
            dep_policy=dep_policy_s,
            completed_deps=self.completed_parents,
            failed_deps=self.failed_parents,
            queue=self.queue,
            output_ref=self.output_ref,
            error=self.error,
            started_at=self.started_at,
            completed_at=self.completed_at,
            retries_remaining=retries_remaining,
            task_timeout_s=self.timeout_s,
            updated_at=self.completed_at or self.started_at,
            attempt_no=self.attempt_no,
        )


@dataclass(slots=True)
class AdvanceResult:
    """What changed after a single ``apply_completion`` call.

    Returned to the coordinator so it can:
    * push newly-READY tasks to their JobQ queue,
    * evaluate conditions on ``candidates`` whose dep-policy is met
      but whose condition gate has not yet been checked,
    * detect duplicate completions cheaply (``ignored_duplicate``).
    """

    ready: list[str] = field(default_factory=list)
    candidates: list[str] = field(default_factory=list)
    skipped_cascade: list[str] = field(default_factory=list)
    ignored_duplicate: bool = False
    retry_rearmed: bool = False


class WorkflowRuntime:
    """All in-memory state for one workflow.

    Constructed once via :meth:`from_definition` at submission, then
    mutated by the coordinator and persisted as a single blob.
    """

    __slots__ = (
        "cancel_requested",
        "created_at",
        "default_queue",
        "error",
        "max_parallelism",
        "name",
        "result_policy",
        "tasks",
        "updated_at",
        "workflow_id",
        "workflow_state",
    )

    def __init__(
        self,
        *,
        workflow_id: str,
        name: str,
        default_queue: str,
        result_policy: ResultPolicy,
        max_parallelism: int | None,
        tasks: dict[str, TaskRuntime],
        workflow_state: WorkflowState = WorkflowState.PENDING,
        cancel_requested: bool = False,
        created_at: datetime,
        updated_at: datetime,
        error: str | None = None,
    ) -> None:
        self.workflow_id = workflow_id
        self.name = name
        self.default_queue = default_queue
        self.result_policy = result_policy
        self.max_parallelism = max_parallelism
        self.tasks = tasks
        self.workflow_state = workflow_state
        self.cancel_requested = cancel_requested
        self.created_at = created_at
        self.updated_at = updated_at
        self.error = error

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_definition(
        cls,
        workflow_id: str,
        defn: WorkflowDefinition,
        *,
        now: datetime | None = None,
    ) -> WorkflowRuntime:
        """Build a fresh runtime from a user-submitted definition.

        Root tasks (no parents) start in :attr:`TaskState.READY`; all
        others start in :attr:`TaskState.PENDING`.
        """
        ts = now if now is not None else _utcnow()
        _intern = sys.intern

        # parent → list of children
        children_map: dict[str, list[str]] = {t.name: [] for t in defn.tasks}
        for t in defn.tasks:
            for dep in t.depends_on:
                children_map.setdefault(dep, []).append(t.name)

        tasks: dict[str, TaskRuntime] = {}
        for t in defn.tasks:
            name = _intern(t.name)
            tasks[name] = TaskRuntime(
                name=name,
                parents=tuple(_intern(d) for d in t.depends_on),
                children=tuple(_intern(c) for c in children_map.get(t.name, ())),
                queue=_intern(t.queue or defn.default_queue),
                num_retries=t.num_retries,
                dep_policy=t.dep_policy,
                timeout_s=t.timeout_s if t.timeout_s is not None else defn.default_task_timeout_s,
                condition=t.condition,
                kwargs_raw=json.dumps(t.kwargs).encode() if t.kwargs else b"",
            )

        # Root tasks → READY (no condition possible because conditions
        # require a dependency; validated at submission).
        for tr in tasks.values():
            if not tr.parents:
                tr.state = TaskState.READY

        rt = cls(
            workflow_id=workflow_id,
            name=defn.name,
            default_queue=defn.default_queue,
            result_policy=defn.result_policy,
            max_parallelism=defn.max_parallelism,
            tasks=tasks,
            cancel_requested=False,
            created_at=ts,
            updated_at=ts,
        )
        rt._recompute_workflow_state()
        return rt

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def ready_tasks(self) -> list[str]:
        """Names of tasks currently in :attr:`TaskState.READY`.

        Honours :attr:`max_parallelism`: at most
        ``max_parallelism - inflight`` names are returned (extras stay
        in READY but the coordinator should not dispatch them this
        iteration).  Pass ``None`` for unlimited.
        """
        all_ready = [n for n, t in self.tasks.items() if t.state == TaskState.READY]
        if self.max_parallelism is None:
            return all_ready
        inflight = sum(1 for t in self.tasks.values() if t.state == TaskState.RUNNING)
        free = max(0, self.max_parallelism - inflight)
        return all_ready[:free]

    def is_terminal(self) -> bool:
        return WorkflowState.is_terminal(self.workflow_state)

    def queues_used(self) -> list[str]:
        """Sorted unique queue names referenced by any task."""
        return sorted({t.queue for t in self.tasks.values()})

    def parent_output_refs(self, child_name: str) -> dict[str, str | None]:
        """Map ``parent_name → output_ref`` for *child_name*'s parents.

        Used by the coordinator to build a child task's
        ``__upstream_outputs_compact`` payload (see
        :mod:`ai4s.jobq.workflow._compact_refs`).
        """
        ch = self.tasks[child_name]
        return {p: self.tasks[p].output_ref for p in ch.parents}

    def parent_outputs_inline(self, child_name: str) -> dict[str, Any] | None:
        """Decode inline parent outputs for condition evaluation.

        Returns a dict ``{parent_name: deserialized_output}`` if every
        parent's output is inline (or absent — treated as ``None``).
        Returns ``None`` if any parent's output is blob-stashed and
        therefore unavailable without IO.
        """
        from ai4s.jobq.workflow.ids import is_blob_ref

        ch = self.tasks[child_name]
        out: dict[str, Any] = {}
        for p in ch.parents:
            ref = self.tasks[p].output_ref
            if ref is None:
                out[p] = None
            elif is_blob_ref(ref):
                return None
            else:
                try:
                    out[p] = json.loads(ref)
                except json.JSONDecodeError:
                    out[p] = ref
        return out

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def mark_dispatched(self, names: list[str]) -> None:
        """Flip ``READY → RUNNING`` for *names* and bump ``started_at``."""
        ts = _utcnow()
        for n in names:
            t = self.tasks.get(n)
            if t is None or t.state != TaskState.READY:
                continue
            t.state = TaskState.RUNNING
            t.started_at = ts
            t.attempt_no += 1
        self.updated_at = ts
        self._recompute_workflow_state()

    def set_ready(self, name: str) -> None:
        """Promote a PENDING task to READY (used after a satisfied condition)."""
        t = self.tasks.get(name)
        if t is None or t.state != TaskState.PENDING:
            return
        t.state = TaskState.READY
        self.updated_at = _utcnow()
        self._recompute_workflow_state()

    def mark_skipped(self, name: str, reason: str | None = None) -> list[str]:
        """Mark a task SKIPPED (condition was False or upstream blob-only).

        Returns names whose state transitioned as a result of cascading
        the skip into descendants.
        """
        t = self.tasks.get(name)
        if t is None or t.is_terminal:
            return []
        t.state = TaskState.SKIPPED
        t.completed_at = _utcnow()
        t.kwargs_raw = b""
        if reason:
            t.error = reason
        self.updated_at = t.completed_at
        cascaded = self._propagate_parent_completion(name)
        self._recompute_workflow_state()
        return cascaded

    def apply_completion(
        self,
        name: str,
        *,
        success: bool,
        output_ref: str | None = None,
        error: str | None = None,
        attempt_no: int | None = None,
    ) -> AdvanceResult:
        """Apply a single worker completion to the runtime.

        Idempotent: if the task is already terminal, returns an
        ``AdvanceResult`` with ``ignored_duplicate=True`` and no
        cascade.  Otherwise transitions the task to its terminal state
        and propagates the change to children, returning the lists
        the coordinator needs to dispatch downstream work.

        Args:
            name: Task name within this workflow.
            success: Whether the task succeeded.
            output_ref: Inline JSON or ``blob:`` reference for the
                task's output, or ``None``.
            error: Failure description (ignored when ``success``).
            attempt_no: Which attempt produced this completion (1-based).
                When given, the runtime treats completions with an
                ``attempt_no`` lower than the task's current
                ``attempt_no`` as stale duplicates and ignores them.
                ``None`` (pre-R3 workers) means "always apply".

        Retry semantics:
            On failure with ``task.attempt_no < task.num_retries``, the
            task is *re-armed* — its state goes back to READY and it
            appears in ``AdvanceResult.ready`` and the result's
            ``retry_rearmed=True``.  The coordinator then re-dispatches
            it (which bumps ``attempt_no`` again).  This contains all
            workflow-level retry policy in one place — workers never
            decide what to retry.
        """
        t = self.tasks.get(name)
        if t is None:
            return AdvanceResult(ignored_duplicate=True)
        if t.is_terminal:
            return AdvanceResult(ignored_duplicate=True)

        # Stale completions from earlier attempts: e.g. attempt-1 fails →
        # retry rearms task → attempt-2 dispatched and running.  An
        # attempt-1 completion arriving late must be ignored.
        if attempt_no is not None and attempt_no < t.attempt_no:
            return AdvanceResult(ignored_duplicate=True)

        # During the rearm window (failure processed, awaiting redispatch
        # back to RUNNING), the task's state is READY but a duplicate of
        # the same attempt could still arrive.  Reject it — only RUNNING
        # tasks can accept completions of the *same* attempt.  This
        # matches "completion is the transition out of RUNNING" semantics.
        #
        # The READY-but-fresh-attempt case is push-before-flush recovery:
        # the coordinator pushed (bumping attempt_no in-memory) but
        # crashed before flushing.  We detect that via
        # ``attempt_no > task.attempt_no`` below and accept.
        if t.state == TaskState.READY:
            if attempt_no is None:
                # Pre-R3 worker / direct unit-test path: trust the
                # completion (the task was definitely pushed, otherwise
                # the worker couldn't have produced this message).
                pass
            elif attempt_no > t.attempt_no:
                # Push-before-flush recovery — accept.
                pass
            else:
                return AdvanceResult(ignored_duplicate=True)
        elif t.state != TaskState.RUNNING:
            return AdvanceResult(ignored_duplicate=True)

        # Push-before-flush recovery: catch up the durable attempt_no
        # to whatever the worker actually ran.
        if attempt_no is not None and attempt_no > t.attempt_no:
            t.attempt_no = attempt_no

        ts = _utcnow()
        if success:
            t.state = TaskState.COMPLETED
            t.output_ref = output_ref
            t.error = None
            t.completed_at = ts
            t.kwargs_raw = b""
            self.updated_at = ts
            cascaded = self._propagate_parent_completion(name)
        else:
            # Failure path: decide between retry rearm and FAILED.
            retries_left = t.attempt_no < t.num_retries
            if retries_left and not self.cancel_requested:
                # Re-arm for retry.  Stays as READY; coordinator picks
                # it up via the ``ready`` list and re-dispatches.
                t.state = TaskState.READY
                t.error = error
                t.started_at = None
                self.updated_at = ts
                self._recompute_workflow_state()
                return AdvanceResult(ready=[name], retry_rearmed=True)

            t.state = TaskState.FAILED
            t.output_ref = output_ref
            t.error = error or "task failed"
            t.completed_at = ts
            t.kwargs_raw = b""
            self.updated_at = ts
            cascaded = self._propagate_parent_completion(name)

        ready: list[str] = []
        candidates: list[str] = []
        skipped: list[str] = []
        for n in cascaded:
            s = self.tasks[n].state
            if s == TaskState.READY:
                ready.append(n)
            elif s == TaskState.PENDING:
                # Was promoted to candidate (dep-policy met) but still
                # has a condition to evaluate.  Coordinator picks it up
                # via candidates.
                candidates.append(n)
            elif s in (TaskState.UPSTREAM_FAILED, TaskState.SKIPPED):
                skipped.append(n)

        self._recompute_workflow_state()
        return AdvanceResult(ready=ready, candidates=candidates, skipped_cascade=skipped)

    def candidate_children(self, parent: str) -> list[str]:
        """Names of children of *parent* whose dep-policy is now met.

        For coordinator use after cancellation / retry where it wants
        to re-evaluate readiness without applying a completion.
        """
        p = self.tasks.get(parent)
        if p is None:
            return []
        return [c for c in p.children if self._is_dep_satisfied(c)]

    def request_cancel(self) -> list[str]:
        """Flag the workflow for cancellation.

        Marks every PENDING / READY / READY_PENDING_BUDGET task as
        CANCELLED, leaves RUNNING tasks alone (their workers learn via
        the index-table flag and can abort cooperatively).

        Returns names of RUNNING tasks (so the coordinator can decide
        what — if anything — to do with them).

        Idempotent: safe to call repeatedly; only mutates state on
        the first application (detected by a still-cancellable task
        being present), regardless of how ``cancel_requested`` was
        set (e.g. hydrated from the index table on load).
        """
        cancellable = (
            TaskState.PENDING,
            TaskState.READY,
            TaskState.READY_PENDING_BUDGET,
        )
        if self.cancel_requested and not any(t.state in cancellable for t in self.tasks.values()):
            # Cancel was already fully applied (idempotent path).  Still
            # recompute to ensure the state reflects CANCELLING vs CANCELLED
            # correctly (e.g. after loading a legacy state blob).
            self._recompute_workflow_state()
            return [n for n, t in self.tasks.items() if t.state == TaskState.RUNNING]
        self.cancel_requested = True
        ts = _utcnow()
        running: list[str] = []
        for t in self.tasks.values():
            if t.state in cancellable:
                t.state = TaskState.CANCELLED
                t.completed_at = ts
                t.kwargs_raw = b""
            elif t.state == TaskState.RUNNING:
                running.append(t.name)
        self.updated_at = ts
        self._recompute_workflow_state()
        return running

    def reset_failed_tasks(self) -> dict[str, int]:
        """Reset every failed / upstream-failed / cancelled task.

        Used by ``client.retry()``.  Failed tasks become PENDING again
        (or READY if all parents are dep-satisfied).  Counters are
        recomputed from scratch.  Returns counters with names matching
        the legacy implementation:

        ``{"reset": n, "now_ready": m, "still_pending": k}``
        """
        eligible = (TaskState.FAILED, TaskState.UPSTREAM_FAILED, TaskState.CANCELLED)
        reset = 0
        for t in self.tasks.values():
            if t.state in eligible:
                t.state = TaskState.PENDING
                t.completed_at = None
                t.started_at = None
                t.error = None
                t.attempt_no = 0
                reset += 1
        self.cancel_requested = False
        self._recompute_parent_counters()

        now_ready = 0
        still_pending = 0
        for t in self.tasks.values():
            if t.state != TaskState.PENDING:
                continue
            if self._is_dep_satisfied(t.name):
                # No condition gate at this point; the coordinator will
                # evaluate when it loads the runtime.  Leave as PENDING
                # iff there is a condition; otherwise promote to READY.
                if t.condition is None:
                    t.state = TaskState.READY
                    now_ready += 1
                else:
                    still_pending += 1
            else:
                still_pending += 1

        self.updated_at = _utcnow()
        self._recompute_workflow_state()
        return {"reset": reset, "now_ready": now_ready, "still_pending": still_pending}

    # ------------------------------------------------------------------
    # Internal cascade machinery
    # ------------------------------------------------------------------

    def _propagate_parent_completion(self, parent_name: str) -> list[str]:
        """Walk children of *parent_name*, update their counters and
        possibly transition them.  Returns names whose state changed.
        """
        parent = self.tasks[parent_name]
        changed: list[str] = []
        for child_name in parent.children:
            child = self.tasks[child_name]
            if child.is_terminal:
                continue
            # Recompute this child's counters from scratch — cheap
            # (parents tuple is small) and immune to duplicate
            # completions.
            self._recompute_child_counters(child)
            transitioned = self._transition_child(child)
            if transitioned:
                changed.append(child_name)
                if child.state in (TaskState.UPSTREAM_FAILED, TaskState.SKIPPED):
                    # Cascade skip/upstream-fail downstream too.
                    changed.extend(self._propagate_parent_completion(child_name))
        return changed

    def _recompute_child_counters(self, child: TaskRuntime) -> None:
        cp = 0
        fp = 0
        for p in child.parents:
            ps = self.tasks[p].state
            if TaskState.is_dep_satisfied(ps):
                cp += 1
            elif TaskState.is_dep_failed(ps):
                fp += 1
        child.completed_parents = cp
        child.failed_parents = fp

    def _recompute_parent_counters(self) -> None:
        for t in self.tasks.values():
            self._recompute_child_counters(t)

    def _is_dep_satisfied(self, child_name: str) -> bool:
        """Is *child_name*'s dep policy now satisfied (independent of
        condition evaluation)?"""
        child = self.tasks[child_name]
        if not child.parents:
            return True
        policy = child.dep_policy
        if isinstance(policy, int):
            return child.completed_parents >= policy
        if policy == DepPolicy.ALL:
            return child.completed_parents == len(child.parents)
        if policy == DepPolicy.ANY:
            return child.completed_parents >= 1
        if policy == DepPolicy.ALL_SETTLED:
            settled = child.completed_parents + child.failed_parents
            return settled == len(child.parents) and child.completed_parents >= 1
        return False

    def _is_dep_unsatisfiable(self, child_name: str) -> bool:
        """Has *child_name*'s dep policy become *impossible* to satisfy?"""
        child = self.tasks[child_name]
        if not child.parents:
            return False
        policy = child.dep_policy
        total = len(child.parents)
        settled = child.completed_parents + child.failed_parents
        terminal_failed = child.failed_parents
        if isinstance(policy, int):
            return (total - terminal_failed) < policy
        if policy == DepPolicy.ALL:
            return terminal_failed > 0
        if policy == DepPolicy.ANY:
            return terminal_failed == total
        if policy == DepPolicy.ALL_SETTLED:
            return settled == total and child.completed_parents == 0
        return False

    def _transition_child(self, child: TaskRuntime) -> bool:
        """Transition *child* based on its (just-recomputed) counters.

        Returns ``True`` iff the child's state changed.  Three outcomes:

        * UPSTREAM_FAILED — dep policy is impossible to satisfy.
        * "candidate" — dep policy met; if no condition, promote to
          READY immediately; otherwise leave as PENDING for the
          coordinator to evaluate via :meth:`set_ready` / :meth:`mark_skipped`.
        * No change — still waiting for more parents.
        """
        if child.state != TaskState.PENDING:
            return False
        if self._is_dep_unsatisfiable(child.name):
            child.state = TaskState.UPSTREAM_FAILED
            child.completed_at = _utcnow()
            child.error = "upstream task(s) failed"
            child.kwargs_raw = b""
            return True
        if self._is_dep_satisfied(child.name):
            if child.condition is None:
                child.state = TaskState.READY
                return True
            # Condition needs evaluation by the coordinator; signal
            # candidacy without changing state — the AdvanceResult will
            # surface this via the ``candidates`` list because the child
            # is PENDING but has its deps satisfied.  Mark the change as
            # "transitioned" so the caller picks it up.
            return True
        return False

    def _recompute_workflow_state(self) -> None:
        """Compute :attr:`workflow_state` from the current task states."""
        if not self.tasks:
            self.workflow_state = WorkflowState.PENDING
            return

        any_active = False
        any_failed = False
        any_completed = False
        any_dispatched_or_done = False  # READY/RUNNING/terminal
        for t in self.tasks.values():
            s = t.state
            if TaskState.is_active(s):
                any_active = True
                if s in (TaskState.READY, TaskState.RUNNING, TaskState.READY_PENDING_BUDGET):
                    any_dispatched_or_done = True
            else:
                any_dispatched_or_done = True
                if TaskState.is_dep_failed(s):
                    any_failed = True
                else:
                    any_completed = True

        if self.cancel_requested and any_active is False:
            # Everything has settled after cancel; freeze as CANCELLED
            # so the UI distinguishes "cancelled" from natural failure.
            self.workflow_state = WorkflowState.CANCELLED
            return

        if self.cancel_requested and any_active:
            # Cancel was applied but workers are still running; surface
            # this intermediate state so the UI shows "cancelling".
            self.workflow_state = WorkflowState.CANCELLING
            return

        if any_active:
            # Some task is still doing work.
            if not any_dispatched_or_done:
                self.workflow_state = WorkflowState.PENDING
            else:
                self.workflow_state = WorkflowState.RUNNING
            return

        # All tasks terminal — decide COMPLETED vs FAILED per policy.
        if self._is_workflow_failed(any_failed=any_failed, any_completed=any_completed):
            self.workflow_state = WorkflowState.FAILED
            return
        self.workflow_state = WorkflowState.COMPLETED

    def _is_workflow_failed(self, *, any_failed: bool, any_completed: bool) -> bool:
        if self.result_policy == ResultPolicy.ALL_TASKS_SUCCEED:
            return any_failed
        # ALL_SINKS_SUCCEED: workflow fails iff any sink (no children)
        # is in a dep-failed terminal state.
        for t in self.tasks.values():
            if t.children:
                continue
            if TaskState.is_dep_failed(t.state):
                return True
        # If no tasks succeeded at all (all skipped / cancelled), treat
        # as failed too — otherwise a fully-skipped workflow would be
        # reported as COMPLETED which is unhelpful.
        return not any_completed

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_status(self) -> WorkflowStatus:
        counts: dict[str, int] = {
            "total": 0,
            "completed": 0,
            "running": 0,
            "failed": 0,
            "pending": 0,
            "skipped": 0,
        }
        tasks_out: dict[str, TaskStatus] = {}
        for n, t in self.tasks.items():
            counts["total"] += 1
            tasks_out[n] = t.to_status()
            s = t.state
            if s == TaskState.COMPLETED:
                counts["completed"] += 1
            elif s == TaskState.RUNNING:
                counts["running"] += 1
            elif s in (TaskState.FAILED, TaskState.UPSTREAM_FAILED, TaskState.CANCELLED):
                counts["failed"] += 1
            elif s == TaskState.SKIPPED:
                counts["skipped"] += 1
            else:
                counts["pending"] += 1
        return WorkflowStatus(
            workflow_id=self.workflow_id,
            name=self.name,
            status=self.workflow_state,
            total=counts["total"],
            completed=counts["completed"],
            running=counts["running"],
            failed=counts["failed"],
            pending=counts["pending"],
            skipped=counts["skipped"],
            default_queue=self.default_queue,
            queues_used=self.queues_used(),
            created_at=self.created_at,
            updated_at=self.updated_at,
            error=self.error,
            tasks=tasks_out,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "v": 1,
            "workflow_id": self.workflow_id,
            "name": self.name,
            "default_queue": self.default_queue,
            "result_policy": str(self.result_policy),
            "max_parallelism": self.max_parallelism,
            "workflow_state": str(self.workflow_state),
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "error": self.error,
            "tasks": {
                n: {
                    "name": t.name,
                    "parents": list(t.parents),
                    "children": list(t.children),
                    "queue": t.queue,
                    "num_retries": t.num_retries,
                    "dep_policy": (
                        t.dep_policy if isinstance(t.dep_policy, int) else str(t.dep_policy)
                    ),
                    "timeout_s": t.timeout_s,
                    "condition": t.condition,
                    "kwargs": t.kwargs,
                    "state": str(t.state),
                    "completed_parents": t.completed_parents,
                    "failed_parents": t.failed_parents,
                    "output_ref": t.output_ref,
                    "error": t.error,
                    "started_at": t.started_at.isoformat() if t.started_at else None,
                    "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                    "attempt_no": t.attempt_no,
                }
                for n, t in self.tasks.items()
            },
        }

    # ------------------------------------------------------------------
    # Split serialization (definition + state)
    # ------------------------------------------------------------------

    def to_definition_json(self) -> dict[str, Any]:
        """Serialize the immutable definition (topology, kwargs, queues).

        Written once at submission and never updated.  Paired with
        :meth:`to_state_json` for the mutable portion.
        """
        return {
            "v": 1,
            "workflow_id": self.workflow_id,
            "name": self.name,
            "default_queue": self.default_queue,
            "result_policy": str(self.result_policy),
            "max_parallelism": self.max_parallelism,
            "created_at": self.created_at.isoformat(),
            "tasks": {
                n: {
                    "parents": list(t.parents),
                    "children": list(t.children),
                    "queue": t.queue,
                    "num_retries": t.num_retries,
                    "dep_policy": (
                        t.dep_policy if isinstance(t.dep_policy, int) else str(t.dep_policy)
                    ),
                    "timeout_s": t.timeout_s,
                    "condition": t.condition,
                    "kwargs": t.kwargs,
                }
                for n, t in self.tasks.items()
            },
        }

    def to_state_json(self) -> dict[str, Any]:
        """Serialize only the mutable state (task states, counters, timestamps).

        This is the portion rewritten on every coordinator flush.
        """
        return {
            "v": 1,
            "workflow_id": self.workflow_id,
            "workflow_state": str(self.workflow_state),
            "cancel_requested": self.cancel_requested,
            "updated_at": self.updated_at.isoformat(),
            "error": self.error,
            "tasks": {
                n: {
                    "state": str(t.state),
                    "completed_parents": t.completed_parents,
                    "failed_parents": t.failed_parents,
                    "output_ref": t.output_ref,
                    "error": t.error,
                    "started_at": t.started_at.isoformat() if t.started_at else None,
                    "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                    "attempt_no": t.attempt_no,
                }
                for n, t in self.tasks.items()
            },
        }

    @classmethod
    def from_split_json(cls, definition: dict[str, Any], state: dict[str, Any]) -> WorkflowRuntime:
        """Reconstruct a runtime from separate definition and state dicts.

        This is the inverse of :meth:`to_definition_json` +
        :meth:`to_state_json`.
        """
        def_v = definition.get("v", 1)
        state_v = state.get("v", 1)
        if def_v != 1 or state_v != 1:
            raise ValueError(
                f"WorkflowRuntime: unsupported split schema versions (def={def_v}, state={state_v})"
            )

        def _dt(s: str | None) -> datetime | None:
            return datetime.fromisoformat(s) if s else None

        _intern = sys.intern
        tasks: dict[str, TaskRuntime] = {}
        for n, td in definition["tasks"].items():
            dp_raw = td["dep_policy"]
            dp: DepPolicy | int = dp_raw if isinstance(dp_raw, int) else DepPolicy(dp_raw)
            # Merge mutable state from the state dict
            ts = state["tasks"].get(n, {})
            name = _intern(n)
            tasks[name] = TaskRuntime(
                name=name,
                parents=tuple(_intern(p) for p in td["parents"]),
                children=tuple(_intern(c) for c in td["children"]),
                queue=_intern(td["queue"]),
                num_retries=td["num_retries"],
                dep_policy=dp,
                timeout_s=td.get("timeout_s"),
                condition=td.get("condition"),
                kwargs_raw=json.dumps(td.get("kwargs", {})).encode(),
                state=TaskState(ts["state"]) if ts.get("state") else TaskState.PENDING,
                completed_parents=ts.get("completed_parents", 0),
                failed_parents=ts.get("failed_parents", 0),
                output_ref=ts.get("output_ref"),
                error=ts.get("error"),
                started_at=_dt(ts.get("started_at")),
                completed_at=_dt(ts.get("completed_at")),
                attempt_no=ts.get("attempt_no", 0),
            )

        created_at = _dt(definition["created_at"])
        updated_at = _dt(state["updated_at"])
        if created_at is None or updated_at is None:
            raise ValueError("WorkflowRuntime: created_at/updated_at are required")

        return cls(
            workflow_id=definition["workflow_id"],
            name=definition["name"],
            default_queue=definition["default_queue"],
            result_policy=ResultPolicy(definition["result_policy"]),
            max_parallelism=definition.get("max_parallelism"),
            tasks=tasks,
            workflow_state=WorkflowState(state["workflow_state"]),
            cancel_requested=state.get("cancel_requested", False),
            created_at=created_at,
            updated_at=updated_at,
            error=state.get("error"),
        )

    # ------------------------------------------------------------------
    # Legacy single-blob deserialization
    # ------------------------------------------------------------------

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> WorkflowRuntime:
        version = d.get("v", 1)
        if version != 1:
            raise ValueError(f"WorkflowRuntime: unsupported schema version {version}")

        def _dt(s: str | None) -> datetime | None:
            return datetime.fromisoformat(s) if s else None

        _intern = sys.intern
        tasks: dict[str, TaskRuntime] = {}
        for n, td in d["tasks"].items():
            dp_raw = td["dep_policy"]
            dp: DepPolicy | int = dp_raw if isinstance(dp_raw, int) else DepPolicy(dp_raw)
            name = _intern(n)
            tasks[name] = TaskRuntime(
                name=name,
                parents=tuple(_intern(p) for p in td["parents"]),
                children=tuple(_intern(c) for c in td["children"]),
                queue=_intern(td["queue"]),
                num_retries=td["num_retries"],
                dep_policy=dp,
                timeout_s=td.get("timeout_s"),
                condition=td.get("condition"),
                kwargs_raw=json.dumps(td.get("kwargs", {})).encode(),
                state=TaskState(td["state"]),
                completed_parents=td.get("completed_parents", 0),
                failed_parents=td.get("failed_parents", 0),
                output_ref=td.get("output_ref"),
                error=td.get("error"),
                started_at=_dt(td.get("started_at")),
                completed_at=_dt(td.get("completed_at")),
                attempt_no=td.get("attempt_no", 0),
            )
        created_at = _dt(d["created_at"])
        updated_at = _dt(d["updated_at"])
        if created_at is None or updated_at is None:
            raise ValueError("WorkflowRuntime: created_at/updated_at are required")
        return cls(
            workflow_id=d["workflow_id"],
            name=d["name"],
            default_queue=d["default_queue"],
            result_policy=ResultPolicy(d["result_policy"]),
            max_parallelism=d.get("max_parallelism"),
            tasks=tasks,
            workflow_state=WorkflowState(d["workflow_state"]),
            cancel_requested=d.get("cancel_requested", False),
            created_at=created_at,
            updated_at=updated_at,
            error=d.get("error"),
        )
