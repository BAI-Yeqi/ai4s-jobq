# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Data model for DAG-based workflows."""

from __future__ import annotations

import json
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from datetime import datetime


# ---------------------------------------------------------------------------
# State enums
# ---------------------------------------------------------------------------
#
# These are ``str`` subclasses so their members compare equal to (and hash
# the same as) the underlying string.  That means existing string-typed
# Table rows round-trip unchanged, callers can still write
# ``status == "completed"``, and membership tests like
# ``s in TaskState.DEP_SATISFIED`` work for plain strings ``s``.


class TaskState(str, Enum):
    """Lifecycle states of a workflow task."""

    PENDING = "pending"
    READY = "ready"
    READY_PENDING_BUDGET = "ready_pending_budget"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"
    CANCELLED = "cancelled"

    # Predicate sets (populated after class body — see below).
    TERMINAL: ClassVar[frozenset[str]]
    ACTIVE: ClassVar[frozenset[str]]
    DEP_SATISFIED: ClassVar[frozenset[str]]
    DEP_FAILED: ClassVar[frozenset[str]]

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def is_terminal(cls, s: str) -> bool:
        """Task has reached an end state; no further transitions expected."""
        return s in cls.TERMINAL

    @classmethod
    def is_active(cls, s: str) -> bool:
        """Task is either waiting on deps or executing."""
        return s in cls.ACTIVE

    @classmethod
    def is_dep_satisfied(cls, s: str) -> bool:
        """From a downstream task's perspective, this upstream counts as 'done'."""
        return s in cls.DEP_SATISFIED

    @classmethod
    def is_dep_failed(cls, s: str) -> bool:
        """From a downstream task's perspective, this upstream is a blocker."""
        return s in cls.DEP_FAILED


TaskState.TERMINAL = frozenset(
    {
        TaskState.COMPLETED,
        TaskState.FAILED,
        TaskState.SKIPPED,
        TaskState.UPSTREAM_FAILED,
        TaskState.CANCELLED,
    }
)
# READY_PENDING_BUDGET is treated as ACTIVE for accounting purposes:
# the task has cleared its dependencies and is logically ready to run,
# but the actor has parked it because the workflow is at its
# ``max_parallelism`` budget. It will transition to READY (and be
# enqueued) when in-flight slots free up. From the workflow's point of
# view the task is still in the active funnel and counts toward
# remaining_nonterminal.
TaskState.ACTIVE = frozenset(
    {
        TaskState.PENDING,
        TaskState.READY,
        TaskState.READY_PENDING_BUDGET,
        TaskState.RUNNING,
    }
)
TaskState.DEP_SATISFIED = frozenset({TaskState.COMPLETED, TaskState.SKIPPED})
TaskState.DEP_FAILED = frozenset({TaskState.FAILED, TaskState.UPSTREAM_FAILED, TaskState.CANCELLED})


class WorkflowState(str, Enum):
    """Lifecycle states of a workflow."""

    PENDING = "pending"
    RUNNING = "running"
    CANCELLING = "cancelling"
    """Cancellation has been requested and applied; workers are still running."""
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    TERMINAL: ClassVar[frozenset[str]]
    ACTIVE: ClassVar[frozenset[str]]

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def is_terminal(cls, s: str) -> bool:
        return s in cls.TERMINAL

    @classmethod
    def is_active(cls, s: str) -> bool:
        return s in cls.ACTIVE


WorkflowState.TERMINAL = frozenset(
    {WorkflowState.COMPLETED, WorkflowState.FAILED, WorkflowState.CANCELLED}
)
WorkflowState.ACTIVE = frozenset(
    {WorkflowState.PENDING, WorkflowState.RUNNING, WorkflowState.CANCELLING}
)


class DepPolicy(str, Enum):
    """How many parent dependencies must complete before a task is ready."""

    ALL = "all"
    ANY = "any"
    ALL_SETTLED = "all_settled"
    """Barrier policy: wait for every dep to reach a terminal state
    (success or failure), then activate iff at least one succeeded.

    Contrast with :attr:`ANY`, which activates eagerly on the first
    success and can race against still-running deps (producing
    "downstream finished before upstream" timelines). ``ALL_SETTLED``
    always waits — no late results — at the cost of latency.
    """

    def __str__(self) -> str:
        return self.value


class ResultPolicy(str, Enum):
    """How the workflow's overall terminal status is computed.

    The choice only affects the COMPLETED-vs-FAILED decision once
    every task has reached a terminal state; CANCELLED stickiness and
    the in-flight RUNNING projection are independent of it.
    """

    ALL_SINKS_SUCCEED = "all_sinks_succeed"
    """Default. The workflow is ``completed`` iff every sink (leaf
    task — one with no downstream dependents) ended in a dep-satisfied
    state (``completed`` or ``skipped``).  Intermediate-task failures
    that the DAG absorbs (e.g. via ``dep_policy='all_settled'`` or
    ``'any'`` on a merge) do not flip the workflow to ``failed`` as
    long as the user-visible outputs were produced.
    """

    ALL_TASKS_SUCCEED = "all_tasks_succeed"
    """Strict policy: any task that fails (or is cancelled / upstream-
    failed) flips the workflow to ``failed``, even if downstream
    consumers absorbed the failure.  Use when every task carries
    side effects that must succeed, or to preserve legacy behaviour.
    """

    def __str__(self) -> str:
        return self.value


# ---------------------------------------------------------------------------
# Auto-tiered output helpers
# ---------------------------------------------------------------------------

OUTPUT_INLINE_THRESHOLD = 32 * 1024  # 32 KB


def serialize_output(output: Any) -> str:
    """Serialize a task output to JSON string.

    If the serialized form exceeds ``OUTPUT_INLINE_THRESHOLD`` bytes the
    caller should stash it in Blob Storage and store the URI instead.
    This helper only handles the inline path — the caller is responsible
    for the blob upload when the return value is too large.
    """
    return json.dumps(output)


def deserialize_output(output_ref: str) -> Any:
    """Deserialize an output reference.

    If *output_ref* starts with ``blob:`` it is a blob stash reference
    (``blob:<filename>`` or ``blob:<filename>:<md5>``).  The caller
    should use ``BlobContainer.unstash_from_json`` to retrieve the data.
    For inline data, parses as JSON directly.
    """
    if output_ref.startswith("blob:"):
        return output_ref  # caller handles via BlobContainer
    return json.loads(output_ref)


def output_needs_blob(serialized: str) -> bool:
    """Return ``True`` if the serialized output exceeds the inline threshold."""
    return len(serialized.encode()) > OUTPUT_INLINE_THRESHOLD


# ---------------------------------------------------------------------------
# Workflow definition (user-facing, submitted to create a workflow)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowTask:
    """A single task in a workflow DAG."""

    name: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    queue: str | None = None
    dep_policy: DepPolicy | int = DepPolicy.ALL
    timeout_s: int | None = None
    num_retries: int = 0
    condition: str | None = None


@dataclass(frozen=True)
class WorkflowDefinition:
    """A DAG of tasks.  Validated at submission time."""

    name: str
    tasks: list[WorkflowTask]
    default_queue: str = "default"
    default_task_timeout_s: int | None = None
    # Per-workflow fan-out budget. When set, the coordinator's
    # per-workflow actor enforces a cap on the number of simultaneously
    # in-flight (RUNNING + freshly READY but not yet picked up) tasks
    # for this workflow. Tasks beyond the cap are parked in the
    # READY_PENDING_BUDGET state and released as in-flight slots free
    # up. ``None`` means unlimited (legacy behaviour).
    #
    # Use this when a workflow's fan-out can swamp shared resources —
    # a downstream API quota, a fixed worker pool size, a cost ceiling
    # — but the workflow itself describes the full DAG up front rather
    # than artificially serialising via dependency edges. Setting a
    # budget keeps the DAG declarative while bounding parallelism.
    max_parallelism: int | None = None

    # How the workflow's terminal status is computed once every task is
    # terminal.  See :class:`ResultPolicy` for details.  Default
    # ``ALL_SINKS_SUCCEED`` matches the "the merge ran, we're good"
    # intuition for DAGs whose intermediate failures are absorbed by
    # ``dep_policy='all_settled'`` / ``'any'`` joins.
    result_policy: ResultPolicy = ResultPolicy.ALL_SINKS_SUCCEED

    # -- helpers -------------------------------------------------------------

    @property
    def task_map(self) -> dict[str, WorkflowTask]:
        return {t.name: t for t in self.tasks}

    @property
    def child_map(self) -> dict[str, list[str]]:
        """Adjacency map: task name → list of names that directly depend on it.

        Built once on first access and cached on the instance.  This is the
        canonical source for ``children_of``; computing it on each call is
        O(N·D) where N is the number of tasks and D is the average dep
        list length, which dominates coordinator hot paths for fan-out
        workflows.  The cached map turns those lookups into O(1).
        """
        cached: dict[str, list[str]] | None = self.__dict__.get("_child_map")
        if cached is not None:
            return cached
        cm: dict[str, list[str]] = {t.name: [] for t in self.tasks}
        for t in self.tasks:
            for dep in t.depends_on:
                # Tolerate references that don't exist in tasks (validate()
                # would have raised, but this helper may be called before
                # validation in error paths).
                cm.setdefault(dep, []).append(t.name)
        # Bypass frozen-dataclass guard to memoise on the instance.
        object.__setattr__(self, "_child_map", cm)
        return cm

    @property
    def root_tasks(self) -> list[WorkflowTask]:
        """Tasks with no dependencies (entry points of the DAG)."""
        return [t for t in self.tasks if not t.depends_on]

    @property
    def sink_tasks(self) -> list[WorkflowTask]:
        """Tasks with no downstream dependents (leaves of the DAG).

        Used by :class:`ResultPolicy.ALL_SINKS_SUCCEED` to decide the
        workflow's terminal status from the user-visible outputs only.
        """
        cm = self.child_map
        return [t for t in self.tasks if not cm.get(t.name)]

    def children_of(self, task_name: str) -> list[str]:
        """Return task names that directly depend on *task_name*."""
        return list(self.child_map.get(task_name, ()))

    # -- validation ----------------------------------------------------------

    def validate(self) -> None:
        """Check the DAG for structural errors.

        Raises ``ValueError`` on:
        - empty workflow or task names
        - duplicate task names
        - references to non-existent tasks in ``depends_on``
        - cycles (uses Kahn's algorithm / topological sort)
        - unreachable tasks (no path from a root)
        - invalid dep_policy values
        - invalid condition usage
        """
        self._validate_workflow_name()
        self._validate_task_names()
        self._validate_max_parallelism()
        names = {t.name for t in self.tasks}
        child_map = self.child_map

        self._validate_unique_names(names)
        self._validate_dep_references(names)
        self._validate_no_self_deps()
        self._validate_acyclic(child_map)
        self._validate_reachable(names, child_map)
        self._validate_dep_policies()
        self._validate_conditions()

    def _validate_workflow_name(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("workflow name must be a non-empty string")

    def _validate_max_parallelism(self) -> None:
        mp = self.max_parallelism
        if mp is None:
            return
        if not isinstance(mp, int) or isinstance(mp, bool):
            raise ValueError(
                f"max_parallelism must be a positive int or None, got {mp!r} ({type(mp).__name__})"
            )
        if mp < 1:
            raise ValueError(
                f"max_parallelism must be >= 1 when set; got {mp}. Use None for unlimited."
            )

    def _validate_task_names(self) -> None:
        for t in self.tasks:
            if not isinstance(t.name, str) or not t.name.strip():
                raise ValueError("task name must be a non-empty string")

    def _validate_unique_names(self, names: set[str]) -> None:
        if len(names) != len(self.tasks):
            seen: set[str] = set()
            for t in self.tasks:
                if t.name in seen:
                    raise ValueError(f"Duplicate task name: {t.name!r}")
                seen.add(t.name)

    def _validate_dep_references(self, names: set[str]) -> None:
        for t in self.tasks:
            for dep in t.depends_on:
                if dep not in names:
                    raise ValueError(f"Task {t.name!r} depends on {dep!r} which does not exist")

    def _validate_no_self_deps(self) -> None:
        for t in self.tasks:
            if t.name in t.depends_on:
                raise ValueError(f"Task {t.name!r} depends on itself")

    def _validate_acyclic(self, child_map: dict[str, list[str]]) -> None:
        """Cycle detection via topological sort (Kahn's algorithm)."""
        in_degree: dict[str, int] = {t.name: len(t.depends_on) for t in self.tasks}
        queue: deque[str] = deque(n for n, d in in_degree.items() if d == 0)
        sorted_count = 0

        while queue:
            node = queue.popleft()
            sorted_count += 1
            for child in child_map[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if sorted_count != len(self.tasks):
            cycle_members = [n for n, d in in_degree.items() if d > 0]
            raise ValueError(f"Cycle detected involving tasks: {cycle_members}")

    def _validate_reachable(self, names: set[str], child_map: dict[str, list[str]]) -> None:
        """Ensure every task is reachable from at least one root."""
        roots = {t.name for t in self.tasks if not t.depends_on}
        if not roots:
            raise ValueError("No root tasks (every task has dependencies)")

        reachable: set[str] = set()
        visit_queue: deque[str] = deque(roots)
        while visit_queue:
            node = visit_queue.popleft()
            if node in reachable:
                continue
            reachable.add(node)
            visit_queue.extend(child_map[node])

        unreachable = names - reachable
        if unreachable:
            raise ValueError(f"Unreachable tasks (no path from a root): {unreachable}")

    def _validate_dep_policies(self) -> None:
        for t in self.tasks:
            if isinstance(t.dep_policy, int):
                if t.dep_policy < 1:
                    raise ValueError(
                        f"Task {t.name!r}: dep_policy int must be >= 1, got {t.dep_policy}"
                    )
                if t.dep_policy > len(t.depends_on):
                    raise ValueError(
                        f"Task {t.name!r}: dep_policy={t.dep_policy} but only "
                        f"{len(t.depends_on)} dependencies"
                    )
            elif t.dep_policy not in (DepPolicy.ALL, DepPolicy.ANY, DepPolicy.ALL_SETTLED):
                raise ValueError(
                    f"Task {t.name!r}: dep_policy must be 'all', 'any', "
                    f"'all_settled', or int, got {t.dep_policy!r}"
                )

    def _validate_conditions(self) -> None:
        for t in self.tasks:
            if t.condition is not None:
                if not t.depends_on:
                    raise ValueError(f"Task {t.name!r}: condition requires at least one dependency")
                if any(d.startswith("__merge_") for d in t.depends_on):
                    raise ValueError(
                        f"Task {t.name!r}: conditions are incompatible with"
                        " sequentialize_fan_in.  The condition evaluator cannot"
                        " see through dummy merge nodes to the original upstream"
                        " outputs.  Remove the condition or reduce fan-in below"
                        " max_fan_in."
                    )
                from ai4s.jobq.workflow.condition import validate_condition

                validate_condition(t.condition, t.depends_on)

    # -- serialization -------------------------------------------------------

    def to_json(self) -> str:
        return json.dumps(self._to_dict())

    @classmethod
    def from_json(cls, s: str) -> WorkflowDefinition:
        return cls._from_dict(json.loads(s), source="<json>")

    @classmethod
    def from_file(cls, path: str) -> WorkflowDefinition:
        """Load a workflow definition from a JSON or YAML file.

        File format is determined by extension (``.yaml``/``.yml`` for YAML,
        anything else is treated as JSON).
        """
        with open(path) as f:
            if path.endswith((".yaml", ".yml")):
                import yaml

                data = yaml.safe_load(f)
            else:
                data = json.load(f)
        return cls._from_dict(data, source=path)

    def _to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self.name,
            "default_queue": self.default_queue,
            "default_task_timeout_s": self.default_task_timeout_s,
            "tasks": [
                {
                    "name": t.name,
                    "kwargs": t.kwargs,
                    "depends_on": t.depends_on,
                    "queue": t.queue,
                    "dep_policy": t.dep_policy,
                    "timeout_s": t.timeout_s,
                    "num_retries": t.num_retries,
                    **({"condition": t.condition} if t.condition else {}),
                }
                for t in self.tasks
            ],
        }
        # Round-trip ``max_parallelism`` only when set, so existing
        # serialised definitions in storage continue to round-trip
        # byte-for-byte.
        if self.max_parallelism is not None:
            d["max_parallelism"] = self.max_parallelism
        # Round-trip ``result_policy`` only when non-default for the
        # same byte-for-byte reason.
        if self.result_policy != ResultPolicy.ALL_SINKS_SUCCEED:
            d["result_policy"] = str(self.result_policy)
        return d

    @classmethod
    def _from_dict(cls, d: dict[str, Any], source: str = "<input>") -> WorkflowDefinition:
        """Parse and validate a raw dict into a WorkflowDefinition.

        Raises ``ValueError`` with descriptive messages for:
        - missing required fields
        - unknown fields
        - wrong field types
        """

        def _check(cond: bool, msg: str) -> None:
            if not cond:
                raise ValueError(msg)

        _check(isinstance(d, dict), f"{source}: expected a mapping, got {type(d).__name__}")

        # -- Top-level validation --
        workflow_fields = {
            "name",
            "tasks",
            "default_queue",
            "default_task_timeout_s",
            "max_parallelism",
            "result_policy",
        }
        unknown_top = set(d.keys()) - workflow_fields
        _check(not unknown_top, f"{source}: unknown workflow fields: {sorted(unknown_top)}")
        _check("name" in d, f"{source}: 'name' is required")
        _check(isinstance(d.get("name"), str), f"{source}: 'name' must be a string")
        _check("tasks" in d, f"{source}: 'tasks' is required")
        _check(isinstance(d.get("tasks"), list), f"{source}: 'tasks' must be a list")
        _check(bool(d.get("tasks")), f"{source}: 'tasks' must not be empty")
        if "default_queue" in d:
            _check(
                isinstance(d["default_queue"], str), f"{source}: 'default_queue' must be a string"
            )
        if "default_task_timeout_s" in d and d["default_task_timeout_s"] is not None:
            _check(
                isinstance(d["default_task_timeout_s"], int),
                f"{source}: 'default_task_timeout_s' must be an integer or null",
            )
        if "max_parallelism" in d and d["max_parallelism"] is not None:
            mp = d["max_parallelism"]
            _check(
                isinstance(mp, int) and not isinstance(mp, bool) and mp >= 1,
                f"{source}: 'max_parallelism' must be a positive integer or null, got {mp!r}",
            )
        rp_raw = d.get("result_policy")
        if rp_raw is None:
            result_policy = ResultPolicy.ALL_SINKS_SUCCEED
        else:
            _check(
                isinstance(rp_raw, str)
                and rp_raw in (ResultPolicy.ALL_SINKS_SUCCEED, ResultPolicy.ALL_TASKS_SUCCEED),
                f"{source}: 'result_policy' must be 'all_sinks_succeed' or "
                f"'all_tasks_succeed', got {rp_raw!r}",
            )
            result_policy = ResultPolicy(rp_raw)

        # -- Duplicate task name validation --
        seen_task_names: set[str] = set()
        for i, t in enumerate(d["tasks"]):
            if isinstance(t, dict) and isinstance(t.get("name"), str):
                tname_raw = t["name"]
                if tname_raw in seen_task_names:
                    raise ValueError(f"{source}: tasks[{i}]: Duplicate task name: {tname_raw!r}")
                seen_task_names.add(tname_raw)

        # -- Task validation --
        task_fields = {
            "name",
            "kwargs",
            "depends_on",
            "queue",
            "dep_policy",
            "timeout_s",
            "num_retries",
            "condition",
        }
        tasks: list[WorkflowTask] = []
        for i, t in enumerate(d["tasks"]):
            loc = f"{source}: tasks[{i}]"
            _check(isinstance(t, dict), f"{loc}: expected a mapping, got {type(t).__name__}")
            unknown_task = set(t.keys()) - task_fields
            _check(not unknown_task, f"{loc}: unknown task fields: {sorted(unknown_task)}")
            _check("name" in t, f"{loc}: 'name' is required")
            _check(isinstance(t.get("name"), str), f"{loc}: 'name' must be a string")
            _check(bool(t.get("name", "").strip()), f"{loc}: 'name' must not be empty")

            tname = t["name"]
            if "kwargs" in t:
                _check(
                    isinstance(t["kwargs"], dict), f"{loc} ({tname}): 'kwargs' must be a mapping"
                )
            if "depends_on" in t:
                _check(
                    isinstance(t["depends_on"], list),
                    f"{loc} ({tname}): 'depends_on' must be a list",
                )
                for j, dep in enumerate(t["depends_on"]):
                    _check(
                        isinstance(dep, str), f"{loc} ({tname}): depends_on[{j}] must be a string"
                    )
            if "queue" in t and t["queue"] is not None:
                _check(
                    isinstance(t["queue"], str),
                    f"{loc} ({tname}): 'queue' must be a string or null",
                )
            if "dep_policy" in t:
                dp = t["dep_policy"]
                _check(
                    isinstance(dp, (str, int)),
                    f"{loc} ({tname}): 'dep_policy' must be 'all', 'any', 'all_settled', or int",
                )
                if isinstance(dp, str):
                    _check(
                        dp in (DepPolicy.ALL, DepPolicy.ANY, DepPolicy.ALL_SETTLED),
                        f"{loc} ({tname}): 'dep_policy' string must be 'all', 'any', "
                        "or 'all_settled'",
                    )
            if "timeout_s" in t and t["timeout_s"] is not None:
                _check(
                    isinstance(t["timeout_s"], int),
                    f"{loc} ({tname}): 'timeout_s' must be an integer or null",
                )
            if "num_retries" in t:
                _check(
                    isinstance(t["num_retries"], int),
                    f"{loc} ({tname}): 'num_retries' must be an integer",
                )
            if "condition" in t:
                _check(
                    isinstance(t["condition"], str),
                    f"{loc} ({tname}): 'condition' must be a string expression",
                )

            tasks.append(
                WorkflowTask(
                    name=tname,
                    kwargs=t.get("kwargs", {}),
                    depends_on=t.get("depends_on", []),
                    queue=t.get("queue"),
                    dep_policy=t.get("dep_policy", DepPolicy.ALL),
                    timeout_s=t.get("timeout_s"),
                    num_retries=t.get("num_retries", 0),
                    condition=t.get("condition"),
                )
            )

        return cls(
            name=d["name"],
            tasks=tasks,
            default_queue=d.get("default_queue", "default"),
            default_task_timeout_s=d.get("default_task_timeout_s"),
            max_parallelism=d.get("max_parallelism"),
            result_policy=result_policy,
        )


# ---------------------------------------------------------------------------
# Workflow status (returned by queries)
# ---------------------------------------------------------------------------


@dataclass
class TaskStatus:
    """Status of a single task in a running workflow."""

    name: str
    status: TaskState
    depends_on: list[str]
    depended_by: list[str]
    dep_policy: str
    completed_deps: int
    failed_deps: int
    queue: str | None
    output_ref: str | None
    error: str | None
    started_at: datetime | None
    completed_at: datetime | None
    retries_remaining: int
    task_timeout_s: int | None
    updated_at: datetime | None = None
    fan_out_at: datetime | None = None
    attempt_no: int = 0
    fan_out_done: bool = False

    def __post_init__(self) -> None:
        # Completed tasks must carry a completion timestamp; otherwise
        # downstream consumers (dashboards, sweepers, recount) cannot
        # tell when the row reached its terminal state.
        if self.status == TaskState.COMPLETED and self.completed_at is None:
            raise ValueError(
                f"TaskStatus(name={self.name!r}): completed_at is required when status is COMPLETED"
            )


@dataclass
class WorkflowStatus:
    """Overall status of a workflow."""

    workflow_id: str
    name: str
    status: WorkflowState
    total: int
    completed: int
    running: int
    failed: int
    pending: int
    skipped: int
    default_queue: str
    queues_used: list[str]
    created_at: datetime
    updated_at: datetime
    error: str | None = None
    tasks: dict[str, TaskStatus] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Completion message (worker → coordinator via Service Bus)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkflowCompletion:
    """Sent by a worker to the completion queue after task execution.

    The worker writes the task's terminal status to Table Storage *before*
    publishing this completion. The coordinator reads the row, gates fan-out
    on a boolean ``fan_out_done`` CAS marker, and advances children.

    The optional ``task_etag`` / ``task_depended_by`` / ``task_queue``
    fields let the worker stamp the post-write task-row state onto the
    message it publishes.  When set, the coordinator's
    ``_process_completion_inner`` skips its mandatory
    ``get_task_entity`` GET and uses the embedded ETag directly for the
    fan-out CAS — collapsing the coordinator hot path from 3 to 2
    sequential RTs.  Old-style messages without these fields still work
    through the slow GET-and-CAS fallback path.
    """

    workflow_id: str
    task_name: str
    success: bool
    output_ref: str | None = None
    error: str | None = None
    # Coordinator fast-path hints stamped by the worker after
    # ``apply_completion`` writes the terminal row.
    task_etag: str | None = None
    task_depended_by: list[str] | None = None
    task_queue: str | None = None
    # Which attempt produced this completion (1-based). ``None`` for
    # messages emitted by pre-R3 workers — coordinator treats those
    # without an attempt-staleness check (back-compat).
    attempt_no: int | None = None

    def serialize(self) -> str:
        payload: dict[str, Any] = {
            "workflow_id": self.workflow_id,
            "task_name": self.task_name,
            "success": self.success,
            "output_ref": self.output_ref,
            "error": self.error,
        }
        # Additive optional fields — omit when absent so the wire size
        # stays minimal for old-style messages and for tests that
        # build completions directly.
        if self.task_etag is not None:
            payload["task_etag"] = self.task_etag
        if self.task_depended_by is not None:
            payload["task_depended_by"] = self.task_depended_by
        if self.task_queue is not None:
            payload["task_queue"] = self.task_queue
        if self.attempt_no is not None:
            payload["attempt_no"] = self.attempt_no
        return json.dumps(payload)

    @classmethod
    def deserialize(cls, data: str | bytes) -> WorkflowCompletion:
        if isinstance(data, bytes):
            data = data.decode()
        d = json.loads(data)
        return cls(
            workflow_id=d["workflow_id"],
            task_name=d["task_name"],
            success=d["success"],
            output_ref=d.get("output_ref"),
            error=d.get("error"),
            task_etag=d.get("task_etag"),
            task_depended_by=d.get("task_depended_by"),
            task_queue=d.get("task_queue"),
            attempt_no=d.get("attempt_no"),
        )


def generate_workflow_id() -> str:
    """Generate a unique workflow ID."""
    return uuid.uuid4().hex
