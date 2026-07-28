# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for workflow entities and DAG validation."""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from unittest.mock import mock_open

import pytest

from ai4s.jobq.workflow.condition import evaluate_condition, validate_condition
from ai4s.jobq.workflow.entities import (
    TaskState,
    WorkflowCompletion,
    WorkflowDefinition,
    WorkflowTask,
    deserialize_output,
    generate_workflow_id,
    output_needs_blob,
    serialize_output,
)

# ---------------------------------------------------------------------------
# WorkflowDefinition validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_valid_diamond(self):
        """Verify that workflow validation accepts a diamond-shaped dependency graph."""
        wf = WorkflowDefinition(
            name="diamond",
            tasks=[
                WorkflowTask(name="a", kwargs={"x": 1}),
                WorkflowTask(name="b", depends_on=["a"]),
                WorkflowTask(name="c", depends_on=["a"]),
                WorkflowTask(name="d", depends_on=["b", "c"]),
            ],
        )
        wf.validate()

    def test_valid_linear(self):
        """Verify that workflow validation accepts a linear dependency chain."""
        wf = WorkflowDefinition(
            name="linear",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b", depends_on=["a"]),
                WorkflowTask(name="c", depends_on=["b"]),
            ],
        )
        wf.validate()

    def test_valid_single_task(self):
        """Verify that workflow validation accepts a single-task workflow with no dependencies."""
        wf = WorkflowDefinition(name="single", tasks=[WorkflowTask(name="only")])
        wf.validate()

    def test_duplicate_names(self):
        """Verify that workflow validation rejects duplicate task names."""
        wf = WorkflowDefinition(
            name="dup",
            tasks=[WorkflowTask(name="a"), WorkflowTask(name="a")],
        )
        with pytest.raises(ValueError, match="Duplicate task name"):
            wf.validate()

    def test_missing_dependency(self):
        """Verify that workflow validation rejects tasks that depend on missing predecessors."""
        wf = WorkflowDefinition(
            name="missing",
            tasks=[WorkflowTask(name="a", depends_on=["nonexistent"])],
        )
        with pytest.raises(ValueError, match="does not exist"):
            wf.validate()

    def test_self_dependency(self):
        """Verify that workflow validation rejects tasks that depend on themselves."""
        wf = WorkflowDefinition(
            name="self",
            tasks=[WorkflowTask(name="a", depends_on=["a"])],
        )
        with pytest.raises(ValueError, match="depends on itself"):
            wf.validate()

    def test_cycle_detection(self):
        """Verify that workflow validation detects dependency cycles."""
        wf = WorkflowDefinition(
            name="cycle",
            tasks=[
                WorkflowTask(name="a", depends_on=["c"]),
                WorkflowTask(name="b", depends_on=["a"]),
                WorkflowTask(name="c", depends_on=["b"]),
            ],
        )
        with pytest.raises(ValueError, match="Cycle detected"):
            wf.validate()

    def test_no_roots(self):
        """Every task has deps → caught by cycle detection or no-roots check."""
        wf = WorkflowDefinition(
            name="no-roots",
            tasks=[
                WorkflowTask(name="a", depends_on=["b"]),
                WorkflowTask(name="b", depends_on=["a"]),
            ],
        )
        with pytest.raises(ValueError, match="Cycle detected"):
            wf.validate()

    def test_dep_policy_int_too_large(self):
        """Verify that workflow validation rejects integer dep_policy values larger than the number of dependencies."""
        wf = WorkflowDefinition(
            name="policy",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b"),
                WorkflowTask(name="c", depends_on=["a", "b"], dep_policy=5),
            ],
        )
        with pytest.raises(ValueError, match="dep_policy=5"):
            wf.validate()

    def test_dep_policy_int_zero(self):
        """Verify that workflow validation rejects dep_policy integers below one."""
        wf = WorkflowDefinition(
            name="policy",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b", depends_on=["a"], dep_policy=0),
            ],
        )
        with pytest.raises(ValueError, match="dep_policy int must be >= 1"):
            wf.validate()

    def test_dep_policy_valid_int(self):
        """Verify that workflow validation accepts integer dep_policy values within the dependency count."""
        wf = WorkflowDefinition(
            name="policy",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b"),
                WorkflowTask(name="c", depends_on=["a", "b"], dep_policy=1),
            ],
        )
        wf.validate()

    def test_dep_policy_any(self):
        """Verify that workflow validation accepts the "any" dependency policy."""
        wf = WorkflowDefinition(
            name="policy",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b"),
                WorkflowTask(name="c", depends_on=["a", "b"], dep_policy="any"),
            ],
        )
        wf.validate()

    def test_dep_policy_all_settled(self):
        """Verify that workflow validation accepts the "all_settled" policy."""
        wf = WorkflowDefinition(
            name="policy",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b"),
                WorkflowTask(name="c", depends_on=["a", "b"], dep_policy="all_settled"),
            ],
        )
        wf.validate()

    def test_dep_policy_invalid_string(self):
        """Verify that workflow validation rejects unsupported dependency policy strings."""
        wf = WorkflowDefinition(
            name="policy",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b", depends_on=["a"], dep_policy="bogus"),
            ],
        )
        with pytest.raises(ValueError, match="dep_policy must be 'all', 'any',"):
            wf.validate()

    def test_validate_reachable_rejects_missing_roots(self):
        """Verify that _validate_reachable rejects graphs with no root tasks."""
        wf = WorkflowDefinition(
            name="no-roots-helper",
            tasks=[
                WorkflowTask(name="a", depends_on=["b"]),
                WorkflowTask(name="b", depends_on=["a"]),
            ],
        )

        with pytest.raises(ValueError, match="No root tasks"):
            wf._validate_reachable({"a", "b"}, wf.child_map)

    def test_validate_reachable_rejects_unreachable_tasks(self):
        """Verify that _validate_reachable rejects tasks that cannot be reached from the roots."""
        wf = WorkflowDefinition(
            name="unreachable-helper",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b", depends_on=["a"]),
            ],
        )

        with pytest.raises(ValueError, match="Unreachable tasks"):
            wf._validate_reachable({"a", "b"}, {"a": [], "b": []})


# ---------------------------------------------------------------------------
# WorkflowDefinition helpers
# ---------------------------------------------------------------------------


class TestWorkflowDefinitionHelpers:
    def _diamond(self) -> WorkflowDefinition:
        return WorkflowDefinition(
            name="diamond",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b", depends_on=["a"]),
                WorkflowTask(name="c", depends_on=["a"]),
                WorkflowTask(name="d", depends_on=["b", "c"]),
            ],
        )

    def test_task_map(self):
        """Verify that task_map indexes tasks by name."""
        wf = self._diamond()
        assert set(wf.task_map.keys()) == {"a", "b", "c", "d"}, (
            "result should equal {'a', 'b', 'c', 'd'}"
        )

    def test_root_tasks(self):
        """Verify that root_tasks returns tasks without dependencies."""
        wf = self._diamond()
        assert [t.name for t in wf.root_tasks] == ["a"], (
            "[t.name for t in wf.root_tasks] should equal ['a']"
        )

    def test_children_of(self):
        """Verify that children_of returns the immediate downstream tasks for each node."""
        wf = self._diamond()
        assert sorted(wf.children_of("a")) == ["b", "c"], (
            "sorted child task list should equal ['b', 'c']"
        )
        assert wf.children_of("b") == ["d"], "child task list should equal ['d']"
        assert wf.children_of("d") == [], "child task list should equal []"


# ---------------------------------------------------------------------------
# Serialization round-trips
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_definition_json_roundtrip(self):
        """Verify that WorkflowDefinition serialization round-trips queues, timeouts, and task fields."""
        wf = WorkflowDefinition(
            name="test",
            default_queue="gpu",
            default_task_timeout_s=7200,
            tasks=[
                WorkflowTask(
                    name="a",
                    kwargs={"x": 1},
                    queue="cpu",
                    dep_policy="all",
                    timeout_s=600,
                    num_retries=3,
                ),
                WorkflowTask(
                    name="b",
                    depends_on=["a"],
                    dep_policy="any",
                ),
            ],
        )
        s = wf.to_json()
        wf2 = WorkflowDefinition.from_json(s)
        assert wf2.name == wf.name, "name should equal wf.name"
        assert wf2.default_queue == "gpu", "default queue should equal 'gpu'"
        assert wf2.default_task_timeout_s == 7200, "default task timeout should equal 7200"
        assert len(wf2.tasks) == 2, "tasks length should equal 2"
        assert wf2.task_map["a"].kwargs == {"x": 1}, "kwargs should equal {'x': 1}"
        assert wf2.task_map["a"].queue == "cpu", "queue should equal 'cpu'"
        assert wf2.task_map["a"].timeout_s == 600, "task timeout should equal 600"
        assert wf2.task_map["a"].num_retries == 3, "retry count should equal 3"
        assert wf2.task_map["b"].dep_policy == "any", "dependency policy should equal 'any'"

    def test_definition_from_file_yaml(self, monkeypatch):
        """Verify that WorkflowDefinition.from_file loads workflow data from YAML."""
        yaml_stub = SimpleNamespace(
            safe_load=lambda file_obj: {"name": "yaml-workflow", "tasks": [{"name": "a"}]}
        )
        monkeypatch.setitem(sys.modules, "yaml", yaml_stub)
        monkeypatch.setattr(
            "builtins.open", mock_open(read_data="name: yaml-workflow\ntasks:\n  - name: a\n")
        )

        wf = WorkflowDefinition.from_file("workflow.yaml")

        assert wf.name == "yaml-workflow", "name should equal 'yaml-workflow'"
        assert [task.name for task in wf.tasks] == ["a"], (
            "[task.name for task in wf.tasks] should equal ['a']"
        )

    def test_from_dict_rejects_invalid_top_level_types(self):
        """Verify that WorkflowDefinition._from_dict rejects invalid top-level field types."""
        with pytest.raises(ValueError, match="bad: 'tasks' must be a list"):
            WorkflowDefinition._from_dict({"name": "bad", "tasks": "oops"}, source="bad")

    def test_completion_roundtrip(self):
        """Verify that WorkflowCompletion serialization round-trips successful completion data."""
        c = WorkflowCompletion(
            workflow_id="abc123",
            task_name="train",
            success=True,
            output_ref='{"mae": 0.42}',
        )
        s = c.serialize()
        c2 = WorkflowCompletion.deserialize(s)
        assert c2.workflow_id == c.workflow_id, "workflow ID should equal c.workflow_id"
        assert c2.task_name == c.task_name, "task name should equal c.task_name"
        assert c2.success is True, "success should be True"
        assert c2.output_ref == '{"mae": 0.42}', "output reference should equal '{'mae': 0.42}'"
        assert c2.error is None, "error should be None"

    def test_completion_failure_roundtrip(self):
        """Verify that WorkflowCompletion serialization round-trips failed completion data."""
        c = WorkflowCompletion(
            workflow_id="abc123",
            task_name="train",
            success=False,
            error="OOM",
        )
        c2 = WorkflowCompletion.deserialize(c.serialize())
        assert c2.success is False, "success should be False"
        assert c2.error == "OOM", "error should equal 'OOM'"
        assert c2.output_ref is None, "output reference should be None"

    def test_completion_deserialize_bytes(self):
        """Verify that WorkflowCompletion.deserialize accepts byte strings."""
        c = WorkflowCompletion(workflow_id="x", task_name="y", success=True)
        c2 = WorkflowCompletion.deserialize(c.serialize().encode())
        assert c2.workflow_id == "x", "workflow ID should equal 'x'"

    def test_completion_ignores_legacy_version_field(self):
        """A legacy v1 message with a stray ``version`` key still parses."""
        import json as _json

        legacy = _json.dumps({"version": 1, "workflow_id": "x", "task_name": "y", "success": True})
        c = WorkflowCompletion.deserialize(legacy)
        assert c.workflow_id == "x", "workflow ID should equal 'x'"
        assert c.task_name == "y", "task name should equal 'y'"
        assert c.success is True, "success should be True"


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


class TestOutputHelpers:
    def test_serialize_deserialize_inline(self):
        """Verify that inline workflow output serializes and deserializes without loss."""
        data = {"mae": 0.42, "path": "/data/model.pt"}
        s = serialize_output(data)
        assert deserialize_output(s) == data, "deserialized output should equal data"

    def test_deserialize_blob_ref(self):
        """Verify that blob references are returned unchanged during output deserialization."""
        ref = "blob:abc123.json:d41d8cd98f00b204e9800998ecf8427e"
        assert deserialize_output(ref) == ref, "deserialized output should equal ref"

    def test_output_needs_blob_small(self):
        """Verify that small serialized outputs stay inline."""
        assert not output_needs_blob('{"small": true}'), "blob storage decision should be False"

    def test_output_needs_blob_large(self):
        """Verify that large serialized outputs are marked for blob storage."""
        large = json.dumps({"data": "x" * 50_000})
        assert output_needs_blob(large), "blob storage decision should be True"


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_generate_workflow_id():
    """Verify that generate_workflow_id returns unique 32-character hex strings."""
    wid = generate_workflow_id()
    assert isinstance(wid, str), "wid should be a str"
    assert len(wid) == 32, "wid length should equal 32"  # uuid4 hex
    assert wid != generate_workflow_id(), "wid should not equal generate_workflow_id()"


def test_task_state_is_active():
    """Verify that TaskState.is_active distinguishes active and terminal states."""
    assert TaskState.is_active(TaskState.READY), "task state active check should be True"
    assert not TaskState.is_active(TaskState.COMPLETED), "task state active check should be False"


# ---------------------------------------------------------------------------
# Condition expression evaluator
# ---------------------------------------------------------------------------


class TestConditionEvaluate:
    def test_simple_comparison(self):
        """Verify that the condition evaluator handles simple numeric comparisons."""
        assert evaluate_condition("inputs.A.mae > 0.28", {"A": {"mae": 0.30}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.mae > 0.28", {"A": {"mae": 0.20}}) is False, (
            "condition should be False"
        )

    def test_eq_ne(self):
        """Verify that the condition evaluator supports equality and inequality operators."""
        assert evaluate_condition('inputs.A.status == "pass"', {"A": {"status": "pass"}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition('inputs.A.status != "fail"', {"A": {"status": "pass"}}) is True, (
            "condition should be True"
        )
        assert (
            evaluate_condition('inputs.A.status != "fail"', {"A": {"status": "fail"}}) is False
        ), "condition should be False"

    def test_le_ge_lt(self):
        """Verify that the condition evaluator supports less-than and greater-than comparisons."""
        assert evaluate_condition("inputs.A.x <= 5", {"A": {"x": 5}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.x <= 5", {"A": {"x": 6}}) is False, (
            "condition should be False"
        )
        assert evaluate_condition("inputs.A.x >= 5", {"A": {"x": 5}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.x < 5", {"A": {"x": 4}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.x < 5", {"A": {"x": 5}}) is False, (
            "condition should be False"
        )

    def test_in_operator(self):
        """Verify that the condition evaluator supports membership checks."""
        inputs = {"A": {"tier": "gold"}}
        assert evaluate_condition('inputs.A.tier in ["gold", "silver"]', inputs) is True, (
            "condition should be True"
        )
        assert evaluate_condition('inputs.A.tier in ["bronze"]', inputs) is False, (
            "condition should be False"
        )

    def test_not_in(self):
        """Verify that the condition evaluator supports negated membership checks."""
        assert (
            evaluate_condition('inputs.A.tier not in ["bad"]', {"A": {"tier": "good"}}) is True
        ), "condition should be True"
        assert (
            evaluate_condition('inputs.A.tier not in ["bad"]', {"A": {"tier": "bad"}}) is False
        ), "condition should be False"

    def test_boolean_and(self):
        """Verify that the condition evaluator combines expressions with logical and."""
        inputs = {"A": {"score": 0.9}, "B": {"loss": 0.05}}
        assert evaluate_condition("inputs.A.score > 0.5 and inputs.B.loss < 0.1", inputs) is True, (
            "condition should be True"
        )
        assert (
            evaluate_condition("inputs.A.score > 0.5 and inputs.B.loss > 0.1", inputs) is False
        ), "condition should be False"

    def test_boolean_or(self):
        """Verify that the condition evaluator combines expressions with logical or."""
        inputs = {"A": {"status": "ok"}, "B": {"status": "fail"}}
        assert (
            evaluate_condition('inputs.A.status == "ok" or inputs.B.status == "ok"', inputs) is True
        ), "condition should be True"
        assert (
            evaluate_condition('inputs.A.status == "fail" or inputs.B.status == "ok"', inputs)
            is False
        ), "condition should be False"

    def test_not_operator(self):
        """Verify that the condition evaluator negates boolean input fields."""
        assert (
            evaluate_condition("not inputs.A.has_errors", {"A": {"has_errors": False}}) is True
        ), "condition should be True"
        assert (
            evaluate_condition("not inputs.A.has_errors", {"A": {"has_errors": True}}) is False
        ), "condition should be False"

    def test_bracket_access(self):
        """Verify that the condition evaluator supports bracket access for task names with special characters."""
        inputs = {"prep/featurize": {"passed": True}}
        assert evaluate_condition('inputs["prep/featurize"].passed == True', inputs) is True, (
            "condition should be True"
        )

    def test_nested_dict_access(self):
        """Verify that the condition evaluator traverses nested dictionaries in input payloads."""
        inputs = {"A": {"metrics": {"mae": 0.05}}}
        assert evaluate_condition("inputs.A.metrics.mae < 0.1", inputs) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.metrics.mae < 0.01", inputs) is False, (
            "condition should be False"
        )

    def test_missing_field_returns_false(self):
        """Verify that missing input fields make the condition evaluate to False."""
        assert evaluate_condition("inputs.A.missing > 0.5", {"A": {"other": 1}}) is False, (
            "condition should be False"
        )

    def test_none_input_returns_false(self):
        """Verify that None inputs make the condition evaluate to False."""
        assert evaluate_condition("inputs.A.x > 0", {"A": None}) is False, (
            "condition should be False"
        )

    def test_true_false_null_literals(self):
        """Verify that the condition evaluator accepts JSON-style and Python boolean and null literals."""
        assert evaluate_condition("inputs.A.ok == true", {"A": {"ok": True}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.ok == false", {"A": {"ok": False}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.val == null", {"A": {"val": None}}) is True, (
            "condition should be True"
        )
        assert evaluate_condition("inputs.A.ok == True", {"A": {"ok": True}}) is True, (
            "condition should be True"
        )

    def test_chained_comparison(self):
        """Verify that the condition evaluator supports chained comparisons."""
        inputs = {"A": {"x": 5}}
        assert evaluate_condition("1 < inputs.A.x < 10", inputs) is True, "condition should be True"
        assert evaluate_condition("6 < inputs.A.x < 10", inputs) is False, (
            "condition should be False"
        )


class TestConditionValidate:
    def test_valid_expression(self):
        """Verify that validate_condition accepts a simple condition that references a dependency."""
        validate_condition("inputs.A.score > 0.5", ["A"])

    def test_multi_input_valid(self):
        """Verify that validate_condition accepts conditions that combine multiple dependencies."""
        validate_condition("inputs.A.score > 0.5 and inputs.B.loss < 0.1", ["A", "B"])

    def test_bracket_syntax_valid(self):
        """Verify that validate_condition accepts bracket syntax for dependency names with special characters."""
        validate_condition('inputs["prep/feat"].x > 0', ["prep/feat"])

    def test_unknown_task_ref(self):
        """Verify that validate_condition rejects references to tasks outside depends_on."""
        with pytest.raises(ValueError, match="not in depends_on"):
            validate_condition("inputs.C.x > 0", ["A", "B"])

    def test_no_input_refs(self):
        """Verify that validate_condition rejects conditions without input references."""
        with pytest.raises(ValueError, match="must reference at least one input"):
            validate_condition("1 > 0", ["A"])

    def test_syntax_error(self):
        """Verify that validate_condition reports syntax errors in the expression."""
        with pytest.raises(ValueError, match="syntax error"):
            validate_condition("inputs.A.x >", ["A"])

    def test_disallowed_function_call(self):
        """Verify that validate_condition rejects non-aggregate function calls."""
        with pytest.raises(ValueError, match="Only aggregate functions"):
            validate_condition("len(inputs.A.items) > 0", ["A"])

    def test_disallowed_import(self):
        """Verify that validate_condition rejects import-based expressions."""
        with pytest.raises(ValueError, match="Only aggregate functions"):
            validate_condition("__import__('os').system('echo pwned')", ["A"])

    def test_unknown_variable(self):
        """Verify that validate_condition rejects unknown variable names."""
        with pytest.raises(ValueError, match="Unknown variable"):
            validate_condition("foo > 0", ["A"])


class TestConditionInWorkflow:
    def test_condition_serialization_roundtrip(self):
        """Verify that workflow serialization preserves task conditions across WorkflowDefinition and WorkflowTask."""
        wf = WorkflowDefinition(
            name="cond-test",
            tasks=[
                WorkflowTask(name="train", kwargs={"lr": 0.01}),
                WorkflowTask(
                    name="deploy",
                    depends_on=["train"],
                    condition="inputs.train.mae < 0.1",
                ),
            ],
        )
        wf.validate()
        j = wf.to_json()
        restored = WorkflowDefinition.from_json(j)
        assert restored.task_map["deploy"].condition == "inputs.train.mae < 0.1", (
            "condition should equal 'inputs.train.mae < 0.1'"
        )

    def test_condition_references_unknown_task(self):
        """Verify that workflow validation rejects conditions that reference non-dependencies."""
        wf = WorkflowDefinition(
            name="bad",
            tasks=[
                WorkflowTask(name="A"),
                WorkflowTask(name="B"),
                WorkflowTask(
                    name="C",
                    depends_on=["A"],
                    condition="inputs.B.x > 0",
                ),
            ],
        )
        with pytest.raises(ValueError, match="not in depends_on"):
            wf.validate()

    def test_condition_requires_dependency(self):
        """Verify that workflow validation rejects conditions on tasks without dependencies."""
        wf = WorkflowDefinition(
            name="bad",
            tasks=[
                WorkflowTask(
                    name="A",
                    condition="inputs.X.x > 0",
                ),
            ],
        )
        with pytest.raises(ValueError, match="requires at least one dependency"):
            wf.validate()

    def test_multi_input_condition(self):
        """Verify that workflow validation accepts conditions that reference multiple dependencies."""
        wf = WorkflowDefinition(
            name="multi",
            tasks=[
                WorkflowTask(name="A"),
                WorkflowTask(name="B"),
                WorkflowTask(
                    name="C",
                    depends_on=["A", "B"],
                    condition="inputs.A.score > 0.5 and inputs.B.loss < 0.1",
                ),
            ],
        )
        wf.validate()  # should not raise

    def test_no_condition_is_fine(self):
        """Verify that workflow validation allows tasks without conditions."""
        wf = WorkflowDefinition(
            name="simple",
            tasks=[
                WorkflowTask(name="A"),
                WorkflowTask(name="B", depends_on=["A"]),
            ],
        )
        wf.validate()
        assert wf.task_map["B"].condition is None, "condition should be None"


# ---------------------------------------------------------------------------
# Aggregate condition functions
# ---------------------------------------------------------------------------


class TestAggregateEvaluate:
    """Tests for all(), any(), almost_all(), count() with glob patterns."""

    def setup_method(self):
        self.inputs = {
            "train-a": {"loss": 0.005, "converged": True},
            "train-b": {"loss": 0.02, "converged": True},
            "train-c": {"loss": 0.05, "converged": False},
            "prep": {"loss": 0.0, "status": "ok"},
        }

    def test_all_wildcard_true(self):
        """Verify that all() succeeds when every wildcard-matched input satisfies the predicate."""
        assert evaluate_condition('all(inputs["*"].loss < 1.0)', self.inputs), (
            "condition should be True"
        )

    def test_all_wildcard_false(self):
        """Verify that all() fails when any wildcard-matched input breaks the predicate."""
        assert not evaluate_condition('all(inputs["*"].loss < 0.01)', self.inputs), (
            "condition should be False"
        )

    def test_all_glob_pattern(self):
        # Only train-* inputs, all have loss < 0.1
        """Verify that all() applies glob filtering before evaluating inputs."""
        assert evaluate_condition('all(inputs["train-*"].loss < 0.1)', self.inputs), (
            "condition should be True"
        )

    def test_all_glob_pattern_false(self):
        """Verify that all() returns False when a glob-matched input fails the predicate."""
        assert not evaluate_condition('all(inputs["train-*"].loss < 0.01)', self.inputs), (
            "condition should be False"
        )

    def test_any_true(self):
        """Verify that any() returns True when one glob-matched input satisfies the predicate."""
        assert evaluate_condition('any(inputs["train-*"].loss < 0.01)', self.inputs), (
            "condition should be True"
        )

    def test_any_false(self):
        """Verify that any() returns False when no glob-matched inputs satisfy the predicate."""
        assert not evaluate_condition('any(inputs["train-*"].loss < 0.001)', self.inputs), (
            "condition should be False"
        )

    def test_count(self):
        """Verify that count() can be compared against numeric thresholds."""
        assert evaluate_condition('count(inputs["train-*"].loss < 0.03) >= 2', self.inputs), (
            "condition should be True"
        )

    def test_count_exact(self):
        """Verify that count() returns the exact number of matching inputs."""
        assert evaluate_condition('count(inputs["train-*"].converged) == 2', self.inputs), (
            "condition should be True"
        )

    def test_almost_all_pass(self):
        # 1 failure out of 3 train-* tasks, max_failures=1 → passes
        """Verify that almost_all() allows failures up to the configured tolerance."""
        assert evaluate_condition('almost_all(inputs["train-*"].loss < 0.03, 1)', self.inputs), (
            "condition should be True"
        )

    def test_almost_all_fail(self):
        # 2 failures out of 3 train-* tasks, max_failures=1 → fails
        """Verify that almost_all() fails when matches exceed the configured failure tolerance."""
        assert not evaluate_condition(
            'almost_all(inputs["train-*"].loss < 0.01, 1)', self.inputs
        ), "condition should be False"

    def test_almost_all_zero_tolerance(self):
        # 0 tolerance is equivalent to all()
        """Verify that almost_all() with zero tolerance behaves like all()."""
        assert not evaluate_condition(
            'almost_all(inputs["train-*"].loss < 0.03, 0)', self.inputs
        ), "condition should be False"

    def test_inputs_star_shorthand(self):
        # inputs.* is preprocessed to inputs["*"]
        """Verify that inputs.* shorthand is normalized before aggregate evaluation."""
        assert evaluate_condition("all(inputs.*.loss < 1.0)", self.inputs), (
            "condition should be True"
        )

    def test_inputs_star_shorthand_false(self):
        """Verify that inputs.* shorthand still returns False when a predicate fails."""
        assert not evaluate_condition("all(inputs.*.loss < 0.01)", self.inputs), (
            "condition should be False"
        )

    def test_no_glob_match_all(self):
        # No inputs match "missing-*" — all() on empty set is vacuously true
        """Verify that all() over an empty glob match is vacuously True."""
        assert evaluate_condition('all(inputs["missing-*"].loss < 0.01)', self.inputs), (
            "condition should be True"
        )

    def test_no_glob_match_any(self):
        """Verify that any() over an empty glob match is False."""
        assert not evaluate_condition('any(inputs["missing-*"].loss < 0.01)', self.inputs), (
            "condition should be False"
        )

    def test_no_glob_match_count(self):
        """Verify that count() over an empty glob match returns zero."""
        assert evaluate_condition('count(inputs["missing-*"].loss < 0.01) == 0', self.inputs), (
            "condition should be True"
        )

    def test_combined_with_explicit_ref(self):
        # Mix glob aggregate with explicit input reference
        """Verify that aggregate conditions can be combined with explicit task references."""
        assert evaluate_condition(
            'all(inputs["train-*"].loss < 0.1) and inputs.prep.status == "ok"',
            self.inputs,
        ), "condition should be True"

    def test_nested_field_access(self):
        """Verify that aggregate conditions can read nested fields from matched inputs."""
        inputs = {
            "a": {"metrics": {"loss": 0.01}},
            "b": {"metrics": {"loss": 0.02}},
        }
        assert evaluate_condition('all(inputs["*"].metrics.loss < 0.1)', inputs), (
            "condition should be True"
        )


class TestAggregateValidate:
    def test_glob_wildcard_valid(self):
        """Verify that validate_condition accepts wildcard aggregate expressions."""
        validate_condition('all(inputs["*"].loss < 0.1)', ["A", "B"])

    def test_glob_pattern_valid(self):
        """Verify that validate_condition accepts glob-filtered aggregate expressions."""
        validate_condition('any(inputs["train-*"].loss < 0.1)', ["train-a", "train-b"])

    def test_star_shorthand_valid(self):
        """Verify that validate_condition accepts inputs.* shorthand in aggregate expressions."""
        validate_condition("all(inputs.*.loss < 0.1)", ["A"])

    def test_almost_all_valid(self):
        """Verify that validate_condition accepts almost_all with an explicit tolerance."""
        validate_condition('almost_all(inputs["*"].loss < 0.1, 3)', ["A"])

    def test_disallowed_function(self):
        """Verify that validate_condition rejects non-aggregate helper calls in aggregate expressions."""
        with pytest.raises(ValueError, match="Only aggregate functions"):
            validate_condition("len(inputs) > 0", ["A"])

    def test_glob_with_explicit_refs(self):
        # Glob + explicit ref: explicit refs must be in depends_on
        """Verify that validate_condition allows aggregate expressions mixed with valid explicit dependency references."""
        validate_condition('all(inputs["*"].loss < 0.1) and inputs.B.ok', ["A", "B"])

    def test_glob_with_bad_explicit_ref(self):
        """Verify that validate_condition rejects aggregate expressions with explicit references outside depends_on."""
        with pytest.raises(ValueError, match="not in depends_on"):
            validate_condition('all(inputs["*"].loss < 0.1) and inputs.Z.ok', ["A", "B"])


# ---------------------------------------------------------------------------
# sequentialize_fan_in transform
# ---------------------------------------------------------------------------


class TestSequentializeFanIn:
    """Tests for the DAG transform that restructures large fan-ins."""

    def test_small_fanin_unchanged(self):
        """Tasks below max_fan_in are not modified."""
        from ai4s.jobq.workflow.transforms import sequentialize_fan_in

        wf = WorkflowDefinition(
            name="small",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(name="b"),
                WorkflowTask(name="c", depends_on=["a", "b"]),
            ],
        )
        result = sequentialize_fan_in(wf, max_fan_in=5)
        assert len(result.tasks) == 3, "tasks length should equal 3"
        assert result.task_map["c"].depends_on == ["a", "b"], "depends on should equal ['a', 'b']"

    def test_creates_merge_nodes(self):
        """A 10-way fan-in with max_fan_in=3 creates merge nodes."""
        from ai4s.jobq.workflow.transforms import sequentialize_fan_in

        roots = [WorkflowTask(name=f"r{i}") for i in range(10)]
        leaf = WorkflowTask(name="leaf", depends_on=[f"r{i}" for i in range(10)])
        wf = WorkflowDefinition(name="wide", tasks=[*roots, leaf])

        result = sequentialize_fan_in(wf, max_fan_in=3)

        # Leaf should now depend on only the last merge node
        assert len(result.task_map["leaf"].depends_on) == 1, "depends on length should equal 1"
        last_merge = result.task_map["leaf"].depends_on[0]
        assert last_merge.startswith("__merge_leaf_"), "result should be True"

        # Merge nodes should exist
        merge_tasks = [t for t in result.tasks if t.name.startswith("__merge_")]
        assert len(merge_tasks) == 4, "merge tasks length should equal 4"  # ceil(10/3) = 4 batches

    def test_merge_to_merge_edges(self):
        """Merge nodes link to the previous merge for transitive closure."""
        from ai4s.jobq.workflow.transforms import sequentialize_fan_in

        roots = [WorkflowTask(name=f"r{i}") for i in range(9)]
        leaf = WorkflowTask(name="leaf", depends_on=[f"r{i}" for i in range(9)])
        wf = WorkflowDefinition(name="chain", tasks=[*roots, leaf])

        result = sequentialize_fan_in(wf, max_fan_in=3)

        merge_0 = result.task_map["__merge_leaf_0"]
        merge_1 = result.task_map["__merge_leaf_1"]
        merge_2 = result.task_map["__merge_leaf_2"]

        # First merge has no merge→merge edge (no previous)
        assert "__merge_" not in " ".join(merge_0.depends_on), (
            "'__merge_' should not be in ' '.join(merge_0.depends_on)"
        )
        # Second merge depends on its batch + merge_0
        assert "__merge_leaf_0" in merge_1.depends_on, (
            "'__merge_leaf_0' should be in merge_1.depends_on"
        )
        # Third merge depends on its batch + merge_1
        assert "__merge_leaf_1" in merge_2.depends_on, (
            "'__merge_leaf_1' should be in merge_2.depends_on"
        )

    def test_transitive_closure_finds_all_roots(self):
        """Walking merge→merge edges discovers all real upstream tasks."""
        from ai4s.jobq.workflow.transforms import sequentialize_fan_in

        roots = [WorkflowTask(name=f"r{i}") for i in range(9)]
        leaf = WorkflowTask(name="leaf", depends_on=[f"r{i}" for i in range(9)])
        wf = WorkflowDefinition(name="walk", tasks=[*roots, leaf])

        result = sequentialize_fan_in(wf, max_fan_in=3)

        # Simulate the walk that get_real_upstream_tasks does
        task_map = result.task_map
        leaf_deps = task_map["leaf"].depends_on
        real: list[str] = []
        queue = list(leaf_deps)
        seen: set[str] = set()
        while queue:
            name = queue.pop()
            if name in seen:
                continue
            seen.add(name)
            if name.startswith("__merge_"):
                queue.extend(task_map[name].depends_on)
            else:
                real.append(name)

        assert sorted(real) == [f"r{i}" for i in range(9)], (
            "sorted real should equal [f'r{i}' for i in range(9)]"
        )

    def test_condition_with_merge_nodes_rejected(self):
        """Validate rejects conditions on tasks with __merge_ dependencies."""
        wf = WorkflowDefinition(
            name="bad",
            tasks=[
                WorkflowTask(name="a"),
                WorkflowTask(
                    name="b",
                    depends_on=["__merge_b_0"],
                    condition="inputs.__merge_b_0.x > 0",
                ),
                WorkflowTask(
                    name="__merge_b_0",
                    kwargs={"__batch_merge": True},
                    depends_on=["a"],
                ),
            ],
        )
        with pytest.raises(ValueError, match="incompatible with sequentialize_fan_in"):
            wf.validate()

    def test_condition_on_non_fanin_task_ok(self):
        """Conditions on tasks without merge deps still work fine."""
        from ai4s.jobq.workflow.transforms import sequentialize_fan_in

        roots = [WorkflowTask(name=f"r{i}") for i in range(10)]
        # Task with condition but only 2 deps — not affected by transform
        conditioned = WorkflowTask(
            name="check",
            depends_on=["r0", "r1"],
            condition="inputs.r0.ok and inputs.r1.ok",
        )
        leaf = WorkflowTask(name="leaf", depends_on=[f"r{i}" for i in range(10)])
        wf = WorkflowDefinition(name="mixed", tasks=[*roots, conditioned, leaf])

        result = sequentialize_fan_in(wf, max_fan_in=3)
        result.validate()  # should not raise
        # conditioned task is unchanged
        assert result.task_map["check"].condition == "inputs.r0.ok and inputs.r1.ok", (
            "condition should equal 'inputs.r0.ok and inputs.r1.ok'"
        )

    def test_max_fan_in_too_small_raises(self):
        """Verify that sequentialize_fan_in rejects max_fan_in values smaller than two."""
        from ai4s.jobq.workflow.transforms import sequentialize_fan_in

        wf = WorkflowDefinition(name="x", tasks=[WorkflowTask(name="a")])
        with pytest.raises(ValueError, match="must be >= 2"):
            sequentialize_fan_in(wf, max_fan_in=1)


# ---------------------------------------------------------------------------
# max_parallelism (L2: per-workflow fan-out budget)
# ---------------------------------------------------------------------------


class TestMaxParallelism:
    """Validation + round-trip for the WorkflowDefinition.max_parallelism field."""

    def test_default_is_none(self):
        wf = WorkflowDefinition(name="x", tasks=[WorkflowTask(name="a")])
        assert wf.max_parallelism is None, (
            f"default should be None (unlimited), got {wf.max_parallelism!r}"
        )

    def test_positive_value_validates(self):
        wf = WorkflowDefinition(name="x", tasks=[WorkflowTask(name="a")], max_parallelism=4)
        wf.validate()  # must not raise

    def test_zero_rejected(self):
        wf = WorkflowDefinition(name="x", tasks=[WorkflowTask(name="a")], max_parallelism=0)
        with pytest.raises(ValueError, match="max_parallelism"):
            wf.validate()

    def test_negative_rejected(self):
        wf = WorkflowDefinition(name="x", tasks=[WorkflowTask(name="a")], max_parallelism=-3)
        with pytest.raises(ValueError, match="max_parallelism"):
            wf.validate()

    def test_bool_rejected(self):
        # bools are technically ints in Python — guard against them
        # explicitly so True/False can't slip past as a budget value.
        wf = WorkflowDefinition(
            name="x",
            tasks=[WorkflowTask(name="a")],
            max_parallelism=True,  # type: ignore[arg-type]
        )
        with pytest.raises(ValueError, match="max_parallelism"):
            wf.validate()

    def test_round_trip_when_set(self):
        wf = WorkflowDefinition(name="x", tasks=[WorkflowTask(name="a")], max_parallelism=7)
        restored = WorkflowDefinition.from_json(wf.to_json())
        assert restored.max_parallelism == 7

    def test_round_trip_when_unset_omits_field(self):
        wf = WorkflowDefinition(name="x", tasks=[WorkflowTask(name="a")])
        # When unset, max_parallelism is omitted from the serialised form
        # so existing storage payloads round-trip byte-for-byte.
        d = wf._to_dict()
        assert "max_parallelism" not in d, (
            f"unset max_parallelism should be omitted from dict form, got keys {list(d)}"
        )
        restored = WorkflowDefinition.from_json(wf.to_json())
        assert restored.max_parallelism is None

    def test_from_dict_rejects_invalid_value(self):
        with pytest.raises(ValueError, match="max_parallelism"):
            WorkflowDefinition.from_json(
                '{"name": "x", "tasks": [{"name": "a"}], "max_parallelism": 0}'
            )


# ---------------------------------------------------------------------------
# READY_PENDING_BUDGET task state (L2)
# ---------------------------------------------------------------------------


class TestReadyPendingBudgetState:
    def test_value_is_stable_string(self):
        # The string value persists in storage rows; renaming would
        # require a migration, so pin it.
        assert TaskState.READY_PENDING_BUDGET.value == "ready_pending_budget"

    def test_counts_as_active(self):
        # Tasks parked by the budget are still "in the funnel" and must
        # count toward the workflow's active set.
        assert TaskState.is_active(TaskState.READY_PENDING_BUDGET)
        assert not TaskState.is_terminal(TaskState.READY_PENDING_BUDGET)

    def test_does_not_satisfy_or_fail_dependencies(self):
        # A parked task hasn't run yet: downstream tasks must not see
        # it as either completed or failed.
        assert not TaskState.is_dep_satisfied(TaskState.READY_PENDING_BUDGET)
        assert not TaskState.is_dep_failed(TaskState.READY_PENDING_BUDGET)
