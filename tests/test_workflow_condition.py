# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for uncovered workflow condition helper branches."""

from __future__ import annotations

import ast
from types import SimpleNamespace

import pytest

from ai4s.jobq.workflow.condition import (
    _check_allowed,
    _eval_aggregate,
    _eval_node,
    _find_glob_subscript,
)


class TestConditionHelpers:
    def test_check_allowed_rejects_disallowed_node_type(self):
        """Verify that _check_allowed rejects AST nodes outside the supported expression subset."""
        tree = ast.parse("inputs.A if True else inputs.B", mode="eval")

        with pytest.raises(ValueError, match="Disallowed expression element: IfExp"):
            _check_allowed(tree)

    def test_eval_attribute_reads_object_attributes(self):
        """Verify that _eval_node resolves attribute access on input objects."""
        node = ast.parse("inputs.A.value", mode="eval").body

        assert _eval_node(node, {"A": SimpleNamespace(value=7)}) == 7, "result should equal 7"

    def test_eval_subscript_returns_none_for_none_object(self):
        """Verify that _eval_node returns None for subscript access on a None input."""
        node = ast.parse('inputs.A["value"]', mode="eval").body

        assert _eval_node(node, {"A": None}) is None, "result should be None"

    def test_eval_subscript_indexes_non_dict_objects(self):
        """Verify that _eval_node can index non-dict inputs such as lists."""
        node = ast.parse("inputs.A[1]", mode="eval").body

        assert _eval_node(node, {"A": ["zero", "one"]}) == "one", "result should equal 'one'"

    def test_find_glob_subscript_returns_none_without_glob(self):
        """Verify that _find_glob_subscript returns None when the expression contains no glob input reference."""
        node = ast.parse("inputs.A.value > 0", mode="eval").body

        assert _find_glob_subscript(node) is None, "result should be None"

    def test_aggregate_requires_a_glob_pattern(self):
        """Verify that _eval_aggregate rejects aggregate calls without a glob input pattern."""
        node = ast.parse("all(inputs.A.ok)", mode="eval").body
        assert isinstance(node, ast.Call), "node should be a ast.Call"

        with pytest.raises(
            ValueError, match=r'all\(\) requires a glob pattern like inputs\["\*"\]'
        ):
            _eval_aggregate(node, {"A": {"ok": True}})

    def test_almost_all_requires_max_failures_argument(self):
        """Verify that _eval_aggregate requires the max_failures argument for almost_all."""
        node = ast.parse('almost_all(inputs["*"].ok)', mode="eval").body
        assert isinstance(node, ast.Call), "node should be a ast.Call"

        with pytest.raises(ValueError, match=r"almost_all\(\) requires a second argument"):
            _eval_aggregate(node, {"A": {"ok": True}})
