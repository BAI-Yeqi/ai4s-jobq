# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Safe expression evaluator for workflow task conditions.

Conditions are Python-like expressions evaluated against upstream task
outputs.  The ``inputs`` variable is a dict mapping each direct
dependency name to its JSON output.

Examples::

    inputs.train.mae < 0.1
    inputs.A.score > 0.28 and inputs.B.loss < 0.1
    inputs["prep/featurize"].passed == True
    not inputs.validate.has_errors

Aggregate functions with glob patterns::

    all(inputs["*"].loss < 0.01)
    all(inputs.*.loss < 0.01)          # shorthand for inputs["*"]
    any(inputs["train-*"].converged)
    almost_all(inputs["*"].loss < 0.01, 5)   # at most 5 failures
    count(inputs["*"].passed) > 3

Supported syntax:

- Comparisons: ``==  !=  >  >=  <  <=  in  not in``
- Boolean operators: ``and  or  not``
- Literals: numbers, strings (quoted), ``True``, ``False``, ``None``
  (also accepts ``true``, ``false``, ``null``)
- Attribute access: ``inputs.A.field``
- Bracket access: ``inputs["name/with-slash"].field``
- Aggregate functions: ``all``, ``any``, ``almost_all``, ``count``
  (require a glob pattern like ``inputs["*"]`` or ``inputs["prefix-*"]``)

All other constructs (imports, comprehensions, assignments, arbitrary
function calls, etc.) are rejected at parse time.
"""

from __future__ import annotations

import ast
import fnmatch
import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

LOG = logging.getLogger(__name__)

# Node types we allow in condition expressions.
_ALLOWED_NODES: frozenset[type] = frozenset(
    {
        ast.Expression,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.UnaryOp,
        ast.Not,
        ast.Compare,
        ast.Eq,
        ast.NotEq,
        ast.Gt,
        ast.GtE,
        ast.Lt,
        ast.LtE,
        ast.In,
        ast.NotIn,
        ast.Constant,
        ast.Name,
        ast.Load,
        ast.Attribute,
        ast.Subscript,
        ast.List,
        ast.Tuple,
        ast.Call,
    }
)

# Aggregate functions that may be called in expressions.
_AGGREGATE_FUNCS: frozenset[str] = frozenset({"all", "any", "almost_all", "count"})

# Names that are valid as standalone identifiers in an expression.
_KNOWN_NAMES: frozenset[str] = frozenset(
    {"inputs", "true", "false", "null", "True", "False", "None"} | _AGGREGATE_FUNCS
)

# Regex to convert inputs.* shorthand to inputs["*"].
_INPUTS_STAR_RE = re.compile(r"inputs\.\*")


def validate_condition(expr: str, depends_on: list[str]) -> None:
    """Parse *expr* and check it only uses allowed constructs.

    Raises ``ValueError`` on syntax errors, disallowed constructs, or
    references to tasks not listed in *depends_on*.
    """
    expr = _preprocess(expr)
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"Condition syntax error: {exc.msg}") from exc

    _check_allowed(tree)
    refs, has_glob = _collect_input_refs(tree)
    dep_set = set(depends_on)
    unknown = refs - dep_set
    if unknown:
        raise ValueError(f"Condition references tasks not in depends_on: {sorted(unknown)}")
    if not refs and not has_glob:
        raise ValueError("Condition must reference at least one input (inputs.task_name.field)")


def evaluate_condition(expr: str, inputs: dict[str, Any]) -> bool:
    """Evaluate *expr* against *inputs* and return a boolean.

    If evaluation fails (missing field, type error, etc.) returns
    ``False`` rather than raising — a failing condition skips the task.
    """
    try:
        expr = _preprocess(expr)
        tree = ast.parse(expr, mode="eval")
        result = _eval_node(tree.body, inputs)
        return bool(result)
    except Exception:
        LOG.debug("Condition evaluation failed for %r", expr, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------


def _preprocess(expr: str) -> str:
    """Convert ``inputs.*`` shorthand to ``inputs["*"]``."""
    return _INPUTS_STAR_RE.sub('inputs["*"]', expr)


# ---------------------------------------------------------------------------
# AST validation
# ---------------------------------------------------------------------------


def _check_allowed(node: ast.AST) -> None:
    """Recursively verify every node in *node* is in the allow-list."""
    for child in ast.walk(node):
        if type(child) not in _ALLOWED_NODES:
            raise ValueError(f"Disallowed expression element: {type(child).__name__}")
        # Call nodes must use whitelisted function names
        if isinstance(child, ast.Call) and not (
            isinstance(child.func, ast.Name) and child.func.id in _AGGREGATE_FUNCS
        ):
            raise ValueError(f"Only aggregate functions are allowed: {sorted(_AGGREGATE_FUNCS)}")
        # Only known names are valid
        if isinstance(child, ast.Name) and child.id not in _KNOWN_NAMES:
            raise ValueError(f"Unknown variable: {child.id!r}")


def _collect_input_refs(node: ast.AST) -> tuple[set[str], bool]:
    """Return (explicit_refs, has_glob).

    *explicit_refs* is the set of literal task names referenced via
    ``inputs.X`` or ``inputs["X"]``.  *has_glob* is ``True`` if the
    expression contains a glob pattern like ``inputs["*"]``.
    """
    refs: set[str] = set()
    has_glob = False
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Attribute)
            and isinstance(child.value, ast.Name)
            and child.value.id == "inputs"
        ):
            refs.add(child.attr)
        elif (
            isinstance(child, ast.Subscript)
            and isinstance(child.value, ast.Name)
            and child.value.id == "inputs"
            and isinstance(child.slice, ast.Constant)
            and isinstance(child.slice.value, str)
        ):
            if _is_glob(child.slice.value):
                has_glob = True
            else:
                refs.add(child.slice.value)
    return refs, has_glob


def _is_glob(pattern: str) -> bool:
    """Return ``True`` if *pattern* contains glob metacharacters."""
    return bool(set(pattern) & {"*", "?", "["})


# ---------------------------------------------------------------------------
# AST evaluation
# ---------------------------------------------------------------------------


def _eval_constant(node: ast.AST, _inputs: dict[str, Any]) -> Any:
    assert isinstance(node, ast.Constant)
    return node.value


def _eval_name(node: ast.AST, inputs: dict[str, Any]) -> Any:
    assert isinstance(node, ast.Name)
    if node.id == "inputs":
        return inputs
    if node.id in ("true", "True"):
        return True
    if node.id in ("false", "False"):
        return False
    if node.id in ("null", "None"):
        return None
    raise ValueError(f"Unknown variable: {node.id!r}")  # pragma: no cover


def _eval_attribute(node: ast.AST, inputs: dict[str, Any]) -> Any:
    assert isinstance(node, ast.Attribute)
    obj = _eval_node(node.value, inputs)
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(node.attr)
    return getattr(obj, node.attr, None)


def _eval_subscript(node: ast.AST, inputs: dict[str, Any]) -> Any:
    assert isinstance(node, ast.Subscript)
    obj = _eval_node(node.value, inputs)
    if obj is None:
        return None
    key = _eval_node(node.slice, inputs)
    if isinstance(obj, dict):
        return obj.get(key)
    return obj[key]


def _eval_sequence(node: ast.AST, inputs: dict[str, Any]) -> list[Any]:
    assert isinstance(node, (ast.List, ast.Tuple))
    return [_eval_node(elt, inputs) for elt in node.elts]


def _eval_compare(node: ast.AST, inputs: dict[str, Any]) -> bool:
    assert isinstance(node, ast.Compare)
    left = _eval_node(node.left, inputs)
    for op, comparator in zip(node.ops, node.comparators, strict=True):
        right = _eval_node(comparator, inputs)
        if not _compare(left, op, right):
            return False
        left = right
    return True


def _eval_boolop(node: ast.AST, inputs: dict[str, Any]) -> bool:
    assert isinstance(node, ast.BoolOp)
    if isinstance(node.op, ast.And):
        return all(_eval_node(v, inputs) for v in node.values)
    return any(_eval_node(v, inputs) for v in node.values)


def _eval_unaryop(node: ast.AST, inputs: dict[str, Any]) -> bool:
    assert isinstance(node, ast.UnaryOp)
    assert isinstance(node.op, ast.Not)
    return not _eval_node(node.operand, inputs)


def _eval_call(node: ast.AST, inputs: dict[str, Any]) -> Any:
    assert isinstance(node, ast.Call)
    return _eval_aggregate(node, inputs)


_EVAL_DISPATCH: dict[type, Callable[[ast.AST, dict[str, Any]], Any]] = {
    ast.Constant: _eval_constant,
    ast.Name: _eval_name,
    ast.Attribute: _eval_attribute,
    ast.Subscript: _eval_subscript,
    ast.List: _eval_sequence,
    ast.Tuple: _eval_sequence,
    ast.Compare: _eval_compare,
    ast.BoolOp: _eval_boolop,
    ast.UnaryOp: _eval_unaryop,
    ast.Call: _eval_call,
}


def _eval_node(node: ast.AST, inputs: dict[str, Any]) -> Any:
    """Recursively evaluate an AST node."""
    handler = _EVAL_DISPATCH.get(type(node))
    if handler is not None:
        return handler(node, inputs)
    raise ValueError(f"Unsupported node: {type(node).__name__}")  # pragma: no cover


# ---------------------------------------------------------------------------
# Aggregate function evaluation
# ---------------------------------------------------------------------------


def _find_glob_subscript(node: ast.AST) -> ast.Subscript | None:
    """Find a Subscript on ``inputs`` with a glob pattern in the subtree."""
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Subscript)
            and isinstance(child.value, ast.Name)
            and child.value.id == "inputs"
            and isinstance(child.slice, ast.Constant)
            and isinstance(child.slice.value, str)
            and _is_glob(child.slice.value)
        ):
            return child
    return None


def _eval_aggregate(node: ast.Call, inputs: dict[str, Any]) -> Any:
    """Evaluate an aggregate function call (all, any, almost_all, count)."""
    assert isinstance(node.func, ast.Name)
    func_name = node.func.id
    expr_arg = node.args[0]

    glob_node = _find_glob_subscript(expr_arg)
    if glob_node is None:
        raise ValueError(f'{func_name}() requires a glob pattern like inputs["*"]')

    assert isinstance(glob_node.slice, ast.Constant)
    pattern: str = glob_node.slice.value  # type: ignore[assignment]
    matching_keys = sorted(k for k in inputs if fnmatch.fnmatch(k, pattern))

    # Evaluate the inner expression for each matching input by temporarily
    # swapping the glob subscript's slice to each concrete key.
    results: list[Any] = []
    original_slice = glob_node.slice
    try:
        for key in matching_keys:
            glob_node.slice = ast.Constant(value=key)
            results.append(_eval_node(expr_arg, inputs))
    finally:
        glob_node.slice = original_slice

    if func_name == "all":
        return all(results)
    if func_name == "any":
        return any(results)
    if func_name == "count":
        return sum(1 for r in results if r)
    if func_name == "almost_all":
        if len(node.args) < 2:
            raise ValueError("almost_all() requires a second argument: max_failures")
        max_failures = _eval_node(node.args[1], inputs)
        failures = sum(1 for r in results if not r)
        return failures <= max_failures
    raise ValueError(f"Unknown aggregate: {func_name}")  # pragma: no cover


_COMPARE_DISPATCH: dict[type, Callable[[Any, Any], bool]] = {
    ast.Eq: lambda a, b: a == b,
    ast.NotEq: lambda a, b: a != b,
    ast.Gt: lambda a, b: a > b,
    ast.GtE: lambda a, b: a >= b,
    ast.Lt: lambda a, b: a < b,
    ast.LtE: lambda a, b: a <= b,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}


def _compare(left: Any, op: ast.cmpop, right: Any) -> bool:
    handler = _COMPARE_DISPATCH.get(type(op))
    if handler is not None:
        return handler(left, right)
    raise ValueError(f"Unsupported comparison: {type(op).__name__}")  # pragma: no cover
