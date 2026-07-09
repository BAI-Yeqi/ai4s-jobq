# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Per-layer rollup for workflow status output.

A "layer" is a heuristic grouping of tasks by name prefix. For generated
workflows (e.g. cse-mindless) tasks follow a ``<role>-<NNNNNNN>`` naming
convention, so stripping the trailing ``-NNNN[_NN]?`` index suffix
recovers a natural per-stage rollup without requiring schema changes or
user-supplied tags.

Examples (all from the cse-mindless stress workload)::

    m-0000001                       -> "m"
    b-0000001                       -> "b"
    sim-1-0000001                   -> "sim-1"
    sim-1-0000001_3                 -> "sim-1"
    __merge_sim-1-0000001_0         -> "__merge_sim-1"
    energy-0000001                  -> "energy"
    custom_task                     -> "custom_task"  (no index suffix)

Tasks whose name does not match the suffix pattern map to the full name,
so single-task or human-named workflows still render sensibly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ai4s.jobq.workflow.entities import TaskState

if TYPE_CHECKING:
    from collections.abc import Iterable

    from rich.table import Table

    from ai4s.jobq.workflow.entities import TaskStatus

# Require at least 3 trailing digits to avoid stripping suffixes from
# task names like ``step-2`` (which the user almost certainly intends as
# its own layer). Generated workflows in this repo use 7-digit indices,
# so the bar is comfortable.
_LAYER_RE = re.compile(r"^(.+?)-\d{3,}(?:_\d+)?$")


def task_layer(task_name: str) -> str:
    """Return the layer label for *task_name*.

    Strips a trailing ``-NNNN`` (optionally followed by ``_NN``) suffix.
    Returns the full name if no such suffix exists.
    """
    m = _LAYER_RE.match(task_name)
    return m.group(1) if m else task_name


@dataclass
class LayerCounts:
    """Per-state task counts within a single layer."""

    total: int = 0
    by_state: dict[str, int] = field(default_factory=dict)

    def add(self, state: str) -> None:
        self.total += 1
        self.by_state[state] = self.by_state.get(state, 0) + 1

    def get(self, state: str) -> int:
        return self.by_state.get(state, 0)

    @property
    def terminal(self) -> int:
        return sum(self.by_state.get(s, 0) for s in TaskState.TERMINAL)

    @property
    def active(self) -> int:
        return sum(self.by_state.get(s, 0) for s in TaskState.ACTIVE)


def summarize_layers(tasks: Iterable[TaskStatus]) -> dict[str, LayerCounts]:
    """Group *tasks* by :func:`task_layer` and tally state counts.

    The returned dict preserves first-seen order of layer labels, which
    in practice matches the natural top-down ordering of the workflow
    (roots appear first because the persistence layer streams tasks in
    insertion order).
    """
    out: dict[str, LayerCounts] = {}
    for t in tasks:
        layer = task_layer(t.name)
        if layer not in out:
            out[layer] = LayerCounts()
        out[layer].add(str(t.status))
    return out


def _progress_bar(
    done: int,
    total: int,
    *,
    failed: int = 0,
    running: int = 0,
    skipped: int = 0,
    width: int = 16,
) -> str:
    """Return a small Rich-markup progress bar with colour-coded segments.

    Segments (left to right): green = completed, red = failed/upstream-failed,
    cyan = running, magenta = skipped, dim = pending.  The percentage shows
    terminal tasks (completed + failed + skipped) out of total.
    """
    if total <= 0:
        return ""

    def _w(n: int) -> int:
        return max(0, round(width * n / total))

    n_done = _w(done)
    n_fail = _w(failed)
    n_run = _w(running)
    n_skip = _w(skipped)
    n_pend = max(0, width - n_done - n_fail - n_run - n_skip)

    bar = ""
    if n_done:
        bar += f"[green]{'█' * n_done}[/]"
    if n_fail:
        bar += f"[red]{'█' * n_fail}[/]"
    if n_run:
        bar += f"[cyan]{'█' * n_run}[/]"
    if n_skip:
        bar += f"[magenta]{'█' * n_skip}[/]"
    if n_pend:
        bar += f"[dim]{'░' * n_pend}[/]"

    pct = 100.0 * (done + failed + skipped) / total
    return f"{bar} {pct:5.1f}%"


def render_layer_table(layers: dict[str, LayerCounts]) -> Table:
    """Build a Rich Table rendering of the per-layer rollup."""
    from rich.table import Table

    table = Table(
        title="Layers",
        show_lines=False,
        pad_edge=False,
        title_style="bold",
    )
    table.add_column("Layer", style="bold")
    table.add_column("Total", justify="right")
    table.add_column("Pending", justify="right", style="dim")
    table.add_column("Ready", justify="right", style="yellow")
    table.add_column("Running", justify="right", style="cyan")
    table.add_column("Completed", justify="right", style="green")
    table.add_column("Failed", justify="right", style="red")
    table.add_column("Skipped", justify="right", style="magenta")
    table.add_column("Progress", justify="left")

    # Combine PENDING + READY_PENDING_BUDGET into a single "pending"
    # column and READY + RUNNING into single user-facing columns so the
    # table stays narrow enough to fit a 100-column terminal.
    for layer, counts in layers.items():
        total = counts.total
        pending = counts.get(TaskState.PENDING) + counts.get(TaskState.READY_PENDING_BUDGET)
        ready = counts.get(TaskState.READY)
        running = counts.get(TaskState.RUNNING)
        completed = counts.get(TaskState.COMPLETED)
        failed = (
            counts.get(TaskState.FAILED)
            + counts.get(TaskState.UPSTREAM_FAILED)
            + counts.get(TaskState.CANCELLED)
        )
        skipped = counts.get(TaskState.SKIPPED)
        table.add_row(
            layer,
            str(total),
            str(pending) if pending else "",
            str(ready) if ready else "",
            str(running) if running else "",
            str(completed) if completed else "",
            str(failed) if failed else "",
            str(skipped) if skipped else "",
            _progress_bar(completed, total, failed=failed, running=running, skipped=skipped),
        )
    return table


__all__ = [
    "LayerCounts",
    "render_layer_table",
    "summarize_layers",
    "task_layer",
]
