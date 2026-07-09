# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""``ai4s-jobq workflow explain`` — store-side workflow & task diagnostics.

The legacy implementation queried a heavy ``WorkflowDiagnostics`` surface
that fused store state with in-process coordinator actor state.  The
new persistence-backed coordinator has no separate actor state, so the
explanation here is built inline from the store-visible status alone:
per-state task counts, blocking factors (dep waits, failed upstreams,
stale-READY age), and the most pertinent timestamps.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import asyncclick as click

from ai4s.jobq.workflow.cli._shared import _get_client, workflow_group
from ai4s.jobq.workflow.entities import TaskState, WorkflowState

if TYPE_CHECKING:
    from ai4s.jobq.workflow.entities import WorkflowStatus


@dataclass
class _Explanation:
    """Lightweight explanation record (close-enough shape to the legacy diagnostic)."""

    workflow_id: str
    name: str
    status: str
    summary: str
    counts_by_state: dict[str, int] = field(default_factory=dict)
    blockers: list[str] = field(default_factory=list)
    detail: dict[str, str] = field(default_factory=dict)


@workflow_group.command("explain")
@click.argument("workflow_id")
@click.option(
    "--task",
    "task_name",
    default=None,
    help="Explain a specific task within the workflow instead of the workflow itself.",
)
@click.option("--json", "as_json", is_flag=True, help="Output the explanation as JSON.")
@click.pass_context
async def workflow_explain(
    ctx: click.Context,
    workflow_id: str,
    task_name: str | None,
    as_json: bool,
) -> None:
    """Explain why a workflow (or task) is in its current state.

    Renders a store-derived summary with per-state task counts and
    likely blockers.  This is a lightweight diagnostic: it does not
    consult coordinator runtime state (which is fully encoded in the
    durable store in the new design anyway).
    """
    async with await _get_client(ctx) as client:
        try:
            status = await client.status(workflow_id)
        except KeyError:
            explanation = _Explanation(
                workflow_id=workflow_id,
                name=workflow_id,
                status="missing",
                summary=f"Workflow {workflow_id} not found in the store.",
            )
            _emit(explanation, task_name=task_name, as_json=as_json)
            return

    if task_name is not None:
        explanation = _explain_task(status, task_name)
    else:
        explanation = _explain_workflow(status)

    _emit(explanation, task_name=task_name, as_json=as_json)


def _explain_workflow(status: WorkflowStatus) -> _Explanation:
    counts = Counter(str(t.status) for t in status.tasks.values())
    blockers: list[str] = []
    detail: dict[str, str] = {}

    if status.status == WorkflowState.PENDING:
        blockers.append(
            "Workflow is PENDING — coordinator has not yet picked it up. "
            "Verify the coordinator is running and pointed at this prefix."
        )
    if status.status in {WorkflowState.FAILED, WorkflowState.CANCELLED} and status.error:
        detail["workflow_error"] = status.error

    failed_tasks = [t for t in status.tasks.values() if t.status == TaskState.FAILED]
    if failed_tasks:
        names = ", ".join(t.name for t in failed_tasks[:3])
        more = f" (+{len(failed_tasks) - 3} more)" if len(failed_tasks) > 3 else ""
        blockers.append(f"{len(failed_tasks)} failed task(s): {names}{more}")

    upstream_failed = [t for t in status.tasks.values() if t.status == TaskState.UPSTREAM_FAILED]
    if upstream_failed:
        blockers.append(f"{len(upstream_failed)} task(s) blocked by upstream failures")

    ready_tasks = [
        t for t in status.tasks.values() if t.status == TaskState.READY and t.fan_out_at is not None
    ]
    if ready_tasks:
        now = datetime.now(timezone.utc)
        oldest = min(t.fan_out_at for t in ready_tasks if t.fan_out_at is not None)
        age_min = int((now - oldest).total_seconds() / 60)
        if age_min > 10:
            blockers.append(
                f"{len(ready_tasks)} READY task(s) waiting on workers; oldest enqueued {age_min}m ago"
            )

    pending = [t for t in status.tasks.values() if t.status == TaskState.PENDING]
    if pending and not ready_tasks and status.status == WorkflowState.RUNNING:
        # Note dep waits as a generic informational blocker.
        blockers.append(f"{len(pending)} task(s) pending on upstream completions")

    summary = (
        f"{status.completed}/{status.total} tasks complete"
        f"{', ' + str(status.failed) + ' failed' if status.failed else ''}"
        f"{', ' + str(status.running) + ' running' if status.running else ''}"
    )

    return _Explanation(
        workflow_id=status.workflow_id,
        name=status.name,
        status=str(status.status),
        summary=summary,
        counts_by_state=dict(counts),
        blockers=blockers,
        detail=detail,
    )


def _explain_task(status: WorkflowStatus, task_name: str) -> _Explanation:
    task = status.tasks.get(task_name)
    if task is None:
        return _Explanation(
            workflow_id=status.workflow_id,
            name=task_name,
            status="missing",
            summary=f"Task {task_name!r} not found in workflow {status.workflow_id}.",
        )

    blockers: list[str] = []
    detail: dict[str, str] = {}

    total_deps = len(task.depends_on)
    if task.status == TaskState.PENDING and total_deps:
        waiting = total_deps - task.completed_deps - task.failed_deps
        blockers.append(f"waiting on {waiting} of {total_deps} dependency completion(s)")
    if task.status == TaskState.UPSTREAM_FAILED:
        blockers.append(f"upstream failures: {task.failed_deps}/{total_deps}")
    if task.status == TaskState.FAILED and task.error:
        detail["error"] = task.error
    if task.status == TaskState.READY and task.fan_out_at is not None:
        now = datetime.now(timezone.utc)
        age_min = int((now - task.fan_out_at).total_seconds() / 60)
        detail["enqueued_min_ago"] = str(age_min)
        if age_min > 10:
            blockers.append(
                f"READY for {age_min}m without a worker pick-up — verify "
                f"workers are consuming from queue '{task.queue}'"
            )
    if task.attempt_no:
        detail["attempt_no"] = str(task.attempt_no)
    if task.queue:
        detail["queue"] = task.queue

    summary = (
        f"task {task.name} is {task.status}, "
        f"{task.completed_deps}/{total_deps} deps complete"
        f"{', ' + str(task.failed_deps) + ' failed' if task.failed_deps else ''}"
    )

    return _Explanation(
        workflow_id=status.workflow_id,
        name=task.name,
        status=str(task.status),
        summary=summary,
        counts_by_state={str(task.status): 1},
        blockers=blockers,
        detail=detail,
    )


def _emit(explanation: _Explanation, *, task_name: str | None, as_json: bool) -> None:
    import json

    if as_json:
        click.echo(json.dumps(asdict(explanation), indent=2, default=str))
        return

    from rich.console import Console

    console = Console()
    status_style = {
        "completed": "green",
        "running": "cyan",
        "failed": "red bold",
        "pending": "dim",
        "ready": "yellow",
        "skipped": "magenta",
        "cancelled": "yellow",
        "upstream_failed": "red",
        "missing": "red bold",
    }.get(explanation.status.lower(), "")
    status_text = (
        f"[{status_style}]{explanation.status}[/{status_style}]"
        if status_style
        else explanation.status
    )

    if task_name is not None:
        console.print(f"[bold]Task[/bold] {explanation.workflow_id}/{task_name} — {status_text}")
    else:
        console.print(
            f"[bold]Workflow[/bold] {explanation.workflow_id} ({explanation.name}) — {status_text}"
        )

    console.print(f"  {explanation.summary}")

    if explanation.counts_by_state:
        parts = ", ".join(f"{k}={v}" for k, v in explanation.counts_by_state.items() if v)
        if parts:
            console.print(f"  [dim]counts:[/dim] {parts}")

    if explanation.blockers:
        console.print("  [bold]Blockers:[/bold]")
        for b in explanation.blockers:
            console.print(f"    [yellow]•[/yellow] {b}")

    if explanation.detail:
        console.print("  [dim]detail:[/dim]")
        for k, v in explanation.detail.items():
            console.print(f"    [dim]{k}[/dim] = {v}")
