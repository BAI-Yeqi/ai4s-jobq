# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Shared CLI infrastructure: group definition, client factory, helpers."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import asyncclick as click

if TYPE_CHECKING:
    from ai4s.jobq.workflow.client import WorkflowClient

LOG = logging.getLogger("ai4s.jobq")

# Status → colour mapping used by all renderers.
_STATUS_STYLE: dict[str, str] = {
    "completed": "green",
    "running": "cyan",
    "failed": "red bold",
    "upstream_failed": "red",
    "pending": "dim",
    "ready": "yellow",
    "skipped": "magenta",
    "cancelled": "yellow",
}


def _styled_status(s: str) -> str:
    """Wrap a status string in its Rich markup style (or leave it untouched)."""
    style = _STATUS_STYLE.get(s, "")
    return f"[{style}]{s}[/{style}]" if style else s


_WORKFLOW_SUBCOMMANDS = {
    "submit",
    "validate",
    "status",
    "watch",
    "logs",
    "list",
    "tasks",
    "cancel",
    "retry",
    "coordinator",
    "break-lease",
    "doctor",
    "explain",
    "summary",
    "purge",
    "track",
}


class WorkflowGroup(click.Group):
    """Workflow group that lets the leading positional ``STORAGE/PREFIX``
    arg be omitted when invoking a subcommand directly.
    """

    def parse_args(self, ctx, args):
        i = 0
        while i < len(args) and args[i].startswith("-"):
            i += 1
        if i < len(args) and args[i] in _WORKFLOW_SUBCOMMANDS:
            args = list(args)
            args.insert(i, "__none__")
        return super().parse_args(ctx, args)


def _parse_storage_prefix(value: str | None) -> tuple[str | None, str | None]:
    """Parse a ``STORAGE/PREFIX`` positional into its two parts."""
    from ai4s.jobq.workflow.env import WorkflowEnvError, parse_workflow_value

    if value is None or value == "__none__":
        return None, None
    try:
        return parse_workflow_value(value)
    except WorkflowEnvError as exc:
        raise click.UsageError(str(exc)) from exc


@click.group("workflow", cls=WorkflowGroup)
@click.argument(
    "storage_prefix",
    metavar="[STORAGE/PREFIX]",
    required=False,
    default=None,
)
@click.pass_context
def workflow_group(
    ctx: click.Context,
    storage_prefix: str | None,
) -> None:
    """Manage DAG-based workflows.

    \b
    Storage and prefix can be provided two ways:
      - Positional:  ai4s-jobq workflow myaccount/MyProject submit ...
      - Env var:     JOBQ_WORKFLOW_PREFIX=myaccount/MyProject

    Positional wins when both are supplied. The prefix is always
    required so workflows from different projects on the same storage
    account stay isolated.
    """
    from ai4s.jobq.workflow.env import (
        WORKFLOW_ENV,
        WorkflowEnv,
        WorkflowEnvError,
        _check_legacy_env,
    )

    pos_storage, pos_prefix = _parse_storage_prefix(storage_prefix)

    try:
        _check_legacy_env()
    except WorkflowEnvError as exc:
        raise click.UsageError(str(exc)) from exc

    effective_storage = pos_storage
    effective_prefix = pos_prefix

    if (not effective_storage or not effective_prefix) and WORKFLOW_ENV in os.environ:
        try:
            env = WorkflowEnv.from_environ(state_account=effective_storage, prefix=effective_prefix)
            effective_storage = env.state_account
            effective_prefix = env.prefix
        except WorkflowEnvError as exc:
            raise click.UsageError(str(exc)) from exc

    ctx.ensure_object(dict)
    ctx.obj["storage"] = effective_storage
    ctx.obj["prefix"] = effective_prefix


def _format_target(ctx: click.Context) -> str:
    """Compact ``account/prefix`` description of the configured target."""
    storage = ctx.obj.get("storage") or "<unset>"
    prefix = ctx.obj.get("prefix") or "<unset>"
    return f"{storage}/{prefix}"


def _table_names(prefix: str) -> tuple[str, str]:
    """Return the workflow + task table names for a given prefix."""
    return f"{prefix}Workflows", f"{prefix}WorkflowTasks"


def _print_target_banner(ctx: click.Context, action: str) -> None:
    """Echo a short ``Target: account/prefix`` line to stderr."""
    click.echo(f"{action} target: {_format_target(ctx)}", err=True)


async def _get_client(ctx: click.Context) -> WorkflowClient:
    """Build a WorkflowClient from the resolved storage/prefix.

    Constructs the new persistence-backed :class:`WorkflowClient`
    via ``WorkflowEnv.from_environ`` so queue-backend / blob-account
    overrides are honoured.
    """
    from ai4s.jobq.workflow.client import WorkflowClient as _WorkflowClient
    from ai4s.jobq.workflow.env import WorkflowEnv, WorkflowEnvError
    from ai4s.jobq.workflow.persistence import WorkflowPersistence

    storage = ctx.obj["storage"]
    prefix = ctx.obj["prefix"]
    if not storage or not prefix:
        raise click.UsageError(
            "Workflow account and prefix not configured. Pass STORAGE/PREFIX "
            "positionally (e.g. `ai4s-jobq workflow myaccount/MyProject ...`) "
            "or set JOBQ_WORKFLOW_PREFIX=<account>/<prefix>."
        )
    try:
        env = WorkflowEnv.from_environ(state_account=storage, prefix=prefix)
    except WorkflowEnvError as exc:
        raise click.UsageError(str(exc)) from exc
    persistence = await WorkflowPersistence.from_account(env.state_account, prefix=env.prefix)
    return _WorkflowClient(persistence, queues_account=env.queues, prefix=env.prefix)


def _status_to_dict(status) -> dict:
    """Convert a WorkflowStatus to a JSON-friendly dict."""
    d = {
        "workflow_id": status.workflow_id,
        "name": status.name,
        "status": status.status,
        "total": status.total,
        "completed": status.completed,
        "running": status.running,
        "failed": status.failed,
        "pending": status.pending,
        "skipped": status.skipped,
        "created_at": status.created_at,
        "updated_at": status.updated_at,
    }
    if status.error:
        d["error"] = status.error
    if hasattr(status, "tasks") and status.tasks:
        d["tasks"] = {name: _task_to_dict(t) for name, t in status.tasks.items()}
    return d


def _task_to_dict(task) -> dict:
    """Convert a TaskStatus to a JSON-friendly dict."""
    return {
        "name": task.name,
        "status": task.status,
        "depends_on": task.depends_on,
        "dep_policy": task.dep_policy,
        "completed_deps": task.completed_deps,
        "failed_deps": task.failed_deps,
        "queue": task.queue,
        "error": task.error,
        "started_at": task.started_at,
        "completed_at": task.completed_at,
        "retries_remaining": task.retries_remaining,
    }


def _build_task_table(tasks, *, verbose: bool = False):
    """Build a Rich Table renderable for a list of tasks."""
    from rich.table import Table

    table = Table(show_lines=False, pad_edge=False)
    table.add_column("Task", style="bold")
    table.add_column("Status")
    table.add_column("Queue")
    table.add_column("Deps", justify="right")
    if verbose:
        table.add_column("Error", style="red", overflow="fold")
    else:
        table.add_column("Error", style="red", max_width=40, no_wrap=True)

    truncated = 0
    for t in tasks:
        total_deps = len(t.depends_on) if hasattr(t, "depends_on") else 0
        deps = f"{t.completed_deps}/{total_deps}" if total_deps else ""
        status_text = _styled_status(t.status)
        full_err = t.error or ""
        if verbose:
            error = full_err
        else:
            if len(full_err) > 40:
                truncated += 1
                error = full_err[:37] + "…"
            else:
                error = full_err
        table.add_row(t.name, status_text, t.queue or "", deps, error)

    return table, truncated


def _print_task_table(tasks, *, verbose: bool = False) -> None:
    """Print a task status table using rich."""
    from rich.console import Console

    console = Console()
    table, truncated = _build_task_table(tasks, verbose=verbose)
    console.print(table)
    if truncated:
        console.print(
            f"[dim]({truncated} error message(s) truncated — "
            "rerun with --verbose for full text or --json for raw output.)[/dim]"
        )
