# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Shared CLI infrastructure: group definition, client factory, helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import asyncclick as click

if TYPE_CHECKING:
    from ai4s.jobq.workflow.client import WorkflowClient

LOG = logging.getLogger("ai4s.jobq")

# Status → colour mapping used by all renderers.
_STATUS_STYLE: dict[str, str] = {
    "completed": "green",
    "running": "cyan",
    "cancelling": "bold yellow",
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


class WorkflowGroup(click.Group):
    """Workflow group that lets the leading positional ``STORAGE/PREFIX``
    arg be omitted when invoking a subcommand directly.

    Because the group also accepts value-taking options (``--queues``,
    ``--blobs``, ``--config``), the detection skips option values before
    deciding whether the first positional token is a registered
    subcommand (in which case ``STORAGE/PREFIX`` was omitted) rather than
    relying on a hand-maintained subcommand list.
    """

    def parse_args(self, ctx, args):
        args = list(args)
        value_opts: set[str] = set()
        for param in self.get_params(ctx):
            if isinstance(param, click.Option) and not param.is_flag and not param.count:
                value_opts.update(param.opts)
                value_opts.update(param.secondary_opts)

        i = 0
        while i < len(args):
            tok = args[i]
            if tok == "--":
                i += 1
                break
            if tok.startswith("-"):
                if "=" in tok or tok not in value_opts:
                    i += 1
                else:
                    i += 2
                continue
            break

        if i < len(args) and args[i] in self.commands:
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
@click.option(
    "--queues",
    default=None,
    help="Queue backend for all workflow queues: a single account name or "
    "'sb://<namespace>' for Service Bus (one backend, not a list of queues). "
    "[env: JOBQ_WORKFLOW_QUEUES]",
)
@click.option(
    "--blobs",
    default=None,
    help="Large-output blob storage override, as '<account>/<container>'. "
    "[env: JOBQ_WORKFLOW_BLOBS]",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(),
    help="Path to a jobq.yaml config file. Defaults to ./jobq.yaml, "
    "./.jobq.yaml, or ~/.config/ai4s-jobq/jobq.yaml. [env: JOBQ_WORKFLOW_CONFIG]",
)
@click.pass_context
def workflow_group(
    ctx: click.Context,
    storage_prefix: str | None,
    queues: str | None,
    blobs: str | None,
    config_path: str | None,
) -> None:
    """Manage DAG-based workflows.

    \b
    Storage and prefix can be provided three ways (highest precedence first):
      - Positional:  ai4s-jobq workflow myaccount/MyProject submit ...
      - Env var:     JOBQ_WORKFLOW_PREFIX=myaccount/MyProject
      - Config file: a shared jobq.yaml (connection: storage/prefix)

    Queues and blobs follow the same precedence via --queues/--blobs,
    JOBQ_WORKFLOW_QUEUES/JOBQ_WORKFLOW_BLOBS, or the config file. Run
    `ai4s-jobq workflow config show` to see the resolved values and where
    each came from.
    """
    from ai4s.jobq.workflow.env import (
        WorkflowEnv,
        WorkflowEnvError,
        _check_legacy_env,
        load_config,
    )

    pos_storage, pos_prefix = _parse_storage_prefix(storage_prefix)

    try:
        _check_legacy_env()
        cfg = load_config(config_path)
    except WorkflowEnvError as exc:
        raise click.UsageError(str(exc)) from exc

    # Best-effort full resolution. A missing target is deferred to the
    # command that actually needs one (e.g. `config init` needs nothing),
    # so we swallow only the "not configured" case here.
    env = None
    try:
        env = WorkflowEnv.from_environ(
            state_account=pos_storage,
            prefix=pos_prefix,
            queues=queues,
            blobs=blobs,
            config=cfg,
        )
    except WorkflowEnvError:
        env = None

    ctx.ensure_object(dict)
    ctx.obj["config"] = cfg
    ctx.obj["config_path"] = config_path
    ctx.obj["queues_raw"] = queues
    ctx.obj["blobs_raw"] = blobs
    ctx.obj["env"] = env
    ctx.obj["storage"] = env.state_account if env else pos_storage
    ctx.obj["prefix"] = env.prefix if env else pos_prefix


def _resolve_env(ctx: click.Context):
    """Return the resolved :class:`WorkflowEnv` or raise a friendly error."""
    from ai4s.jobq.workflow.env import WorkflowEnv, WorkflowEnvError

    env = ctx.obj.get("env")
    if env is not None:
        return env
    try:
        return WorkflowEnv.from_environ(
            state_account=ctx.obj.get("storage"),
            prefix=ctx.obj.get("prefix"),
            queues=ctx.obj.get("queues_raw"),
            blobs=ctx.obj.get("blobs_raw"),
            config=ctx.obj.get("config"),
        )
    except WorkflowEnvError as exc:
        raise click.UsageError(str(exc)) from exc


def _format_target(ctx: click.Context) -> str:
    """Compact ``account/prefix`` description of the configured target."""
    storage = ctx.obj.get("storage") or "<unset>"
    prefix = ctx.obj.get("prefix") or "<unset>"
    return f"{storage}/{prefix}"


def _table_names(prefix: str) -> tuple[str, str]:
    """Return the workflow + task table names for a given prefix."""
    return f"{prefix}Workflows", f"{prefix}WorkflowTasks"


def _config_banner(env, action: str | None = None) -> str:
    """Compact one-line summary of resolved config with per-value sources.

    Values whose source is a non-default layer (flag/env/file) are tagged
    with that source so it's obvious what won when config comes from
    multiple places.
    """
    src = env.sources

    def tag(key: str) -> str:
        s = src.get(key, "")
        return f"({s})" if s and s != "default" else ""

    target = f"{env.state_account}{tag('storage')}/{env.prefix}{tag('prefix')}"
    parts = [
        target,
        f"queues={env.queues}{tag('queues')}",
        f"blobs={env.blob_account}/{env.blob_container}{tag('blobs')}",
    ]
    line = "  ".join(parts)
    if env.config_path:
        line += f"  [config: {env.config_path}]"
    lead = f"{action} " if action else ""
    return f"▶ {lead}{line}"


def _print_config_banner(ctx: click.Context, action: str | None = None) -> None:
    """Echo the compact resolved-config banner to stderr (once per run)."""
    if ctx.obj.get("_banner_shown"):
        return
    env = ctx.obj.get("env")
    if env is None:
        return
    click.echo(_config_banner(env, action), err=True)
    ctx.obj["_banner_shown"] = True


def _print_target_banner(ctx: click.Context, action: str) -> None:
    """Backwards-compatible entry point; renders the compact config banner."""
    _print_config_banner(ctx, action)


async def _get_client(ctx: click.Context) -> WorkflowClient:
    """Build a WorkflowClient from the resolved storage/prefix.

    Constructs the new persistence-backed :class:`WorkflowClient`
    via ``WorkflowEnv.from_environ`` so queue-backend / blob-account
    overrides are honoured.
    """
    from ai4s.jobq.workflow.client import WorkflowClient as _WorkflowClient
    from ai4s.jobq.workflow.persistence import WorkflowPersistence

    env = _resolve_env(ctx)
    _print_config_banner(ctx)
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
