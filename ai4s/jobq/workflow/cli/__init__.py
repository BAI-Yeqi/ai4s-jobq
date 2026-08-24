# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""CLI commands for workflow management.

Added to the main CLI as ``ai4s-jobq workflow <subcommand>``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Protocol

import asyncclick as click

from ai4s.jobq.workflow.cli._shared import (
    _STATUS_STYLE,
    _build_task_table,
    _config_banner,
    _format_target,
    _get_client,
    _print_target_banner,
    _print_task_table,
    _resolve_env,
    _status_to_dict,
    _styled_status,
    _table_names,
    _task_to_dict,
    workflow_group,
)
from ai4s.jobq.workflow.entities import TaskState, WorkflowState

__all__ = ["workflow_group"]

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import FrameType

    from rich.text import Text

    from ai4s.jobq.workflow.client import SubmitResult, WorkflowClient
    from ai4s.jobq.workflow.entities import WorkflowStatus

LOG = logging.getLogger("ai4s.jobq")

# Register subcommand modules (side-effect imports).
import ai4s.jobq.workflow.cli._doctor  # noqa: E402
import ai4s.jobq.workflow.cli._explain  # noqa: E402, F401


class _CoordinatorStopper(Protocol):
    def stop(self) -> None: ...


@contextmanager
def _coordinator_signal_handlers(coord: _CoordinatorStopper) -> Iterator[None]:
    """Translate process termination signals into a graceful coordinator stop."""
    loop = asyncio.get_running_loop()
    shutdown_requested = False

    def request_shutdown(received_signal: signal.Signals) -> None:
        nonlocal shutdown_requested
        if shutdown_requested:
            return
        shutdown_requested = True
        click.echo(
            f"\nReceived {received_signal.name}; shutting down coordinator…",
            err=True,
        )
        coord.stop()

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        loop.call_soon_threadsafe(request_shutdown, signal.Signals(signum))

    with ExitStack() as stack:
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                previous_handler = signal.signal(shutdown_signal, handle_signal)
            except (OSError, ValueError):
                continue
            stack.callback(signal.signal, shutdown_signal, previous_handler)
        yield


@workflow_group.command("submit")
@click.argument("definition_files", nargs=-1, type=click.Path())
@click.option(
    "--id", "workflow_id", default=None, help="Custom workflow ID (single-file submit only)."
)
@click.option(
    "--concurrency",
    type=int,
    default=20,
    help="Max concurrent submissions for batch mode.",
)
@click.option(
    "--max-fan-in",
    type=int,
    default=None,
    help=(
        "Max dependencies per task. When set, large fan-ins are "
        "restructured into sequential batches of this size with "
        "dummy merge nodes in between."
    ),
)
@click.pass_context
async def workflow_submit(
    ctx: click.Context,
    definition_files: tuple[str, ...],
    workflow_id: str | None,
    concurrency: int,
    max_fan_in: int | None,
) -> None:
    """Submit workflows from JSON or YAML definition files.

    \b
    Single file:
        ai4s-jobq workflow submit pipeline.yaml
        ai4s-jobq workflow submit pipeline.yaml --id my-run-001

    \b
    Multiple files:
        ai4s-jobq workflow submit wf-*.yaml
        find workflows/ -name '*.yaml' | ai4s-jobq workflow submit

    When no files are given, reads file paths from stdin (one per line).
    Batch mode prints a progress summary to stderr; workflow IDs go to
    stdout (one per line) for piping.
    """
    import sys

    from ai4s.jobq.workflow import WorkflowDefinition

    # Collect file paths: from arguments or stdin
    paths: list[str] = list(definition_files)
    if not paths:
        LOG.info("Reading definition file paths from stdin (one per line).")
        for raw_line in sys.stdin:
            stripped = raw_line.strip()
            if stripped:
                paths.append(stripped)

    if not paths:
        raise click.UsageError("No definition files provided.")

    # Single-file fast path: preserve original simple behaviour
    if len(paths) == 1:
        from rich.console import Console
        from rich.status import Status

        console = Console(stderr=True)
        with Status("[bold cyan]Loading definition…[/]", console=console) as status:
            definition = WorkflowDefinition.from_file(paths[0])
            n_tasks = len(definition.tasks)
            status.update(
                f"[bold cyan]Loaded {n_tasks:,} tasks — applying transforms and validating…[/]"
            )
            if max_fan_in is not None:
                from ai4s.jobq.workflow.transforms import sequentialize_fan_in

                definition = sequentialize_fan_in(definition, max_fan_in=max_fan_in)
            definition.validate()
            status.update(f"[bold cyan]Submitting {len(definition.tasks):,} tasks…[/]")
            async with await _get_client(ctx) as client:
                wf_id = await client.submit(definition, workflow_id=workflow_id)
        roots = [t.name for t in definition.root_tasks]
        roots_preview = ", ".join(roots[:3]) + (
            f", +{len(roots) - 3} more" if len(roots) > 3 else ""
        )
        click.echo(
            f'Submitted "{definition.name}" '
            f"({len(definition.tasks)} task{'s' if len(definition.tasks) != 1 else ''}, "
            f"{len(roots)} root: {roots_preview}) "
            f"to {_format_target(ctx)} → {wf_id}",
            err=True,
        )
        click.echo(wf_id)
        return

    if workflow_id is not None:
        raise click.UsageError("--id cannot be used with multiple files.")

    # Batch mode
    definitions = []
    for p in paths:
        defn = WorkflowDefinition.from_file(p)
        if max_fan_in is not None:
            from ai4s.jobq.workflow.transforms import sequentialize_fan_in

            defn = sequentialize_fan_in(defn, max_fan_in=max_fan_in)
        defn.validate()
        definitions.append(defn)

    _print_target_banner(ctx, "Submit")

    async with await _get_client(ctx) as client:
        total_tasks = 0
        errors = 0
        progress_bar = None
        bar_task = None
        # When both stderr and stdout are TTYs (typical interactive
        # case), printing workflow IDs via plain ``click.echo`` (raw
        # stdout writes) collides with the live progress bar on
        # stderr. Routing them through the progress bar's Rich console
        # makes Rich clear the live region before each line, so the
        # IDs scroll above the bar cleanly. When stdout is piped, we
        # want raw writes to stdout so callers can pipe IDs to xargs.
        both_tty = sys.stdout.isatty() and sys.stderr.isatty()

        def _on_progress(result: SubmitResult) -> None:
            nonlocal total_tasks, errors
            total_tasks += result.task_count
            if result.error:
                errors += 1
            if both_tty and progress_bar is not None:
                progress_bar.console.print(result.workflow_id, highlight=False, soft_wrap=True)
            else:
                click.echo(result.workflow_id)
            if progress_bar is not None:
                progress_bar.update(
                    bar_task,
                    advance=1,
                    info=(
                        f"{total_tasks} tasks" + (f", [red]{errors} err[/red]" if errors else "")
                    ),
                )

        if sys.stderr.isatty():
            from rich.console import Console
            from rich.progress import (
                BarColumn,
                MofNCompleteColumn,
                Progress,
                SpinnerColumn,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
            )

            from ai4s.jobq.logging_utils import get_rich_console

            progress_bar = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                TextColumn("{task.fields[info]}"),
                # Share the active RichHandler's console so log records
                # cooperate with the live region instead of clobbering it.
                console=get_rich_console() or Console(stderr=True),
            )
            with progress_bar:
                bar_task = progress_bar.add_task("Submitting", total=len(definitions), info="")
                results = await client.submit_batch(
                    definitions, concurrency=concurrency, on_progress=_on_progress
                )
        else:
            results = await client.submit_batch(
                definitions, concurrency=concurrency, on_progress=_on_progress
            )

        ok = sum(1 for r in results if not r.error)
        click.echo(
            f"Submitted {ok}/{len(definitions)} workflows ({total_tasks} tasks)",
            err=True,
        )


@workflow_group.command("validate")
@click.argument("definition_files", nargs=-1, required=True, type=click.Path(exists=True))
def workflow_validate(definition_files: tuple[str, ...]) -> None:
    """Validate one or more workflow definition files without submitting.

    Checks DAG structure (cycles, missing dependencies, unreachable
    tasks, dep_policy consistency). Useful in CI or in a pre-submit
    hook. Does not contact the workflow store.

    Exits non-zero if any file is invalid.
    """
    from ai4s.jobq.workflow import WorkflowDefinition

    failures = 0
    for path in definition_files:
        try:
            defn = WorkflowDefinition.from_file(path)
            defn.validate()
        except (ValueError, OSError) as exc:
            click.echo(f"FAIL  {path}: {exc}", err=True)
            failures += 1
        else:
            click.echo(f"OK    {path}  ({len(defn.tasks)} tasks)")

    if failures:
        raise click.exceptions.Exit(1)


@workflow_group.command("status")
@click.argument("workflow_id")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.option(
    "-v", "--verbose", is_flag=True, help="Show full error messages instead of truncating."
)
@click.option(
    "--by-layer/--no-by-layer",
    default=False,
    help=(
        "Show a per-layer rollup of task counts (pending, running, "
        "completed, failed) above the per-task table. A layer is the "
        "task name with its trailing numeric index stripped, so e.g. "
        "tasks ``sim-1-0000001`` and ``sim-1-0000002`` share layer "
        "``sim-1``. Off by default for status."
    ),
)
@click.option(
    "--tasks/--no-tasks",
    "show_tasks",
    default=True,
    help=(
        "Show the per-task table. Pass --no-tasks for large workflows "
        "where the table dominates output; combine with --by-layer for "
        "a compact rollup."
    ),
)
@click.pass_context
async def workflow_status(
    ctx: click.Context,
    workflow_id: str,
    as_json: bool,
    verbose: bool,
    by_layer: bool,
    show_tasks: bool,
) -> None:
    """Show the status of a workflow."""
    async with await _get_client(ctx) as client:
        status = await client.status(workflow_id)

    if as_json:
        click.echo(json.dumps(_status_to_dict(status), indent=2, default=str))
        return

    from rich.console import Console

    Console().print(
        _render_status(status, verbose=verbose, by_layer=by_layer, show_tasks=show_tasks)
    )


def _progress_bar(status: WorkflowStatus, width: int = 50) -> Text:
    """Return a colour-coded progress bar as a Rich Text object.

    Segments (left to right): green = completed, red = failed/upstream-failed,
    cyan = running, magenta = skipped, dim = pending.
    """
    from rich.text import Text

    total = status.total
    if total == 0:
        return Text("░" * width, style="dim white")

    def _w(n: int) -> int:
        return max(0, round(n / total * width))

    n_done = _w(status.completed)
    n_fail = _w(status.failed)
    n_run = _w(status.running)
    n_skip = _w(status.skipped)
    n_pend = max(0, width - n_done - n_fail - n_run - n_skip)

    bar = Text()
    bar.append("█" * n_done, style="green")
    bar.append("█" * n_fail, style="red")
    bar.append("█" * n_run, style="cyan")
    if n_skip:
        bar.append("█" * n_skip, style="magenta")
    bar.append("░" * n_pend, style="dim white")
    pct = (status.completed + status.failed + status.skipped) / total * 100
    bar.append(f" {pct:.0f}%", style="dim")
    return bar


def _render_status(
    status,
    *,
    verbose: bool,
    by_layer: bool = False,
    show_tasks: bool = True,
    cps: float | None = None,
):
    """Build a Rich renderable for a single workflow status.

    Used by both ``workflow status`` (one-shot) and ``workflow watch``
    (live refresh).
    """
    from rich.console import Group
    from rich.panel import Panel
    from rich.text import Text

    from ai4s.jobq.workflow.cli._layers import render_layer_table, summarize_layers

    status_style = _STATUS_STYLE.get(status.status, "")
    header = Text()
    header.append("Name:      ")
    header.append(f"{status.name}\n")
    header.append("Status:    ")
    header.append(f"{status.status}\n", style=status_style)
    if cps is not None:
        header.append("Rate:      ")
        header.append(f"{cps:.1f} completions/s\n", style="bold yellow")

    counters = [
        f"[green]{status.completed}[/] completed",
        f"[cyan]{status.running}[/] running",
        f"[red]{status.failed}[/] failed",
        f"[dim]{status.pending}[/] pending",
    ]
    if status.skipped:
        counters.append(f"[magenta]{status.skipped}[/] skipped")

    remaining = status.total - status.completed
    if cps is not None and cps > 0 and remaining > 0:
        eta_s = remaining / cps
        if eta_s < 60:
            eta_str = f"  eta {eta_s:.0f}s"
        elif eta_s < 3600:
            eta_str = f"  eta {eta_s / 60:.1f}m"
        else:
            eta_str = f"  eta {eta_s / 3600:.1f}h"
    else:
        eta_str = ""
    panel = Panel(
        header,
        title=f"[bold]Workflow {status.workflow_id}[/]",
        subtitle=f"{status.completed}/{status.total} tasks{eta_str}",
        border_style=status_style or "blue",
    )

    parts: list = [
        panel,
        _progress_bar(status),
        Text.from_markup("  ".join(counters)),
        Text(""),
    ]
    if by_layer and status.tasks:
        layers = summarize_layers(status.tasks.values())
        parts.append(render_layer_table(layers))
        parts.append(Text(""))
    if show_tasks and status.tasks:
        table, _truncated = _build_task_table(list(status.tasks.values()), verbose=verbose)
        parts.append(table)
    return Group(*parts)


@workflow_group.command("watch")
@click.argument("workflow_id")
@click.option(
    "--interval",
    type=float,
    default=1.0,
    show_default=True,
    help="Refresh interval in seconds.",
)
@click.option(
    "-v", "--verbose", is_flag=True, help="Show full error messages instead of truncating."
)
@click.option(
    "--by-layer/--no-by-layer",
    default=True,
    help=(
        "Show a per-layer rollup of task counts above the per-task "
        "table. On by default for watch since the rollup is the "
        "primary signal for monitoring large workflows. Pass "
        "--no-by-layer to hide it."
    ),
)
@click.option(
    "--tasks/--no-tasks",
    "show_tasks",
    default=True,
    help=(
        "Show the per-task table. Pass --no-tasks for a compact "
        "rollup-only view — recommended when watching workflows with "
        "more than a few hundred tasks."
    ),
)
@click.pass_context
async def workflow_watch(
    ctx: click.Context,
    workflow_id: str,
    interval: float,
    verbose: bool,
    by_layer: bool,
    show_tasks: bool,
) -> None:
    """Watch a workflow's status with live updates until it terminates.

    Refreshes the status table in place every --interval seconds. Exits
    automatically when the workflow reaches a terminal state
    (completed, failed, cancelled). Press Ctrl-C to detach early.
    """
    import asyncio

    from rich.console import Console
    from rich.live import Live

    terminal = {"completed", "failed", "cancelled"}
    console = Console()

    def _render(s, cps=None):
        return _render_status(s, verbose=verbose, by_layer=by_layer, show_tasks=show_tasks, cps=cps)

    async with await _get_client(ctx) as client:
        # First fetch outside the Live context so connection errors
        # surface plainly instead of garbling the alt-screen.
        status = await client.status(workflow_id)
        import time as _time

        _start_completed = status.completed
        _start_time = _time.monotonic()
        _cps: float | None = None

        with Live(
            _render(status),
            console=console,
            refresh_per_second=max(1.0, 1.0 / max(interval, 0.05)),
            transient=False,
        ) as live:
            while True:
                if status.status in terminal:
                    break
                try:
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                try:
                    status = await client.status(workflow_id)
                except Exception as exc:
                    live.update(_render(status, _cps), refresh=True)
                    console.print(f"[red]Error refreshing: {exc}[/red]")
                    continue
                now = _time.monotonic()
                elapsed = now - _start_time
                if elapsed > 0:
                    _cps = (status.completed - _start_completed) / elapsed
                live.update(_render(status, _cps), refresh=True)

    console.print(
        f"[dim]Workflow {workflow_id} reached terminal state: "
        f"{_styled_status(status.status)}.[/dim]"
    )


@workflow_group.command("logs")
@click.argument("workflow_id", required=True)
@click.argument("task_name", required=False)
@click.option(
    "--since",
    default="1h",
    show_default=True,
    help="Look-back window for the KQL query (e.g. 30m, 1h, 7d).",
)
@click.option(
    "--table",
    default="traces",
    show_default=True,
    help="Application Insights / Log Analytics table to query.",
)
def workflow_logs(
    workflow_id: str,
    task_name: str | None,
    since: str,
    table: str,
) -> None:
    """Print a Log Analytics KQL query for a workflow's stdout/stderr.

    Workers that have ``APPLICATIONINSIGHTS_CONNECTION_STRING`` set forward
    each task's stdout/stderr to Application Insights, tagged with
    ``customDimensions.workflow_id`` and ``customDimensions.task_name``.
    This command prints a ready-to-paste KQL query for the matching log
    lines—run it in the Logs blade of your App Insights resource (or
    against the linked Log Analytics workspace).

    With no ``TASK_NAME``, returns lines for every task in the workflow;
    pass a task name to scope down further.
    """
    filters = [f'customDimensions.workflow_id == "{workflow_id}"']
    if task_name:
        filters.append(f'customDimensions.task_name == "{task_name}"')
    where_clause = "\n| where " + "\n| where ".join(filters)

    query = f"""{table}
| where timestamp > ago({since}){where_clause}
| project
    timestamp,
    severityLevel,
    workflow_id   = tostring(customDimensions.workflow_id),
    task_name     = tostring(customDimensions.task_name),
    message       = message
| order by timestamp asc
"""

    click.echo(
        "# Paste the following KQL query into the Logs blade of your App Insights resource",
        err=True,
    )
    click.echo(
        "# (or the linked Log Analytics workspace). "
        "Workers must have APPLICATIONINSIGHTS_CONNECTION_STRING set for logs to appear.",
        err=True,
    )
    click.echo(query)


@workflow_group.command("list")
@click.option(
    "--status",
    "filter_status",
    type=click.Choice([s.value for s in WorkflowState], case_sensitive=False),
    default=None,
    help="Filter by status.",
)
@click.option(
    "--recent",
    type=int,
    default=None,
    help=(
        "Show the N most recently-terminated workflows. Uses the cheap "
        "R- recency index instead of a full T- range scan, so it stays "
        "fast even with millions of archived workflows. May be combined "
        "with --status to restrict to a single terminal bucket."
    ),
)
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
async def workflow_list(
    ctx: click.Context,
    filter_status: str | None,
    recent: int | None,
    as_json: bool,
) -> None:
    """List workflows."""
    async with await _get_client(ctx) as client:
        if recent is not None:
            if recent <= 0:
                raise click.BadParameter("--recent must be a positive integer")
            workflows = await client.list_recent_terminal(
                limit=recent,
                status=filter_status,
            )
        else:
            workflows = await client.list_workflows(status=filter_status)

    if as_json:
        click.echo(
            json.dumps(
                [_status_to_dict(w) for w in workflows],
                indent=2,
                default=str,
            )
        )
        return

    if not workflows:
        click.echo("No workflows found.")
        return

    from rich.console import Console
    from rich.table import Table

    table = Table(title="Workflows", show_lines=False, pad_edge=False)
    table.add_column("ID", style="bold")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Progress", justify="right")

    for wf in workflows:
        skipped = f", {wf.skipped} skip" if wf.skipped else ""
        progress = f"{wf.completed}/{wf.total} done, {wf.failed} fail{skipped}"
        table.add_row(
            wf.workflow_id,
            wf.name,
            _styled_status(wf.status),
            progress,
        )

    Console().print(table)


@workflow_group.command("tasks")
@click.argument("workflow_ids", metavar="[WORKFLOW_ID ...]", nargs=-1)
@click.option(
    "--status",
    "filter_status",
    type=click.Choice([s.value for s in TaskState], case_sensitive=False),
    default=None,
    help="Filter by task status.",
)
@click.option("--queue", default=None, help="Filter by target queue.")
@click.option(
    "--prefix",
    "name_prefix",
    default=None,
    help="Filter tasks by name prefix (e.g. 'train/' shows train/gnn, train/rf).",
)
@click.option("--limit", type=int, default=100, help="Max tasks to return (default: 100, 0=all).")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.option(
    "-v", "--verbose", is_flag=True, help="Show full error messages instead of truncating."
)
@click.pass_context
async def workflow_tasks(
    ctx: click.Context,
    workflow_ids: tuple[str, ...],
    filter_status: str | None,
    queue: str | None,
    name_prefix: str | None,
    limit: int,
    as_json: bool,
    verbose: bool,
) -> None:
    """List tasks, optionally filtered by workflow, status, queue, or name prefix.

    Pass one or more WORKFLOW_IDs to scope to those workflows (for
    example, ``tasks wf-a wf-b``). Without any, this scans every workflow
    in the store, which can be slow on large deployments — prefer scoping
    to specific workflows whenever you can.
    """
    max_tasks = limit if limit > 0 else None

    async with await _get_client(ctx) as client:
        if len(workflow_ids) == 1:
            await _list_tasks_single(
                client,
                workflow_ids[0],
                filter_status,
                queue,
                name_prefix,
                max_tasks,
                as_json,
                verbose,
            )
        elif workflow_ids:
            await _list_tasks_multi(
                client,
                list(workflow_ids),
                filter_status,
                queue,
                name_prefix,
                max_tasks,
                as_json,
                verbose,
            )
        else:
            printed = await _list_tasks_global(
                client, filter_status, queue, name_prefix, max_tasks, as_json, verbose
            )
            if max_tasks and printed == max_tasks:
                click.echo(f"\n(showing first {max_tasks} — use --limit 0 for all)")


async def _list_tasks_single(
    client: WorkflowClient,
    workflow_id: str,
    filter_status: str | None,
    queue: str | None,
    name_prefix: str | None,
    max_tasks: int | None,
    as_json: bool,
    verbose: bool,
) -> None:
    """List tasks for a single workflow."""
    tasks = await client.list_tasks(
        workflow_id, status=filter_status, queue=queue, name_prefix=name_prefix
    )
    if max_tasks:
        tasks = tasks[:max_tasks]
    if as_json:
        click.echo(json.dumps([_task_to_dict(t) for t in tasks], indent=2, default=str))
    elif not tasks:
        click.echo("No tasks found.")
    else:
        _print_task_table(tasks, verbose=verbose)


async def _list_tasks_multi(
    client: WorkflowClient,
    workflow_ids: list[str],
    filter_status: str | None,
    queue: str | None,
    name_prefix: str | None,
    max_tasks: int | None,
    as_json: bool,
    verbose: bool,
) -> None:
    """List tasks for an explicit set of workflows.

    JSON output is a single array with each entry tagged by
    ``workflow_id``; text output prints one table per workflow.
    """
    collected: list = []
    printed = 0

    for wf_id in workflow_ids:
        if max_tasks and printed >= max_tasks:
            break
        tasks = await client.list_tasks(
            wf_id, status=filter_status, queue=queue, name_prefix=name_prefix
        )
        if as_json:
            for t in tasks:
                entry = _task_to_dict(t)
                entry["workflow_id"] = wf_id
                collected.append(entry)
        else:
            batch = tasks if not max_tasks else tasks[: max_tasks - printed]
            if batch:
                click.echo(f"\n{wf_id}:")
                _print_task_table(batch, verbose=verbose)
                printed += len(batch)

    if as_json:
        if max_tasks:
            collected = collected[:max_tasks]
        click.echo(json.dumps(collected, indent=2, default=str))
    elif printed == 0:
        click.echo("No tasks found.")


async def _list_tasks_global(
    client: WorkflowClient,
    filter_status: str | None,
    queue: str | None,
    name_prefix: str | None,
    max_tasks: int | None,
    as_json: bool,
    verbose: bool,
) -> int:
    """List tasks across all workflows. Returns count of printed tasks."""
    workflows = await client.list_workflows()
    printed = 0
    collected: list = []

    for wf in workflows:
        if max_tasks and printed >= max_tasks:
            break
        wf_tasks = await client.list_tasks(
            wf.workflow_id, status=filter_status, queue=queue, name_prefix=name_prefix
        )

        if as_json:
            for t in wf_tasks:
                entry = _task_to_dict(t)
                entry["workflow_id"] = wf.workflow_id
                collected.append(entry)
        else:
            batch = wf_tasks if not max_tasks else wf_tasks[: max_tasks - printed]
            if batch:
                _print_task_table(batch, verbose=verbose)
                printed += len(batch)

    if as_json:
        if max_tasks:
            collected = collected[:max_tasks]
        click.echo(json.dumps(collected, indent=2, default=str))
        printed = len(collected)
    elif printed == 0:
        click.echo("No tasks found.")

    return printed


@workflow_group.command("cancel")
@click.argument("workflow_id")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
async def workflow_cancel(ctx: click.Context, workflow_id: str, as_json: bool) -> None:
    """Cancel a running workflow."""
    async with await _get_client(ctx) as client:
        await client.cancel(workflow_id)
    if as_json:
        click.echo(
            json.dumps(
                {
                    "workflow_id": workflow_id,
                    "cancel_requested": True,
                    "target": _format_target(ctx),
                }
            )
        )
        return
    click.echo(f"Cancellation requested for workflow {workflow_id} in {_format_target(ctx)}.")


@workflow_group.command("retry")
@click.argument("workflow_id")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
async def workflow_retry(ctx: click.Context, workflow_id: str, as_json: bool) -> None:
    """Re-run failed, upstream-failed, and cancelled tasks in a workflow.

    Resets each affected task to ``ready`` (or ``pending`` if upstream
    deps are not yet satisfied), clears error fields, and re-dispatches
    any now-ready tasks to their queues with a fresh ``attempt_no``.
    ``running``, ``ready``, ``completed``, and ``skipped`` tasks are
    left alone.
    """
    async with await _get_client(ctx) as client:
        result = await client.retry(workflow_id)

    if as_json:
        click.echo(json.dumps(result, indent=2))
        return

    reset = result.get("reset", 0)
    ready = result.get("now_ready", 0)
    pending = result.get("still_pending", 0)

    if reset == 0:
        click.echo(f"Workflow {workflow_id} in {_format_target(ctx)}: no tasks to retry.")
        return

    next_parts = []
    if ready:
        next_parts.append(f"{ready} now ready")
    if pending:
        next_parts.append(f"{pending} still pending")
    next_summary = ", ".join(next_parts) if next_parts else "no state change"

    click.echo(
        f"Workflow {workflow_id} in {_format_target(ctx)}: reset {reset} task(s) → {next_summary}."
    )


@workflow_group.command("coordinator")
@click.option(
    "--batch-size",
    type=int,
    default=None,
    envvar="JOBQ_COORDINATOR_BATCH_SIZE",
    help=(
        "Max completions to pull from the completion queue per batch. "
        "Higher values increase throughput on busy workflows but raise "
        "per-iteration latency. Default: 32. [env: JOBQ_COORDINATOR_BATCH_SIZE, "
        "config: coordinator.batch_size]"
    ),
)
@click.option(
    "--visibility-timeout-s",
    type=float,
    default=None,
    envvar="JOBQ_COORDINATOR_VISIBILITY_TIMEOUT_S",
    help=(
        "Visibility timeout in seconds for completion messages. "
        "If the coordinator crashes mid-batch the messages reappear "
        "after this interval. Default: 60. "
        "[env: JOBQ_COORDINATOR_VISIBILITY_TIMEOUT_S, config: coordinator.visibility_timeout_s]"
    ),
)
@click.option(
    "--idle-sleep-s",
    type=float,
    default=None,
    envvar="JOBQ_COORDINATOR_IDLE_SLEEP_S",
    help=(
        "Sleep interval in seconds between empty completion-queue polls. "
        "Default: 0.5. [env: JOBQ_COORDINATOR_IDLE_SLEEP_S]"
    ),
)
@click.option(
    "--cancel-poll-interval-s",
    type=float,
    default=None,
    envvar="JOBQ_COORDINATOR_CANCEL_POLL_INTERVAL_S",
    help=(
        "Interval in seconds between scans of the index for "
        "cancel-requested workflows. Default: 1.0. "
        "[env: JOBQ_COORDINATOR_CANCEL_POLL_INTERVAL_S]"
    ),
)
@click.option(
    "--flush-retry-limit",
    type=int,
    default=None,
    envvar="JOBQ_COORDINATOR_FLUSH_RETRY_LIMIT",
    help=(
        "Max retries on ETag conflict when flushing workflow state. "
        "Default: 2. [env: JOBQ_COORDINATOR_FLUSH_RETRY_LIMIT]"
    ),
)
@click.option(
    "--ready-sweep-interval-s",
    type=float,
    default=None,
    envvar="JOBQ_COORDINATOR_READY_SWEEP_INTERVAL_S",
    help=(
        "Interval in seconds between ready-repair sweeps. The sweep "
        "re-dispatches READY tasks in workflows that haven't been "
        "updated for at least --ready-repair-threshold-s seconds — "
        "this closes the submit/retry crash window where a task was "
        "marked READY but its queue message was never pushed. "
        "Default: 60. [env: JOBQ_COORDINATOR_READY_SWEEP_INTERVAL_S]"
    ),
)
@click.option(
    "--ready-repair-threshold-s",
    type=float,
    default=None,
    envvar="JOBQ_COORDINATOR_READY_REPAIR_THRESHOLD_S",
    help=(
        "Minimum age in seconds since a workflow's last update before "
        "the ready-repair sweep considers re-dispatching its READY "
        "tasks. Lower values shorten stuck-workflow recovery latency "
        "but increase duplicate execution risk under queue backlog. "
        "Default: 300. [env: JOBQ_COORDINATOR_READY_REPAIR_THRESHOLD_S]"
    ),
)
@click.option(
    "--running-timeout-s",
    type=float,
    default=None,
    envvar="JOBQ_COORDINATOR_RUNNING_TIMEOUT_S",
    help=(
        "Default wall-clock timeout in seconds for RUNNING tasks that "
        "do not set a per-task ``timeout_s``. Tasks stuck in RUNNING "
        "beyond this threshold are failed by the coordinator's "
        "running-timeout sweep, triggering the normal retry/fail "
        "cascade. Unset by default (tasks with no per-task timeout run "
        "indefinitely). [env: JOBQ_COORDINATOR_RUNNING_TIMEOUT_S]"
    ),
)
@click.option(
    "--running-sweep-interval-s",
    type=float,
    default=None,
    envvar="JOBQ_COORDINATOR_RUNNING_SWEEP_INTERVAL_S",
    help=(
        "Interval in seconds between running-timeout sweeps. "
        "Default: 60. [env: JOBQ_COORDINATOR_RUNNING_SWEEP_INTERVAL_S]"
    ),
)
@click.option(
    "--break-lease",
    "break_lease_flag",
    is_flag=True,
    default=False,
    help=(
        "Break any existing coordinator lease before starting. "
        "Use when the previous coordinator crashed without releasing its lease "
        "and you need to start immediately rather than waiting for the "
        "60-second expiry."
    ),
)
@click.pass_context
async def workflow_coordinator(
    ctx: click.Context,
    batch_size: int | None,
    visibility_timeout_s: float | None,
    idle_sleep_s: float | None,
    cancel_poll_interval_s: float | None,
    flush_retry_limit: int | None,
    ready_sweep_interval_s: float | None,
    ready_repair_threshold_s: float | None,
    running_timeout_s: float | None,
    running_sweep_interval_s: float | None,
    break_lease_flag: bool,
) -> None:
    """Run the workflow coordinator process.

    The coordinator owns the workflow event loop: it consumes
    completion messages from the completion queue, advances each
    affected workflow's DAG, dispatches now-ready child tasks, and
    flushes durable state.

    .. warning::
       Only one coordinator may run per workflow environment.
       Running two coordinators against the same persistence prefix
       will corrupt state because both writers will race on the same
       state blob.
    """
    import logging as _stdlib_logging

    from ai4s.jobq.logging_utils import setup_logging
    from ai4s.jobq.workflow.coordinator import (
        DEFAULT_BATCH_SIZE,
        DEFAULT_CANCEL_POLL_INTERVAL_S,
        DEFAULT_FLUSH_RETRY_LIMIT,
        DEFAULT_IDLE_SLEEP_S,
        DEFAULT_READY_REPAIR_THRESHOLD_S,
        DEFAULT_READY_SWEEP_INTERVAL_S,
        DEFAULT_RUNNING_SWEEP_INTERVAL_S,
        DEFAULT_VISIBILITY_TIMEOUT_S,
        Coordinator,
    )
    from ai4s.jobq.workflow.env import WorkflowEnvError

    if not _stdlib_logging.getLogger().handlers:
        setup_logging(
            "workflow-coordinator",
            internal_log_level=_stdlib_logging.INFO,
            base_log_level=_stdlib_logging.WARNING,
        )

    storage = ctx.obj.get("storage")
    prefix = ctx.obj.get("prefix")

    try:
        env = _resolve_env(ctx)
    except WorkflowEnvError as exc:
        raise click.UsageError(str(exc)) from exc
    storage, prefix = env.state_account, env.prefix

    if env.queues.startswith("sb://"):
        raise click.UsageError(
            "Coordinator requires a Storage Queue backend; got Service Bus "
            f"({env.queues}). Unset JOBQ_WORKFLOW_QUEUES or point it at an "
            "Azure Storage account."
        )

    # Resolve tuning knobs with precedence flag/env → config file → default.
    # Click already merged flag + JOBQ_COORDINATOR_* env (None when neither
    # was set), so the config-file `coordinator` section fills the gap.
    coord_cfg = env.coordinator

    def pick(cli_val, key, default):
        if cli_val is not None:
            return cli_val
        if key in coord_cfg:
            return coord_cfg[key]
        return default

    effective_batch_size = pick(batch_size, "batch_size", DEFAULT_BATCH_SIZE)
    effective_visibility_timeout = pick(
        visibility_timeout_s, "visibility_timeout_s", DEFAULT_VISIBILITY_TIMEOUT_S
    )
    effective_idle_sleep = pick(idle_sleep_s, "idle_sleep_s", DEFAULT_IDLE_SLEEP_S)
    effective_cancel_poll = pick(
        cancel_poll_interval_s, "cancel_poll_interval_s", DEFAULT_CANCEL_POLL_INTERVAL_S
    )
    effective_flush_retry = pick(flush_retry_limit, "flush_retry_limit", DEFAULT_FLUSH_RETRY_LIMIT)
    effective_ready_sweep = pick(
        ready_sweep_interval_s, "ready_sweep_interval_s", DEFAULT_READY_SWEEP_INTERVAL_S
    )
    effective_ready_threshold = pick(
        ready_repair_threshold_s, "ready_repair_threshold_s", DEFAULT_READY_REPAIR_THRESHOLD_S
    )
    effective_running_sweep = pick(
        running_sweep_interval_s, "running_sweep_interval_s", DEFAULT_RUNNING_SWEEP_INTERVAL_S
    )
    effective_running_timeout = pick(running_timeout_s, "running_timeout_s", None)

    click.echo(_config_banner(env, "Coordinator"), err=True)
    click.echo("Coordinator target:", err=True)
    click.echo(f"  State account:   {env.state_account} (prefix={env.prefix})", err=True)
    click.echo(f"  Queue backend:   {env.queues} (Storage Queue)", err=True)
    click.echo(f"  Batch size:      {effective_batch_size}", err=True)
    click.echo(f"  Visibility:      {effective_visibility_timeout}s", err=True)
    click.echo(f"  Idle sleep:      {effective_idle_sleep}s", err=True)
    click.echo(f"  Cancel poll:     {effective_cancel_poll}s", err=True)
    click.echo(f"  Flush retry:     {effective_flush_retry}", err=True)
    click.echo(
        f"  Ready repair:    every {effective_ready_sweep}s (threshold {effective_ready_threshold}s)",
        err=True,
    )
    running_timeout_str = (
        f"{effective_running_timeout}s" if effective_running_timeout is not None else "disabled"
    )
    click.echo(
        f"  Running timeout: {running_timeout_str} (sweep every {effective_running_sweep}s)",
        err=True,
    )
    click.echo("Starting workflow coordinator…", err=True)

    if break_lease_flag:
        from ai4s.jobq.workflow._lease import break_lease
        from ai4s.jobq.workflow.persistence import WorkflowPersistence

        persistence = await WorkflowPersistence.from_account(storage, prefix=prefix)
        try:
            await break_lease(persistence._state_container, prefix=prefix)
            click.echo("  Existing lease broken.", err=True)
        finally:
            await persistence.close()

    async with await Coordinator.from_environment(
        state_account=storage,
        prefix=prefix,
        config=ctx.obj.get("config"),
        batch_size=effective_batch_size,
        visibility_timeout_s=effective_visibility_timeout,
        idle_sleep_s=effective_idle_sleep,
        cancel_poll_interval_s=effective_cancel_poll,
        flush_retry_limit=effective_flush_retry,
        ready_sweep_interval_s=effective_ready_sweep,
        ready_repair_threshold_s=effective_ready_threshold,
        running_timeout_s=effective_running_timeout,
        running_sweep_interval_s=effective_running_sweep,
    ) as coord:
        with _coordinator_signal_handlers(coord):
            try:
                await coord.run()
            except (KeyboardInterrupt, asyncio.CancelledError):
                click.echo("\nShutting down coordinator…", err=True)
                coord.stop()
        stats = coord.stats
        click.echo(
            f"Coordinator stopped — "
            f"{stats.completions_handled:,} completions, "
            f"{stats.tasks_pushed:,} tasks pushed, "
            f"{stats.batches:,} batches",
            err=True,
        )


@workflow_group.command("break-lease")
@click.option("--yes", "-y", "confirmed", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
async def workflow_break_lease(ctx: click.Context, confirmed: bool) -> None:
    """Forcibly break the coordinator lease without starting the coordinator.

    Use when a coordinator crashed without releasing its lease and you
    need to clear it before the 60-second expiry.  Safe to run when no
    lease is held (no-op).

    Alternatively, use ``workflow coordinator --break-lease`` to break
    and immediately start a new coordinator in one step.
    """
    from ai4s.jobq.workflow._lease import break_lease
    from ai4s.jobq.workflow.persistence import WorkflowPersistence

    storage = ctx.obj.get("storage")
    prefix = ctx.obj.get("prefix")
    if not storage or not prefix:
        raise click.UsageError(
            "Storage account and prefix are required. Set JOBQ_WORKFLOW_PREFIX or pass STORAGE/PREFIX."
        )

    if not confirmed:
        click.confirm(
            f"Break coordinator lease for {storage}/{prefix}? "
            "This will allow a new coordinator to start immediately.",
            abort=True,
        )

    persistence = await WorkflowPersistence.from_account(storage, prefix=prefix)
    try:
        await break_lease(persistence._state_container, prefix=prefix)
    finally:
        await persistence.close()
    click.echo(f"Lease broken for {storage}/{prefix}.")


@workflow_group.command("summary")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
async def workflow_summary(ctx: click.Context, as_json: bool) -> None:
    """Show aggregate status across all workflows."""
    async with await _get_client(ctx) as client:
        agg = await client.summary()

    if as_json:
        click.echo(
            json.dumps(
                {
                    "workflows": dict(agg.workflows),
                    "total_tasks": agg.total_tasks,
                    "completed_tasks": agg.completed_tasks,
                    "running_tasks": agg.running_tasks,
                    "failed_tasks": agg.failed_tasks,
                    "pending_tasks": agg.pending_tasks,
                    "skipped_tasks": agg.skipped_tasks,
                },
                indent=2,
            )
        )
        return

    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    console = Console()

    # Workflow counts
    wf_table = Table(show_header=False, box=None, pad_edge=False)
    wf_table.add_column("Status", style="dim")
    wf_table.add_column("Count", justify="right")
    for wf_status, count in sorted(agg.workflows.items()):
        wf_table.add_row(
            _styled_status(wf_status),
            str(count),
        )
    wf_table.add_row("[bold]total[/]", f"[bold]{agg.total_workflows}[/]")

    # Task summary
    task_parts = [
        f"[green]{agg.completed_tasks:,}[/] completed",
        f"[cyan]{agg.running_tasks:,}[/] running",
        f"[red]{agg.failed_tasks:,}[/] failed",
        f"[dim]{agg.pending_tasks:,}[/] pending",
    ]
    if agg.skipped_tasks:
        task_parts.append(f"[magenta]{agg.skipped_tasks:,}[/] skipped")

    pct = agg.completed_tasks / agg.total_tasks if agg.total_tasks else 0
    subtitle = f"{pct:.0%} complete — {agg.total_tasks:,} total tasks"

    console.print(Panel(wf_table, title="[bold]Workflows[/]", border_style="blue"))
    console.print(
        Panel(
            "  ".join(task_parts),
            title="[bold]Tasks[/]",
            subtitle=subtitle,
            border_style="blue",
        )
    )


def _confirm_purge(
    *,
    storage: str,
    wf_table: str,
    task_table: str,
    purge_all: bool,
    drain_queues: bool,
    queues_account: str | None,
    discovered_queues: list[str],
) -> None:
    """Print the purge plan and prompt for confirmation. Aborts on 'no'."""
    click.echo("Purge target:", err=True)
    click.echo(f"  Storage account: {storage}", err=True)
    click.echo(f"  Tables:          {wf_table}, {task_table}", err=True)
    if drain_queues:
        click.echo(f"  Queue backend:   {queues_account}", err=True)
        if discovered_queues:
            preview = ", ".join(discovered_queues[:6])
            if len(discovered_queues) > 6:
                preview += f", … (+{len(discovered_queues) - 6} more)"
            click.echo(
                f"  Queues:          {preview}  ({len(discovered_queues)} total)",
                err=True,
            )
        else:
            click.echo("  Queues:          (none discovered)", err=True)
    click.echo("", err=True)
    scope = "ALL (including running)" if purge_all else "terminal (completed/failed/cancelled)"
    action = f"delete {scope} workflow data from those tables"
    if drain_queues:
        action += " AND drain those queues"
    click.confirm(f"This will permanently {action}. Continue?", abort=True)


@workflow_group.command("purge")
@click.option(
    "--drop-tables",
    is_flag=True,
    help="Also drop tables (recreated on next submit).",
)
@click.option(
    "--all",
    "purge_all",
    is_flag=True,
    help=(
        "Delete all workflows, including those still in a running state. "
        "Without this flag only terminal (completed/failed/cancelled) "
        "workflows are removed.  Use with --drain-queues to do a full "
        "reset between test runs."
    ),
)
@click.option(
    "--drain-queues",
    "drain_queues",
    is_flag=True,
    help=(
        "Also drain the task queues and the coordinator's completion "
        "queue.  Useful when in-flight messages survive a table purge "
        "and would otherwise drive a restarted coordinator into a "
        "stale state."
    ),
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.pass_context
async def workflow_purge(
    ctx: click.Context,
    drop_tables: bool,
    purge_all: bool,
    drain_queues: bool,
    yes: bool,
) -> None:
    """Delete workflow data from Table Storage.

    By default only terminal (completed, failed, cancelled) workflows are
    removed.  Pass --all to also remove workflows that are still running —
    useful for resetting between test runs.
    """
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    storage = ctx.obj.get("storage") or "<unset>"
    prefix = ctx.obj.get("prefix") or "<unset>"
    wf_table, task_table = _table_names(prefix)

    # Resolve queue backend up-front so we can show it in the prompt
    # *and* fail fast if --drain-queues was requested without a
    # configured queue backend.
    queues_account: str | None = None
    discovered_queues: list[str] = []
    if drain_queues:
        env = _resolve_env(ctx)
        queues_account = env.queues
        # Discover queue names so the user sees exactly what's about
        # to be drained before they confirm.
        async with await _get_client(ctx) as _client:
            discovered_queues = await _client.discover_queues()

    if not yes:
        _confirm_purge(
            storage=storage,
            wf_table=wf_table,
            task_table=task_table,
            purge_all=purge_all,
            drain_queues=drain_queues,
            queues_account=queues_account,
            discovered_queues=discovered_queues,
        )

    async with await _get_client(ctx) as client:
        from ai4s.jobq.logging_utils import get_rich_console

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=get_rich_console(),
        ) as progress:
            task = progress.add_task("Purging...", total=None)

            def _on_progress(label: str, n: int) -> None:
                progress.update(task, advance=n, description=f"Deleting {label}...")

            counts = await client.purge(
                drop_tables=drop_tables,
                terminal_only=not purge_all,
                on_progress=_on_progress,
            )

            queue_counts: dict[str, int] = {}
            if drain_queues:
                assert queues_account is not None  # narrowed above
                drain_task = progress.add_task(
                    "Draining queues...",
                    total=len(discovered_queues) or None,
                )

                def _on_queue(queue_name: str, drained: int) -> None:
                    progress.update(
                        drain_task,
                        advance=1,
                        description=f"Drained {queue_name}",
                    )

                queue_counts = await client.drain_queues(
                    queues_account=queues_account,
                    queue_names=discovered_queues,
                    on_progress=_on_queue,
                )

    click.echo(f"Deleted {counts['workflows']} workflow rows and {counts['tasks']} task rows.")
    if drop_tables:
        click.echo("Tables dropped (will be recreated on next submit).")
    if drain_queues:
        ok = sum(1 for n in queue_counts.values() if n >= 0)
        failed = sum(1 for n in queue_counts.values() if n < 0)
        total_msgs = sum(n for n in queue_counts.values() if n > 0)
        msg = f"Drained {ok} queue(s) ({total_msgs} approx message(s))."
        if failed:
            msg += f" {failed} queue(s) failed — see logs."
        click.echo(msg)


@workflow_group.command("track")
@click.option("--port", "-p", "port", default=8050, type=int, help="Port to run the dashboard on.")
@click.option(
    "--workflow-file",
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    envvar="JOBQ_WORKFLOW_FILE",
    default=None,
    help="Visualize a local workflow JSON/YAML file without submitting it.",
)
@click.option("--no-open", is_flag=True, help="Don't auto-open a browser window.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
async def workflow_track(
    ctx: click.Context,
    debug: bool,
    port: int,
    workflow_file: str | None,
    no_open: bool,
) -> None:
    """Launch the workflow monitoring dashboard.

    Opens a browser-based Dash app showing workflow health, status,
    task details, and diagnostics — backed by direct Table Storage
    queries (no Log Analytics required).

    \b
    Examples:
        ai4s-jobq workflow myaccount/MyProject track
        JOBQ_WORKFLOW_PREFIX=myaccount/MyProject ai4s-jobq workflow track
        ai4s-jobq workflow track --workflow-file workflow.json
    """
    import os

    try:
        import dash  # noqa: F401
    except ImportError:
        click.echo(
            "The track extension requires extra dependencies.\n"
            "Install them with:  pip install ai4s-jobq[track]",
            err=True,
        )
        raise SystemExit(1)  # noqa: B904

    storage = ctx.obj.get("storage", "")
    prefix = ctx.obj.get("prefix", "")
    if workflow_file:
        os.environ["JOBQ_WORKFLOW_FILE"] = workflow_file
        os.environ.pop("JOBQ_WORKFLOW_PREFIX", None)
    elif storage and prefix:
        os.environ["JOBQ_WORKFLOW_PREFIX"] = f"{storage}/{prefix}"

    from ai4s.jobq.track.app import run_with_default_queue

    run_with_default_queue(debug=debug, port=port, open_browser=not no_open)


_CONFIG_TEMPLATE = """\
# ai4s-jobq workflow config. Shared by the coordinator, workers, and
# clients so multiple terminals use one target without exporting env vars.
# Precedence: CLI flag > env var > this file > built-in default.
connection:
  storage: {storage}          # storage account hosting workflow tables
  prefix:  {prefix}           # namespaces tables: <prefix>Workflows, <prefix>WorkflowTasks
  # queues: sb://my-namespace          # optional: Service Bus, or another account
  # blobs:  {storage}/jobq-workflow-data  # optional: large-output blob storage

# Optional coordinator tuning defaults (all overridable per-invocation):
# coordinator:
#   batch_size: 32
#   visibility_timeout_s: 60
#   running_timeout_s: 3600
"""


@workflow_group.group("config")
def workflow_config() -> None:
    """Inspect and scaffold the shared jobq.yaml config file."""


@workflow_config.command("init")
@click.option(
    "--path",
    "out_path",
    default="jobq.yaml",
    show_default=True,
    type=click.Path(),
    help="Where to write the config file.",
)
@click.option("--force", is_flag=True, help="Overwrite an existing file.")
@click.pass_context
def workflow_config_init(ctx: click.Context, out_path: str, force: bool) -> None:
    """Scaffold a commented jobq.yaml, pre-filled from any resolved target."""
    import os

    if os.path.exists(out_path) and not force:
        raise click.UsageError(f"{out_path} already exists; pass --force to overwrite.")

    storage = ctx.obj.get("storage") or "myaccount"
    prefix = ctx.obj.get("prefix") or "MyProject"
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(_CONFIG_TEMPLATE.format(storage=storage, prefix=prefix))
    click.echo(f"Wrote {out_path}")


@workflow_config.command("show")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.pass_context
def workflow_config_show(ctx: click.Context, as_json: bool) -> None:
    """Show the resolved configuration and where each value came from."""
    env = _resolve_env(ctx)

    if as_json:
        click.echo(
            json.dumps(
                {
                    "storage": env.state_account,
                    "prefix": env.prefix,
                    "queues": env.queues,
                    "blobs": f"{env.blob_account}/{env.blob_container}",
                    "sources": env.sources,
                    "config_path": env.config_path,
                    "coordinator": env.coordinator,
                },
                indent=2,
            )
        )
        return

    rows = [
        ("storage", env.state_account, env.sources.get("storage", "")),
        ("prefix", env.prefix, env.sources.get("prefix", "")),
        ("queues", env.queues, env.sources.get("queues", "")),
        ("blobs", f"{env.blob_account}/{env.blob_container}", env.sources.get("blobs", "")),
    ]
    width = max(len(v) for _, v, _ in rows)
    for key, value, source in rows:
        tag = f"({source})" if source else ""
        click.echo(f"  {key:<8} {value:<{width}}  {tag}")
    click.echo(f"  {'config':<8} {env.config_path or '<none found>'}")
    if env.coordinator:
        click.echo("  coordinator (from config file):")
        for key, value in env.coordinator.items():
            click.echo(f"    {key} = {value}")
