# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Monitor workflow progress during a stress test.

Polls ``WorkflowClient.summary()`` and renders a live ``rich`` dashboard.

Usage::

    export JOBQ_WORKFLOW_PREFIX=mystorageaccount/StressTest
    python monitor.py --interval 5
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from rich.console import Console
from rich.live import Live
from rich.table import Table

from ai4s.jobq.workflow.client import AggregateStatus, WorkflowClient

console = Console()


def build_display(
    agg: AggregateStatus,
    elapsed: float,
    prev_completed: int,
    interval: float,
) -> Table:
    """Build a rich Table showing the current state."""
    grid = Table.grid(padding=(0, 2))
    grid.add_column()

    # Workflow stats
    wf_tbl = Table(title="Workflows", border_style="blue", expand=True)
    wf_tbl.add_column("Running", justify="right", style="green")
    wf_tbl.add_column("Completed", justify="right", style="cyan")
    wf_tbl.add_column("Failed", justify="right", style="red")
    wf_tbl.add_column("Pending", justify="right", style="yellow")
    wf_tbl.add_column("Total", justify="right", style="bold")
    wf_tbl.add_row(
        str(agg.workflows.get("running", 0)),
        str(agg.workflows.get("completed", 0)),
        str(agg.workflows.get("failed", 0)),
        str(agg.workflows.get("pending", 0)),
        str(agg.total_workflows),
    )
    grid.add_row(wf_tbl)

    # Task stats
    task_tbl = Table(title="Tasks", border_style="blue", expand=True)
    task_tbl.add_column("Ready/Running", justify="right", style="green")
    task_tbl.add_column("Completed", justify="right", style="cyan")
    task_tbl.add_column("Failed", justify="right", style="red")
    task_tbl.add_column("Pending", justify="right", style="yellow")
    task_tbl.add_column("Total", justify="right", style="bold")
    task_tbl.add_row(
        f"{agg.running_tasks:,}",
        f"{agg.completed_tasks:,}",
        f"{agg.failed_tasks:,}",
        f"{agg.pending_tasks:,}",
        f"{agg.total_tasks:,}",
    )
    grid.add_row(task_tbl)

    # Progress bar
    if agg.total_tasks > 0:
        pct = agg.completed_tasks / agg.total_tasks
        bar_width = 40
        filled = int(pct * bar_width)
        bar = "█" * filled + "░" * (bar_width - filled)
        grid.add_row(f"  [{bar}] {pct:.1%}")

    # Throughput
    delta = agg.completed_tasks - prev_completed
    rate = delta / interval if interval > 0 else 0
    overall_rate = agg.completed_tasks / elapsed if elapsed > 0 else 0
    grid.add_row(
        f"  Throughput: [bold]{rate:.0f}[/bold] tasks/s (current)  "
        f"[bold]{overall_rate:.0f}[/bold] tasks/s (avg)  "
        f"Elapsed: [bold]{elapsed:.0f}[/bold] s"
    )

    return grid


async def run(args: argparse.Namespace) -> None:
    async with await WorkflowClient.from_environment(prefix=args.prefix) as client:
        # Show exactly what we're polling so users aren't left wondering
        # whether queue depths or worker liveness are also being tracked.
        from ai4s.jobq.workflow.store import _table_names

        from ai4s.jobq.workflow.env import WorkflowEnv, WorkflowEnvError

        try:
            env = WorkflowEnv.from_environ(prefix=args.prefix)
            state_account = env.state_account
            prefix = env.prefix
        except WorkflowEnvError:
            state_account, prefix = "<unset>", args.prefix
        wf_table, task_table = _table_names(prefix)
        console.print(
            f"Monitoring [bold]{state_account}[/] tables "
            f"[cyan]{wf_table}[/] (counters) + [cyan]{task_table}[/] (per-task), "
            f"polling every [bold]{args.interval}[/]s.  "
            "[dim]Queue depths and worker liveness are not tracked.[/]"
        )

        t0 = time.monotonic()
        prev_completed = 0

        try:
            with Live(console=console, refresh_per_second=1) as live:
                while True:
                    try:
                        agg = await client.summary()
                    except Exception as exc:
                        console.print(f"[red]Poll error: {exc}[/red]")
                        await asyncio.sleep(args.interval)
                        continue

                    elapsed = time.monotonic() - t0
                    live.update(build_display(agg, elapsed, prev_completed, args.interval))
                    prev_completed = agg.completed_tasks

                    active = agg.workflows.get("running", 0) + agg.workflows.get("pending", 0)
                    if agg.total_workflows > 0 and active == 0:
                        console.print("\n[bold green]All workflows finished![/bold green]")
                        break

                    await asyncio.sleep(args.interval)
        except KeyboardInterrupt:
            console.print("\n[dim]Stopped.[/dim]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor workflow stress test")
    parser.add_argument("--interval", type=float, default=5.0, help="Poll interval (seconds)")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--prefix",
        default=None,
        help=(
            "Override the table-name prefix.  Defaults to whatever is "
            "in JOBQ_WORKFLOW_PREFIX=<account>/<prefix>, so you usually don't "
            "need this."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
