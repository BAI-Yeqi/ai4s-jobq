#!/usr/bin/env python
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Full-stack single-workflow stress test using the CSE topology.

Like :mod:`run_full_stack` but submits **one** large workflow (the CSE
DAG built from ``sample-data*.json``) instead of many small generated
ones.  Useful for measuring how well the coordinator scales a *single*
workflow's hot path — which after task-row sharding spreads its
completions across all 16 task-row partitions, allowing the
coordinator's per-bucket EGT batching to amortise round trips.

Steps:

1. **Purge** existing workflow + task tables and drain stale queue
   messages (skippable with ``--no-purge``).
2. **Generate** the CSE workflow JSON from the input sample-data file.
3. **Submit** the workflow under a deterministic ID.
4. **Launch** the coordinator (``--single`` mode) and worker
   subprocesses.
5. **Poll** coordinator metrics until the workflow reaches a terminal
   state, rendering a live progress + bottleneck table.
6. **Shutdown** all subprocesses gracefully.

Usage::

    export JOBQ_WORKFLOW_PREFIX=mystorageaccount/CseStress
    python run_cse_stack.py --input ../../sample-data-mindless.json

The shared helpers (coordinator launcher, metrics reader, worker
launcher, progress rendering) are reused from :mod:`run_full_stack` so
both runners stay in sync as the metrics surface evolves.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from rich.console import Console
from rich.live import Live

# ---------------------------------------------------------------------------
# Reuse shared helpers from run_full_stack (CoordinatorMetricsReader,
# launch_coordinator, launch_worker, _build_progress_table, ...).  The
# sibling module is gated under ``if __name__ == "__main__":`` so import
# is side-effect free.
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).parent))
# Importing cse_workflow this way also requires the same sys.path
# insert; the module lives in the same directory.
from cse_workflow import (
    SIM_COUNT_DEFAULT,
    QueueConfig,
    SleepProfile,
    _compute_stats,
    _load_bs,
    _render_stats,
    build_workflow,
)
from run_full_stack import (
    CoordinatorMetricsReader,
    _account_from_env,
    _build_progress_table,
    _dev_pythonpath,
    _require_env,
    drain_task_queue,
    launch_worker,
)

console = Console()


# ---------------------------------------------------------------------------
# Coordinator launcher (local override of run_full_stack.launch_coordinator)
# ---------------------------------------------------------------------------


def launch_coordinator(
    *,
    max_running: int = 100,
    break_lease: bool = True,
) -> subprocess.Popen:
    """Start the coordinator as a subprocess.

    Local override of :func:`run_full_stack.launch_coordinator` that
    adds ``--break-lease`` support.  A stress run frequently follows a
    killed prior coordinator, leaving a stale blob lease that would
    otherwise block startup with ``CoordinatorLeaseHeld``.  Default is
    on for this runner since we just purged everything anyway.
    """
    cmd = [
        sys.executable,
        "-m",
        "ai4s.jobq",
        "workflow",
        "coordinator",
        "--max-running-workflows",
        str(max_running),
        "--single",
    ]
    if break_lease:
        cmd.append("--break-lease")
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": _dev_pythonpath()}
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    extra = " [yellow](--break-lease)[/yellow]" if break_lease else ""
    console.print(f"  Coordinator PID={proc.pid}{extra}")
    return proc


# ---------------------------------------------------------------------------
# Step: Purge
# ---------------------------------------------------------------------------


def purge_workflow_data(*, drain_queues: bool = True) -> None:
    """Run ``ai4s-jobq workflow purge --yes [--drain-queues]``.

    The purge removes all workflow + task rows from Table Storage.
    Combined with ``--drain-queues`` it also clears any in-flight
    task-queue / completion-queue messages from prior runs so a stale
    coordinator-bus state doesn't leak into the new run.
    """
    cmd = [
        sys.executable,
        "-m",
        "ai4s.jobq",
        "workflow",
        "purge",
        "--yes",
    ]
    if drain_queues:
        cmd.append("--drain-queues")
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": _dev_pythonpath()}
    result = subprocess.run(cmd, env=env, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        console.print(f"[red]Purge failed (exit {result.returncode}):[/red]")
        if result.stderr:
            console.print(result.stderr)
        sys.exit(1)
    # Emit a couple of lines from stderr/stdout for visibility.
    for line in (result.stderr or result.stdout).splitlines()[-5:]:
        console.print(f"    {line}")


# ---------------------------------------------------------------------------
# Step: Generate the single CSE workflow JSON
# ---------------------------------------------------------------------------


def generate_cse_workflow_json(
    *,
    input_path: Path,
    out_path: Path,
    name: str,
    sim_count: int,
    m_seconds: tuple[float, float],
    b_seconds: tuple[float, float],
    sim_seconds: tuple[float, float],
    d_seconds: tuple[float, float],
    time_scale: float,
    num_retries: int,
    default_task_timeout_s: int | None,
    max_parallelism: int | None,
    fail_pct_m: int,
    fail_pct_other: int,
    seed: int,
    show_stats: bool,
) -> int:
    """Build the CSE workflow definition and write it to *out_path*.

    Returns the total task count for the progress display.
    """
    import random

    random.seed(seed)
    bs = _load_bs(input_path)
    queues = QueueConfig()  # all roles share ``stress-test`` queue
    sleep = SleepProfile(
        m=m_seconds,
        b=b_seconds,
        sim=sim_seconds,
        d=d_seconds,
        time_scale=time_scale,
    )
    definition = build_workflow(
        bs,
        name=name,
        queues=queues,
        sleep=sleep,
        num_retries=num_retries,
        default_task_timeout_s=default_task_timeout_s,
        max_parallelism=max_parallelism,
        fail_pct_m=fail_pct_m,
        fail_pct_other=fail_pct_other,
        sim_count=sim_count,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(definition.to_json())

    if show_stats:
        stats = _compute_stats(bs, sim_count=sim_count)
        _render_stats(stats, console, title=f"Generated: {out_path}")

    n_tasks = len(definition.tasks)
    console.print(
        f"  Wrote [cyan]{out_path}[/] — [bold]{n_tasks:,}[/] tasks "
        f"({out_path.stat().st_size / 1024 / 1024:.1f} MB)"
    )
    return n_tasks


# ---------------------------------------------------------------------------
# Step: Submit the single workflow with a known ID
# ---------------------------------------------------------------------------


def submit_single_workflow(json_path: Path, workflow_id: str) -> None:
    """Submit *json_path* under *workflow_id* via the CLI.

    Uses ``--id`` so the polling loop can map back to the workflow.
    """
    cmd = [
        sys.executable,
        "-m",
        "ai4s.jobq",
        "workflow",
        "submit",
        "--id",
        workflow_id,
        str(json_path),
    ]
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": _dev_pythonpath()}
    result = subprocess.run(cmd, env=env, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        console.print(f"[red]Submit failed (exit {result.returncode}):[/red]")
        if result.stderr:
            console.print(result.stderr)
        sys.exit(1)
    for line in (result.stderr or result.stdout).splitlines()[-3:]:
        if line.strip():
            console.print(f"    {line}")
    console.print(f"  Submitted workflow id=[cyan]{workflow_id}[/cyan]")


# ---------------------------------------------------------------------------
# Step: Poll until the single workflow terminates
# ---------------------------------------------------------------------------


class CoordinatorDied(RuntimeError):  # noqa: N818 — descriptive name preferred over Error suffix
    """Raised when the coordinator subprocess exits mid-poll."""


async def poll_single_workflow_until_done(
    *,
    total_tasks: int,
    interval: float,
    metrics_reader: CoordinatorMetricsReader,
    coord: subprocess.Popen,
) -> tuple[float, bool]:
    """Poll the coordinator metrics until exactly one workflow finishes.

    Returns ``(elapsed_seconds, succeeded)``.  *succeeded* is ``True``
    when the workflow finished in the ``completed`` state, ``False``
    when it ended in ``failed``.

    Logic mirrors :func:`run_full_stack.poll_until_done` but treats the
    target as a single workflow — done when
    ``workflows_completed + workflows_failed >= 1``.

    Raises :class:`CoordinatorDied` if the coordinator subprocess exits
    before the workflow finishes.  Without this guard the poll loop
    would hang forever waiting for metrics that will never arrive.
    """
    t0 = time.monotonic()
    prev_completed = 0

    with Live(console=console, refresh_per_second=1) as live:
        metrics_reader._console = live.console
        try:
            while True:
                await asyncio.sleep(interval)
                elapsed = time.monotonic() - t0

                rc = coord.poll()
                if rc is not None:
                    raise CoordinatorDied(
                        f"Coordinator subprocess exited with code {rc} "
                        f"after {elapsed:.1f}s; aborting poll loop."
                    )

                with metrics_reader._lock:
                    wf_completed = int(metrics_reader._latest.get("workflows_completed", 0))
                    wf_failed = int(metrics_reader._latest.get("workflows_failed", 0))
                    completed_tasks_now = int(
                        metrics_reader._latest.get("completions_processed", 0)
                    )

                bottleneck = metrics_reader.bottleneck_summary()

                live.update(
                    _build_progress_table(
                        total_wf=1,
                        wf_completed=wf_completed,
                        wf_failed=wf_failed,
                        total_tasks=total_tasks,
                        completed_tasks=completed_tasks_now,
                        elapsed=elapsed,
                        prev_completed=prev_completed,
                        interval=interval,
                        bottleneck=bottleneck,
                    )
                )

                if wf_completed + wf_failed >= 1:
                    return time.monotonic() - t0, wf_completed >= 1

                prev_completed = completed_tasks_now
        finally:
            # Restore the stderr-bound console so post-Live log lines
            # render normally instead of being swallowed by the Live region.
            from run_full_stack import _stderr_console

            metrics_reader._console = _stderr_console


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_pair(s: str) -> tuple[float, float]:
    """Parse 'lo hi' or 'lo,hi' into a (float, float)."""
    parts = s.split(",") if "," in s else s.split()
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"expected 'lo hi' or 'lo,hi'; got {s!r}")
    return float(parts[0]), float(parts[1])


def _default_workflow_id() -> str:
    return f"cse-stack-{uuid.uuid4().hex[:8]}"


def _default_sample_data() -> Path:
    """Best-effort default: pick a sample-data file at the repo root."""
    repo_root = Path(__file__).resolve().parents[2]
    for candidate in ("sample-data-mindless.json", "sample-data.json"):
        p = repo_root / candidate
        if p.exists():
            return p
    return repo_root / "sample-data-mindless.json"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Full-stack single-CSE-workflow stress test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Workflow shape ---
    parser.add_argument(
        "--input",
        type=Path,
        default=_default_sample_data(),
        help="Path to the CSE sample-data JSON.",
    )
    parser.add_argument(
        "--workflow-name",
        type=str,
        default="cse-stack",
        help="Workflow name (becomes part of summary output).",
    )
    parser.add_argument(
        "--workflow-id",
        type=str,
        default=None,
        help="Custom workflow ID. Default: cse-stack-<uuid8>.",
    )
    parser.add_argument(
        "--sim-count",
        type=int,
        default=SIM_COUNT_DEFAULT,
        help="Sim siblings per molecule.",
    )
    parser.add_argument(
        "--m-seconds",
        type=_parse_pair,
        default=(0.1, 0.1),
        help="M-task sleep range (real seconds). Compressed by --time-scale.",
    )
    parser.add_argument(
        "--b-seconds",
        type=_parse_pair,
        default=(0.0, 0.0),
        help="B-task sleep range (typically 0).",
    )
    parser.add_argument(
        "--sim-seconds",
        type=_parse_pair,
        default=(0.1, 0.1),
        help="Sim-task sleep range.",
    )
    parser.add_argument(
        "--d-seconds",
        type=_parse_pair,
        default=(0.0, 0.0),
        help="D-task sleep range (typically 0).",
    )
    parser.add_argument(
        "--time-scale",
        type=float,
        default=1.0,
        help="Divisor applied to sampled seconds.",
    )
    parser.add_argument(
        "--num-retries",
        type=int,
        default=0,
        help="num_retries on every task.",
    )
    parser.add_argument(
        "--max-parallelism",
        type=int,
        default=None,
        help="max_parallelism on the workflow (cap of in-flight tasks).",
    )
    parser.add_argument(
        "--default-task-timeout-s",
        type=int,
        default=None,
        help="default_task_timeout_s on the workflow.",
    )
    parser.add_argument(
        "--fail-m",
        type=int,
        default=0,
        metavar="PCT",
        help=(
            "Deterministic failure rate (%%) for M (baselines) tasks — every attempt fails, "
            "cascading to upstream_failed downstream. Default 0."
        ),
    )
    parser.add_argument(
        "--fail-other",
        type=int,
        default=0,
        metavar="PCT",
        help="Deterministic failure rate (%%) for B, sim, and d tasks — every attempt fails. Default 0.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for sleep sampling.",
    )

    # --- Stack ---
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Async workers per worker process.",
    )
    parser.add_argument(
        "--worker-procs",
        type=int,
        default=1,
        help="Number of worker processes.",
    )
    parser.add_argument(
        "--max-running",
        type=int,
        default=200,
        help="Coordinator --max-running-workflows.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=3.0,
        help="Status poll interval (s).",
    )

    # --- Step toggles ---
    parser.add_argument(
        "--no-purge",
        action="store_true",
        help="Skip the purge step (use when re-running with an existing workflow).",
    )
    parser.add_argument(
        "--no-drain",
        action="store_true",
        help="Skip --drain-queues on purge.",
    )
    parser.add_argument(
        "--no-break-lease",
        action="store_true",
        help=(
            "Do not pass --break-lease to the coordinator. "
            "Default is to break a stale lease left over from a previous run."
        ),
    )
    parser.add_argument(
        "--skip-submit",
        action="store_true",
        help="Skip generation and submission (assumes the workflow is already submitted).",
    )
    parser.add_argument(
        "--workflow-json",
        type=Path,
        default=None,
        help=(
            "Output path for the generated workflow JSON. "
            "Default: examples/stress_test/workflows/cse-stack.json."
        ),
    )
    parser.add_argument(
        "--show-stats",
        action="store_true",
        help="Print batch shape + sim-dedup distribution after generation.",
    )
    return parser


def _terminal_summary(succeeded: bool, elapsed: float, total_tasks: int) -> None:
    rate = total_tasks / elapsed if elapsed > 0 else 0.0
    if succeeded:
        console.rule("[bold green]Done[/bold green]")
        console.print(
            f"  Workflow completed in {elapsed:.1f}s — [bold]{rate:.1f}[/] tasks/s (cumulative)."
        )
    else:
        console.rule("[bold red]Workflow failed[/bold red]")
        console.print(
            f"  Workflow terminated as FAILED after {elapsed:.1f}s. "
            f"Run [dim]ai4s-jobq workflow status <id>[/dim] for details."
        )
    console.print("  Cleanup: [dim]ai4s-jobq workflow purge --yes --drain-queues[/dim]")


def main() -> None:
    args = _build_parser().parse_args()

    wf_env = _require_env()
    account = _account_from_env(wf_env)
    workflow_id = args.workflow_id or _default_workflow_id()
    workflow_json = args.workflow_json or (Path(__file__).parent / "workflows" / "cse-stack.json")

    console.rule("[bold blue]Full-stack CSE single-workflow stress test[/bold blue]")
    console.print(f"  JOBQ_WORKFLOW_PREFIX = {wf_env}")
    console.print(f"  Workflow ID   = {workflow_id}")
    console.print(f"  Workers       = {args.workers} x {args.worker_procs} proc(s)")
    console.print(f"  Sample data   = {args.input}")
    console.print()

    if not args.input.exists():
        console.print(
            f"[red]Input file not found: {args.input}[/red]\n"
            "Pass --input or place sample-data-mindless.json at the repo root."
        )
        sys.exit(1)

    total_tasks = 0

    # --- 1. Purge ---
    if args.no_purge:
        console.print("[dim]Skipping purge[/dim]")
    elif args.skip_submit:
        console.print(
            "[yellow]Skipping purge because --skip-submit is set "
            "(would wipe the workflow you intend to re-run).[/yellow]"
        )
    else:
        console.print("[bold]1. Purging existing workflow data...[/bold]")
        purge_workflow_data(drain_queues=not args.no_drain)

    # --- 2. Generate + 3. Submit ---
    if args.skip_submit:
        console.print("[dim]Skipping generate + submit[/dim]")
        # Best-effort: count tasks from the existing JSON for the
        # progress display (purely informational).
        if workflow_json.exists():
            with contextlib.suppress(Exception):
                total_tasks = len(json.loads(workflow_json.read_text())["tasks"])
    else:
        console.print("[bold]2. Generating CSE workflow JSON...[/bold]")
        total_tasks = generate_cse_workflow_json(
            input_path=args.input,
            out_path=workflow_json,
            name=args.workflow_name,
            sim_count=args.sim_count,
            m_seconds=args.m_seconds,
            b_seconds=args.b_seconds,
            sim_seconds=args.sim_seconds,
            d_seconds=args.d_seconds,
            time_scale=args.time_scale,
            num_retries=args.num_retries,
            default_task_timeout_s=args.default_task_timeout_s,
            max_parallelism=args.max_parallelism,
            fail_pct_m=args.fail_m,
            fail_pct_other=args.fail_other,
            seed=args.seed,
            show_stats=args.show_stats,
        )

        console.print("[bold]3. Submitting workflow...[/bold]")
        # The cse-workflow generator targets queue ``stress-test``; if
        # any messages survived a prior partial run they will be
        # consumed by workers before our fresh submission, leading to
        # ResourceNotFoundError on the task lookup.  Drain proactively
        # even when purge already ran (defensive — the purge above
        # already covers this, but a no-op extra drain is cheap).
        if not args.no_drain:
            drained = asyncio.run(drain_task_queue(account))
            if drained:
                console.print(f"  Drained {drained} stale task-queue message(s)")
        submit_single_workflow(workflow_json, workflow_id)

    # --- 4. Launch coordinator + workers ---
    console.print("[bold]4. Launching coordinator + workers...[/bold]")
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    procs: list[subprocess.Popen] = []
    succeeded = False
    elapsed = 0.0
    try:
        coord = launch_coordinator(
            max_running=args.max_running,
            break_lease=not args.no_break_lease,
        )
        procs.append(coord)
        metrics_reader = CoordinatorMetricsReader(coord)

        # Brief pause so coordinator creates queues before workers connect.
        time.sleep(2)

        for _ in range(args.worker_procs):
            w = launch_worker(account, num_workers=args.workers, log_dir=log_dir)
            procs.append(w)

        # --- 5. Poll ---
        console.print("[bold]5. Waiting for completion...[/bold]")
        try:
            elapsed, succeeded = asyncio.run(
                poll_single_workflow_until_done(
                    total_tasks=total_tasks,
                    interval=args.poll_interval,
                    metrics_reader=metrics_reader,
                    coord=coord,
                )
            )
        except CoordinatorDied as e:
            console.print(f"[red]{e}[/red]")
            log_path = log_dir / "coordinator.log"
            if log_path.exists():
                console.print(f"[red]See coordinator log: {log_path}[/red]")
            succeeded = False
            elapsed = 0.0
        else:
            _terminal_summary(succeeded, elapsed, total_tasks)

    finally:
        # --- 6. Shutdown ---
        console.print("\n[dim]Shutting down subprocesses...[/dim]")
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    sys.exit(0 if succeeded else 2)


if __name__ == "__main__":
    main()
