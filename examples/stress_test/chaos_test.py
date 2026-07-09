#!/usr/bin/env python
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Chaos-test harness for the workflow stress test.

Submits N workflows, launches a coordinator + a fleet of worker
processes, then injects failures (SIGKILL coordinator or workers) at
configurable intervals and restarts them.  The goal is to confirm:

* in-flight task messages redeliver and complete,
* the coordinator's stuck-task and orphan sweepers repair state,
* no completions are silently lost across cold restarts.

Usage::

    export JOBQ_WORKFLOW_PREFIX=haschulzbackup/StressChaos
    python chaos_test.py --workflows 50 --workers 20 --kill-workers-every 25 \
        --kill-coordinator-after 60

By default the script exits as soon as
``workflows_completed + workflows_failed == --workflows``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

# Reuse helpers from run_full_stack.
sys.path.insert(0, str(Path(__file__).parent))
from rich.console import Console
from run_full_stack import (
    CoordinatorMetricsReader,
    _account_from_env,
    _require_env,
    drain_task_queue,
    generate_workflows,
    launch_coordinator,
    submit_workflows,
)

console = Console()


def launch_worker_with_visibility(
    account: str,
    *,
    num_workers: int,
    log_dir: Path,
    visibility_timeout: str = "60s",
) -> subprocess.Popen:
    """Like ``launch_worker`` but with a short visibility timeout so
    tasks abandoned by SIGKILL'd workers reappear in the queue quickly
    enough for the chaos test to make progress (default JobQ visibility
    is 10 minutes, far too long for chaos cycles every ~30s).
    """
    from run_full_stack import _dev_pythonpath, _next_worker_id, _queue_backend_spec

    backend_spec = _queue_backend_spec(account, "stress-test")
    cmd = [
        sys.executable,
        "-m",
        "ai4s.jobq",
        backend_spec,
        "worker",
        "--proc",
        "dummy_processor.DummyWorkflowProcessor",
        "-n",
        str(num_workers),
        "--idle-timeout",
        "30m",
        "--max-idle-backoff",
        "3s",
        "--visibility-timeout",
        visibility_timeout,
    ]
    env = {
        **os.environ,
        "PYTHONPATH": _dev_pythonpath(extra=[str(Path(__file__).parent)]),
        "PYTHONUNBUFFERED": "1",
    }
    log_file = log_dir / f"worker-{_next_worker_id()}.log"
    fh = open(log_file, "w")  # noqa: SIM115
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=fh,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    fh.close()
    console.print(
        f"  Worker PID={proc.pid} ({num_workers} async workers, vis={visibility_timeout}) → {log_file}"
    )
    return proc


def launch_coordinator_break_lease(*, max_running: int) -> subprocess.Popen:
    """Like ``launch_coordinator`` but passes ``--break-lease`` so a fresh
    coordinator can take over from one that died holding the blob lease
    (without waiting up to 60s for the lease to expire).
    """
    cmd = [
        sys.executable,
        "-m",
        "ai4s.jobq",
        "workflow",
        "coordinator",
        "--break-lease",
    ]
    env = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
    }
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    console.print(f"  Coordinator (relaunch, --break-lease) PID={proc.pid}")
    return proc


def _alive(p: subprocess.Popen) -> bool:
    return p.poll() is None


def _kill(p: subprocess.Popen, *, sig: int = signal.SIGKILL) -> None:
    if not _alive(p):
        return
    try:
        p.send_signal(sig)
    except ProcessLookupError:
        return
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


async def chaos_loop(
    *,
    account: str,
    total_wf: int,
    workers_per_proc: int,
    worker_procs: int,
    max_running: int,
    kill_workers_every: float,
    kill_coordinator_after: float | None,
    kill_coordinator_every: float = 0.0,
    log_dir: Path,
    poll_interval: float = 5.0,
) -> dict[str, int]:
    """Run the chaos test and return final coordinator metrics."""
    rnd = random.Random(0)
    coord_proc = launch_coordinator(max_running=max_running)
    metrics = CoordinatorMetricsReader(coord_proc)
    await asyncio.sleep(2)

    workers: list[subprocess.Popen] = [
        launch_worker_with_visibility(account, num_workers=workers_per_proc, log_dir=log_dir)
        for _ in range(worker_procs)
    ]

    t0 = time.monotonic()
    next_worker_kill = t0 + kill_workers_every if kill_workers_every > 0 else float("inf")
    coord_killed_once = False
    next_coord_kill = t0 + kill_coordinator_every if kill_coordinator_every > 0 else float("inf")

    # Build a WorkflowClient for summary polling.  The coordinator
    # in-memory metrics counter (``workflows_completed``) resets to
    # zero on every coordinator restart, so it's unsafe as a chaos
    # progress signal — query Table Storage directly instead.
    from ai4s.jobq.workflow.client import WorkflowClient

    wf_client = await WorkflowClient.from_environment()

    try:
        while True:
            await asyncio.sleep(poll_interval)
            now = time.monotonic()
            elapsed = now - t0

            with metrics._lock:
                snap = dict(metrics._latest)
            cps = snap.get("completions_processed", 0)

            summary = await wf_client.summary()
            done = summary.workflows.get("completed", 0)
            failed = summary.workflows.get("failed", 0)

            console.print(
                f"[dim]t={elapsed:5.0f}s[/dim] "
                f"wf done={done} failed={failed}/{total_wf}  "
                f"tasks_done={summary.completed_tasks} running={summary.running_tasks} "
                f"completions={cps}  "
                f"workers_alive={sum(_alive(w) for w in workers)}/"
                f"{len(workers)}  coord_alive={_alive(coord_proc)}"
            )

            # Respawn any worker process that died on its own (crash,
            # unhandled exception, etc.).  Without this, a "low-chaos"
            # run can stall completely when every worker has died but
            # the chaos schedule isn't due to kill-and-replace.
            for idx, w in enumerate(workers):
                if not _alive(w):
                    console.print(
                        f"[red]✗ worker PID={w.pid} (idx {idx}) died unexpectedly — respawning[/red]"
                    )
                    workers[idx] = launch_worker_with_visibility(
                        account, num_workers=workers_per_proc, log_dir=log_dir
                    )

            # Respawn coordinator if it died on its own.
            if not _alive(coord_proc):
                console.print(
                    f"[red]✗ coordinator PID={coord_proc.pid} died unexpectedly — respawning[/red]"
                )
                await asyncio.sleep(2)
                coord_proc = launch_coordinator_break_lease(max_running=max_running)
                metrics = CoordinatorMetricsReader(coord_proc)

            if done + failed >= total_wf:
                console.print("[green]All workflows reached terminal state[/green]")
                # Snapshot one last time including ground-truth summary.
                snap["_summary_completed"] = done
                snap["_summary_failed"] = failed
                return snap

            # Worker chaos: kill a random worker and immediately respawn it.
            if now >= next_worker_kill and workers:
                victim_idx = rnd.randrange(len(workers))
                victim = workers[victim_idx]
                console.print(
                    f"[yellow]💣 SIGKILL worker PID={victim.pid} (idx {victim_idx})[/yellow]"
                )
                _kill(victim)
                fresh = launch_worker_with_visibility(
                    account, num_workers=workers_per_proc, log_dir=log_dir
                )
                workers[victim_idx] = fresh
                next_worker_kill = now + kill_workers_every

            # Coordinator chaos: kill exactly once at the requested time.
            if (
                kill_coordinator_after is not None
                and not coord_killed_once
                and elapsed >= kill_coordinator_after
            ):
                console.print(f"[red]💥 SIGKILL coordinator PID={coord_proc.pid}[/red]")
                _kill(coord_proc)
                coord_killed_once = True
                # Tear down old metrics reader and restart with
                # --break-lease so we don't have to wait ~60s for the
                # dead coordinator's blob lease to expire.
                await asyncio.sleep(2)
                coord_proc = launch_coordinator_break_lease(max_running=max_running)
                metrics = CoordinatorMetricsReader(coord_proc)
                console.print("[blue]🔄 coordinator relaunched[/blue]")

            # Periodic coordinator kills (in addition to the one-shot above).
            if now >= next_coord_kill:
                console.print(f"[red]💥 periodic SIGKILL coordinator PID={coord_proc.pid}[/red]")
                _kill(coord_proc)
                await asyncio.sleep(2)
                coord_proc = launch_coordinator_break_lease(max_running=max_running)
                metrics = CoordinatorMetricsReader(coord_proc)
                next_coord_kill = now + kill_coordinator_every
                console.print("[blue]🔄 coordinator relaunched (periodic)[/blue]")
    finally:
        console.print("[dim]Cleaning up subprocesses...[/dim]")
        _kill(coord_proc, sig=signal.SIGTERM)
        for w in workers:
            _kill(w, sig=signal.SIGTERM)
        await wf_client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Chaos test for workflow engine")
    parser.add_argument("--workflows", type=int, default=50)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--worker-procs", type=int, default=2)
    parser.add_argument("--max-running", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--kill-workers-every",
        type=float,
        default=30.0,
        help="Seconds between random-worker SIGKILLs (0 = never)",
    )
    parser.add_argument(
        "--kill-coordinator-after",
        type=float,
        default=None,
        help="Seconds after start to SIGKILL the coordinator once (default: never)",
    )
    parser.add_argument(
        "--kill-coordinator-every",
        type=float,
        default=0.0,
        help="Seconds between periodic coordinator SIGKILLs (0 = never)",
    )
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--skip-submit", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=5.0)
    args = parser.parse_args()

    wf_env = _require_env()
    account = _account_from_env(wf_env)

    work_dir = Path(__file__).parent / "workflows"
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)

    if not args.skip_generate:
        console.print("[bold]1. Generating workflows...[/bold]")
        total_tasks = generate_workflows(work_dir, args.workflows, args.seed)
        console.print(f"  {args.workflows} workflows, {total_tasks} tasks")

    if not args.skip_submit:
        console.print("[bold]2. Draining old task queue + submitting...[/bold]")
        drained = asyncio.run(drain_task_queue(account))
        if drained:
            console.print(f"  Drained {drained} stale task-queue messages")
        submit_workflows(work_dir, concurrency=20)

    console.print("[bold]3. Launching coordinator + workers (chaos mode)...[/bold]")
    final = asyncio.run(
        chaos_loop(
            account=account,
            total_wf=args.workflows,
            workers_per_proc=args.workers,
            worker_procs=args.worker_procs,
            max_running=args.max_running,
            kill_workers_every=args.kill_workers_every,
            kill_coordinator_after=args.kill_coordinator_after,
            kill_coordinator_every=args.kill_coordinator_every,
            log_dir=log_dir,
            poll_interval=args.poll_interval,
        )
    )

    console.rule("[bold green]Chaos test result[/bold green]")
    done = int(final.get("_summary_completed", final.get("workflows_completed", 0)))
    failed = int(final.get("_summary_failed", final.get("workflows_failed", 0)))
    console.print(
        f"  {done} completed + {failed} failed = {done + failed}/{args.workflows} (ground truth)"
    )
    if done + failed != args.workflows:
        console.print("[red]MISMATCH — some workflows still in flight[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
