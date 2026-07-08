#!/usr/bin/env python
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Full-stack multi-workflow stress test: coordinator + workers + queues.

Submits many small workflows, launches a coordinator and worker
processes, polls until all workflows complete, and reports stats.

Unlike ``run-local`` (which bypasses queues entirely), this test
exercises the **full** completion path: workers dequeue tasks, execute
them, push completions to the completion queue, and the coordinator
picks them up, advances the DAG, and enqueues successor tasks.

Usage::

    export JOBQ_WORKFLOW_PREFIX=mystorageaccount/StressMulti
    python run_full_stack.py

    # With options:
    python run_full_stack.py --workflows 500 --workers 20 --worker-procs 2

    # Cleanup afterwards:
    ai4s-jobq workflow purge --yes --drain-queues
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import contextlib
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.table import Table

console = Console()
_stderr_console = Console(stderr=True, highlight=False)


# ---------------------------------------------------------------------------
# Coordinator metrics parser
# ---------------------------------------------------------------------------

# The coordinator logs lines like:
#   II ai4s.jobq.workflow.coordinator: [metrics] {'completions_processed': 42, ...}
_METRICS_RE = re.compile(r"\[metrics\]\s+(\{.+\})")


class CoordinatorMetricsReader:
    """Background thread that reads coordinator stdout and parses metrics.

    Computes delta-based rates between successive metrics snapshots so
    the startup phase (recount, activation) doesn't skew the numbers.
    """

    def __init__(self, proc: subprocess.Popen) -> None:
        self._proc = proc
        self._latest: dict[str, float] = {}
        self._prev: dict[str, float] = {}
        self._prev_time: float = 0.0
        self._latest_time: float = 0.0
        self._lock = threading.Lock()
        self._console: Console = _stderr_console
        self._log_file = open(  # noqa: SIM115
            Path(__file__).parent / "logs" / "coordinator.log", "a"
        )
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def _read_loop(self) -> None:
        assert self._proc.stdout is not None
        for line in self._proc.stdout:
            # Forward all lines to log file
            self._log_file.write(line)
            self._log_file.flush()
            # Parse metrics snapshots
            m = _METRICS_RE.search(line)
            if m:
                try:
                    snap = ast.literal_eval(m.group(1))
                    now = time.monotonic()
                    with self._lock:
                        self._prev = self._latest
                        self._prev_time = self._latest_time
                        self._latest = snap
                        self._latest_time = now
                except Exception:  # noqa: S110
                    pass
                continue
            # Colorize important lines via rich
            self._print_log_line(line.rstrip(), self._console)

    @staticmethod
    def _print_log_line(line: str, out: Console) -> None:
        """Print a coordinator log line with rich colors."""
        if not line:
            return
        # Skip noisy DEBUG / INFO lines — only show events worth attention
        if " INFO:" in line and "workflow_" not in line:
            return
        if " DEBUG:" in line:
            return

        # Strip the logger name suffix for brevity
        line = re.sub(r"\s*\[ai4s\.jobq\.\S+\]\s*$", "", line)

        # Shorten repetitive orphan-repair messages
        m = re.search(
            r"Repaired orphaned child (\S+?)/(\S+?): (\S+) → (\S+) "
            r"\(completed_deps (\d+)→(\d+).*?\)",
            line,
        )
        if m:
            wf_short = m.group(1)[:8]
            task, old_st, new_st = m.group(2), m.group(3), m.group(4)
            deps_from, deps_to = m.group(5), m.group(6)
            out.print(
                f"  [yellow]⚕ repair[/yellow] {wf_short}…/{task}: "
                f"[dim]{old_st}[/dim] → [green]{new_st}[/green] "
                f"(deps {deps_from}→{deps_to})"
            )
            return

        # Workflow terminal events
        if "workflow_completed" in line:
            m2 = re.search(r"wf=(\S+)\s+\(([^)]+)\)", line)
            name = m2.group(2) if m2 else "?"
            out.print(f"  [green]✓ completed[/green] {name}")
            return
        if "workflow_failed" in line:
            m2 = re.search(r"wf=(\S+)\s+\(([^)]+)\)", line)
            name = m2.group(2) if m2 else "?"
            out.print(f"  [red]✗ failed[/red] {name}")
            return

        # Activation
        if "workflow_activated" in line:
            m2 = re.search(r"wf=(\S+)\s+\(([^)]+)\)", line)
            name = m2.group(2) if m2 else "?"
            out.print(f"  [blue]▶ activated[/blue] {name}")
            return

        # Warnings and errors (generic)
        if " ERROR:" in line or " CRITICAL:" in line:
            out.print(f"  [bold red]{line}[/bold red]")
        elif " WARNING:" in line:
            # Truncate overly long warning lines
            if len(line) > 120:
                line = line[:117] + "…"
            out.print(f"  [yellow]{line}[/yellow]")

    def _delta(self, key: str) -> float:
        """Compute the change in a counter between the last two snapshots."""
        return self._latest.get(key, 0) - self._prev.get(key, 0)

    def _delta_rate(self, key: str) -> float:
        """Compute the per-second rate of a counter between snapshots."""
        dt = self._latest_time - self._prev_time
        if dt <= 0:
            return 0.0
        return self._delta(key) / dt

    def _delta_avg_ms(self, total_key: str, count_key: str) -> float:
        """Compute average latency from delta of total_ms and count."""
        dc = self._delta(count_key)
        if dc <= 0:
            return 0.0
        return self._delta(total_key) / dc

    def bottleneck_summary(self) -> str:
        """Return a short string identifying the likely bottleneck.

        All rates and latencies are computed over the most recent
        metrics interval (delta between last two snapshots), so the
        startup phase doesn't pollute the numbers.
        """
        with self._lock:
            if not self._latest:
                return "[dim]waiting for coordinator metrics...[/dim]"
            if not self._prev:
                return "[dim]waiting for second metrics tick...[/dim]"

            cps = self._delta_rate("completions_processed")
            d_empty = self._delta("empty_polls")
            d_received = self._delta("messages_received")
            d_enqueued = self._delta("tasks_enqueued")
            d_races = self._delta("apply_completion_races")

            # Phase latencies (delta-based averages over this interval)
            phases = {
                "recv": self._delta_avg_ms("receive_msg_total_ms", "receive_msg_calls"),
                "fetch": self._delta_avg_ms("fetch_entities_total_ms", "fetch_entities_calls"),
                "apply": self._delta_avg_ms("apply_completion_total_ms", "apply_completion_calls"),
                "children": self._delta_avg_ms(
                    "advance_children_total_ms", "advance_children_calls"
                ),
                "enqueue": self._delta_avg_ms("enqueue_task_total_ms", "enqueue_task_calls"),
                "summary": self._delta_avg_ms("summary_update_total_ms", "summary_update_calls"),
                "ack": self._delta_avg_ms("ack_msg_total_ms", "ack_msg_calls"),
                "e2e": self._delta_avg_ms("completion_e2e_total_ms", "completion_e2e_calls"),
            }
            slowest_phase = max(phases, key=phases.get)  # type: ignore[arg-type]
            slowest_ms = phases[slowest_phase]

            # ETag contention (delta)
            d_etag_child = self._delta("etag_conflicts_child")
            d_etag_summary = self._delta("etag_conflicts_summary")

            # Coalescer effectiveness (cumulative is fine here)
            child_coalesce = self._latest.get("child_dep_coalescer_coalesce_factor", 0)
            summary_coalesce = self._latest.get("summary_coalescer_coalesce_factor", 0)

            # Cumulative totals for context
            total_completed = self._latest.get("completions_processed", 0)
            total_enqueued = self._latest.get("tasks_enqueued", 0)

        parts = [f"[cyan]{cps:.1f}[/cyan] completions/s"]

        # Total per-completion cost (sum of phase averages)
        total_phase_ms = sum(phases.values())

        # Diagnose zero-throughput intervals
        if cps == 0:
            if d_received == 0 and d_empty == 0:
                parts.append("[yellow]stalled[/yellow] — no receive activity")
            elif d_received == 0 and d_empty > 0:
                parts.append(f"[yellow]idle[/yellow] — queue empty ({d_empty:.0f} empty polls)")
            elif d_received > 0:
                parts.append(f"[red]failing[/red] — received {d_received:.0f} but 0 succeeded")
                if d_races > 0:
                    parts.append(f"races={d_races:.0f}")
        elif d_received > 0 and d_empty < d_received * 0.1:
            # Queue never empties — coordinator can't keep up
            parts.append(
                f"[red]saturated[/red] ({total_phase_ms:.0f}ms/completion, "
                f"recv={d_received:.0f} empty={d_empty:.0f})"
            )
        elif d_received > 0 and d_empty > d_received * 2:
            parts.append("[yellow]starved[/yellow] (workers slow)")
        elif slowest_ms > 50:
            parts.append(f"[red]bottleneck[/red]: {slowest_phase} ({slowest_ms:.0f}ms)")
        else:
            parts.append("[green]healthy[/green]")

        # Phase breakdown (always show)
        phase_strs = [f"{k}={v:.0f}ms" for k, v in phases.items() if v > 0]
        if phase_strs:
            parts.append(" ".join(phase_strs))

        if d_etag_child + d_etag_summary > 0:
            parts.append(f"ETag: child={d_etag_child:.0f} summary={d_etag_summary:.0f}")

        if child_coalesce > 1.1:
            parts.append(f"coalesce: child={child_coalesce:.1f}x summary={summary_coalesce:.1f}x")

        # Enqueue activity — shows whether new tasks are being activated
        if d_enqueued > 0:
            parts.append(f"enqueued={d_enqueued:.0f}")

        # Cumulative context
        wf_hits = self._latest.get("workflow_cache_hits", 0)
        wf_misses = self._latest.get("workflow_cache_misses", 0)
        task_hits = self._latest.get("task_cache_hits", 0)
        task_misses = self._latest.get("task_cache_misses", 0)
        wf_rate = wf_hits / max(wf_hits + wf_misses, 1) * 100
        task_rate = task_hits / max(task_hits + task_misses, 1) * 100
        summary_writes = self._latest.get("summary_coalescer_writes", 0)
        parts.append(
            f"[dim]Σ done={total_completed} enq={total_enqueued} "
            f"sw={summary_writes:.0f} "
            f"cache: wf={wf_rate:.0f}% task={task_rate:.0f}%[/dim]"
        )

        # Error indicators (only when non-zero)
        summary_errors = self._latest.get("summary_coalescer_write_errors", 0)
        child_errors = self._latest.get("child_dep_coalescer_write_errors", 0)
        ack_failures = self._latest.get("ack_msg_failures", 0)
        error_parts = []
        if summary_errors:
            error_parts.append(f"summary_write_err={summary_errors}")
        if child_errors:
            error_parts.append(f"child_write_err={child_errors}")
        if ack_failures:
            error_parts.append(f"ack_fail={ack_failures}")
        if error_parts:
            parts.append("[red]" + " ".join(error_parts) + "[/red]")

        return "  ".join(parts)


def _require_env() -> str:
    """Ensure JOBQ_WORKFLOW_PREFIX is set."""
    val = os.environ.get("JOBQ_WORKFLOW_PREFIX", "").strip()
    if not val:
        console.print(
            "[red]JOBQ_WORKFLOW_PREFIX not set.[/red]  "
            "Export it first, for example:\n"
            "  export JOBQ_WORKFLOW_PREFIX=mystorageaccount/StressMulti"
        )
        sys.exit(1)
    return val


def _account_from_env(wf: str) -> str:
    """Extract the account segment from JOBQ_WORKFLOW_PREFIX=account/prefix."""
    return wf.rsplit("/", 1)[0]


# ---------------------------------------------------------------------------
# Step 1: Generate workflow JSONs
# ---------------------------------------------------------------------------


def generate_workflows(
    out_dir: Path,
    count: int,
    seed: int,
    *,
    topology: str | None = None,
    fan_in_width: int | None = None,
    sleep_range: tuple[float, float] | None = None,
) -> int:
    """Generate workflow JSON files and return total task count."""
    import json
    import random

    # Import the generator from the same directory
    sys.path.insert(0, str(Path(__file__).parent))
    from generate import generate_workflow  # type: ignore[import-not-found]

    random.seed(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    total_tasks = 0
    for i in range(count):
        wf = generate_workflow(
            i,
            topology=topology,
            fan_in_width=fan_in_width,
            sleep_range=sleep_range,
        )
        total_tasks += len(wf["tasks"])
        with open(out_dir / f"wf-{i:05d}.json", "w") as f:
            json.dump(wf, f)
    return total_tasks


# ---------------------------------------------------------------------------
# Step 2: Submit workflows
# ---------------------------------------------------------------------------


def submit_workflows(
    out_dir: Path,
    concurrency: int = 20,
) -> None:
    """Submit workflow JSONs via the CLI."""
    files = sorted(out_dir.glob("wf-*.json"))
    console.print(f"  Submitting {len(files)} workflows (concurrency={concurrency})...")
    _submit_batch(files, concurrency)


async def drain_task_queue(account: str, queue_name: str = "stress-test") -> int:
    """Drain stale messages from the shared task queue.

    The generated workflows enqueue tasks into a single shared queue
    name (``stress-test``).  Switching prefixes between runs leaves
    messages in this queue that reference workflow IDs from earlier
    runs (whose tables were purged), and workers then crash with
    ``ResourceNotFoundError`` looking up the missing task rows.
    Drain the queue before submitting new workflows so each run starts
    clean.

    Returns the approximate number of messages drained.
    """
    from azure.storage.queue.aio import QueueClient

    from ai4s.jobq.auth import close_cached_credentials, get_token_credential

    cred = get_token_credential()
    account_url = f"https://{account}.queue.core.windows.net"
    drained = 0
    try:
        async with QueueClient(
            account_url=account_url, queue_name=queue_name, credential=cred
        ) as q:
            with contextlib.suppress(Exception):
                await q.create_queue()
            props = await q.get_queue_properties()
            approx = props.approximate_message_count or 0
            if approx == 0:
                return 0
            await q.clear_messages()
            drained = approx
    finally:
        await close_cached_credentials()
    return drained


def _submit_batch(files: list[Path], concurrency: int) -> None:
    file_list = "\n".join(str(f) for f in files)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ai4s.jobq",
            "workflow",
            "submit",
            "--concurrency",
            str(concurrency),
        ],
        input=file_list,
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "PYTHONPATH": _dev_pythonpath()},
    )
    if result.returncode != 0:
        console.print(f"[red]Submit failed:[/red]\n{result.stderr}")
        sys.exit(1)
    lines = result.stderr.strip().split("\n") if result.stderr else []
    for line in lines[-3:]:
        console.print(f"    {line}")


# ---------------------------------------------------------------------------
# Step 3: Launch coordinator + workers
# ---------------------------------------------------------------------------


def _repo_root() -> str:
    """Return the path to the repo root (so subprocesses can import the dev tree)."""
    # examples/stress_test/run_full_stack.py → repo root is two levels up
    return str(Path(__file__).resolve().parents[2])


def _dev_pythonpath(extra: list[str] | None = None) -> str:
    """Build a PYTHONPATH that prefers the dev source tree over any installed copy.

    Without this, ``python -m ai4s.jobq`` inside ``examples/stress_test``
    (or any cwd that doesn't contain the package) loads the *installed*
    copy and silently runs stale code.
    """
    parts = [_repo_root()]
    if extra:
        parts.extend(extra)
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


def launch_coordinator(*, max_running: int = 100) -> subprocess.Popen:
    """Start the coordinator as a subprocess."""
    cmd = [
        sys.executable,
        "-m",
        "ai4s.jobq",
        "workflow",
        "coordinator",
    ]
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": _dev_pythonpath()}
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    console.print(f"  Coordinator PID={proc.pid}")
    return proc


def _queue_backend_spec(account: str, queue_name: str) -> str:
    """Build the CLI backend spec, respecting JOBQ_WORKFLOW_QUEUES.

    When ``JOBQ_WORKFLOW_QUEUES`` points at a Service Bus namespace
    (``sb://…``), the backend spec must use the ``sb://`` prefix so the
    CLI connects to Service Bus instead of a Storage Queue.
    """
    queues_override = os.environ.get("JOBQ_WORKFLOW_QUEUES", "").strip()
    if queues_override.startswith("sb://"):
        return f"{queues_override}/{queue_name}"
    if queues_override:
        return f"{queues_override}/{queue_name}"
    return f"{account}/{queue_name}"


def launch_worker(
    account: str,
    *,
    num_workers: int = 50,
    idle_timeout: str = "30m",
    max_idle_backoff: str = "3s",
    log_dir: Path | None = None,
) -> subprocess.Popen:
    """Start a worker process."""
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
        idle_timeout,
        "--max-idle-backoff",
        max_idle_backoff,
    ]
    existing_pp = os.environ.get("PYTHONPATH", "")
    pp_parts = [str(Path(__file__).parent)]
    if existing_pp:
        pp_parts.append(existing_pp)
    env = {
        **os.environ,
        "PYTHONPATH": _dev_pythonpath(extra=[str(Path(__file__).parent)]),
        "PYTHONUNBUFFERED": "1",
    }
    # Worker output must not go to a PIPE without a reader — the 64KB
    # pipe buffer fills up and the child blocks on any log write,
    # silently freezing all task processing.  Write to a log file so
    # errors are still inspectable.
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"worker-{_next_worker_id()}.log"
        fh = open(log_file, "w")  # noqa: SIM115
    else:
        fh = open(os.devnull, "w")  # noqa: SIM115
        log_file = None
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=fh,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    fh.close()  # child inherited the fd; parent can close its copy
    suffix = f" → {log_file}" if log_file else ""
    console.print(f"  Worker PID={proc.pid} ({num_workers} async workers){suffix}")
    return proc


_worker_counter = 0


def _next_worker_id() -> int:
    global _worker_counter  # noqa: PLW0603
    _worker_counter += 1
    return _worker_counter


# ---------------------------------------------------------------------------
# Step 4: Poll until done
# ---------------------------------------------------------------------------


def _build_progress_table(
    total_wf: int,
    wf_completed: int,
    wf_failed: int,
    total_tasks: int,
    completed_tasks: int,
    elapsed: float,
    prev_completed: int,
    interval: float,
    bottleneck: str = "",
) -> Table:
    grid = Table.grid(padding=(0, 2))
    grid.add_column()

    wf_tbl = Table(title="Workflows", border_style="blue", expand=True)
    wf_tbl.add_column("In-flight", justify="right", style="yellow")
    wf_tbl.add_column("Completed", justify="right", style="cyan")
    wf_tbl.add_column("Failed", justify="right", style="red")
    wf_tbl.add_column("Total", justify="right", style="bold")
    wf_in_flight = max(0, total_wf - wf_completed - wf_failed)
    wf_tbl.add_row(
        str(wf_in_flight),
        str(wf_completed),
        str(wf_failed),
        str(total_wf),
    )
    grid.add_row(wf_tbl)

    task_tbl = Table(title="Tasks", border_style="green", expand=True)
    task_tbl.add_column("Completed", justify="right", style="cyan")
    task_tbl.add_column("Total", justify="right", style="bold")
    task_tbl.add_column("Tasks/s (now)", justify="right", style="magenta")
    task_tbl.add_column("Tasks/s (avg)", justify="right", style="magenta")

    delta = completed_tasks - prev_completed
    rate_now = delta / interval if interval > 0 else 0
    rate_avg = completed_tasks / elapsed if elapsed > 0 else 0

    task_tbl.add_row(
        str(completed_tasks),
        str(total_tasks) if total_tasks > 0 else "?",
        f"{rate_now:.1f}",
        f"{rate_avg:.1f}",
    )
    grid.add_row(task_tbl)

    if bottleneck:
        grid.add_row(f"  Coordinator: {bottleneck}")
    grid.add_row(f"[dim]Elapsed: {elapsed:.0f}s[/dim]")
    return grid


async def poll_until_done(
    total_wf: int,
    total_tasks: int = 0,
    interval: float = 3.0,
    metrics_reader: CoordinatorMetricsReader | None = None,
) -> float:
    """Poll coordinator metrics until all workflows reach terminal state.

    Uses the coordinator's ``workflows_completed`` and ``workflows_failed``
    counters (emitted in the ``[metrics]`` log line) instead of querying
    Table Storage.  This avoids the expensive ``list_workflows()`` scan
    that used to contend with the coordinator's hot path.

    Returns elapsed seconds.
    """
    t0 = time.monotonic()
    prev_completed = 0

    with Live(console=console, refresh_per_second=1) as live:
        # Redirect coordinator log lines through the Live console so
        # they render above the progress table instead of corrupting it.
        if metrics_reader:
            metrics_reader._console = live.console
        while True:
            await asyncio.sleep(interval)
            elapsed = time.monotonic() - t0
            bottleneck = metrics_reader.bottleneck_summary() if metrics_reader else ""

            # Read progress from coordinator metrics
            wf_completed = 0
            wf_failed = 0
            completed_tasks_now = 0
            if metrics_reader:
                with metrics_reader._lock:
                    wf_completed = int(metrics_reader._latest.get("workflows_completed", 0))
                    wf_failed = int(metrics_reader._latest.get("workflows_failed", 0))
                    completed_tasks_now = int(
                        metrics_reader._latest.get("completions_processed", 0)
                    )

            live.update(
                _build_progress_table(
                    total_wf=total_wf,
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

            if wf_completed + wf_failed >= total_wf:
                if metrics_reader:
                    metrics_reader._console = _stderr_console
                return time.monotonic() - t0

            prev_completed = completed_tasks_now

    # Restore console if we exit the loop without returning (shouldn't happen)
    if metrics_reader:
        metrics_reader._console = _stderr_console
    return time.monotonic() - t0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Full-stack workflow stress test (coordinator + workers + queues)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--workflows", type=int, default=2000, help="Number of workflows")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for generation")
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Async workers per worker process",
    )
    parser.add_argument(
        "--worker-procs",
        type=int,
        default=1,
        help="Number of worker processes to launch",
    )
    parser.add_argument(
        "--submit-concurrency",
        type=int,
        default=20,
        help="Concurrent workflow submissions",
    )
    parser.add_argument(
        "--max-running",
        type=int,
        default=200,
        help="Coordinator --max-running-workflows",
    )
    parser.add_argument("--poll-interval", type=float, default=3.0, help="Status poll interval (s)")
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip generation if workflows/ already exists",
    )
    parser.add_argument(
        "--skip-submit",
        action="store_true",
        help="Skip submission (workflows already submitted)",
    )
    parser.add_argument(
        "--topology",
        type=str,
        default=None,
        choices=["linear", "diamond", "wide", "staircase", "resilient_diamond"],
        help="Force a single topology (default: weighted random mix)",
    )
    parser.add_argument(
        "--fan-in-width",
        type=int,
        default=None,
        help="Override fan-out / branch width for topologies that have one",
    )
    parser.add_argument(
        "--sleep-range",
        type=float,
        nargs=2,
        default=None,
        metavar=("LO", "HI"),
        help="Override per-task sleep_s range (uniform LO..HI). "
        "Default: built-in role-specific ranges.",
    )
    args = parser.parse_args()

    wf_env = _require_env()
    account = _account_from_env(wf_env)

    console.rule("[bold blue]Full-stack workflow stress test[/bold blue]")
    console.print(f"  JOBQ_WORKFLOW_PREFIX = {wf_env}")
    console.print(
        f"  Workflows: {args.workflows}, Workers: {args.workers} x {args.worker_procs} proc(s)"
    )
    console.print()

    # --- Generate ---
    work_dir = Path(__file__).parent / "workflows"
    total_tasks = 0
    if args.skip_generate and work_dir.exists() and any(work_dir.glob("wf-*.json")):
        n_files = len(list(work_dir.glob("wf-*.json")))
        console.print(f"[dim]Skipping generation ({n_files} files exist)[/dim]")
        # Count tasks from existing files for the progress display
        import json

        for f in sorted(work_dir.glob("wf-*.json")):
            with open(f) as fh:
                total_tasks += len(json.load(fh)["tasks"])
    else:
        console.print("[bold]1. Generating workflows...[/bold]")
        sleep_range = tuple(args.sleep_range) if args.sleep_range else None
        total_tasks = generate_workflows(
            work_dir,
            args.workflows,
            args.seed,
            topology=args.topology,
            fan_in_width=args.fan_in_width,
            sleep_range=sleep_range,
        )
        console.print(f"  {args.workflows} workflows, {total_tasks} total tasks")

    # --- Submit ---
    if args.skip_submit:
        console.print("[dim]Skipping submission[/dim]")
    else:
        console.print("[bold]2. Submitting workflows...[/bold]")
        # Drain stale task-queue messages from previous runs first;
        # otherwise workers pop tasks for workflow IDs whose tables
        # have been purged and crash with ResourceNotFoundError.
        drained = asyncio.run(drain_task_queue(account))
        if drained:
            console.print(f"  Drained {drained} stale task-queue message(s)")
        submit_workflows(
            work_dir,
            concurrency=args.submit_concurrency,
        )

    # --- Launch coordinator + workers ---
    console.print("[bold]3. Launching coordinator + workers...[/bold]")
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    procs: list[subprocess.Popen] = []
    try:
        coord = launch_coordinator(max_running=args.max_running)
        procs.append(coord)
        metrics_reader = CoordinatorMetricsReader(coord)

        # Brief pause so coordinator creates queues before workers connect
        time.sleep(2)

        for _i in range(args.worker_procs):
            w = launch_worker(account, num_workers=args.workers, log_dir=log_dir)
            procs.append(w)

        # --- Poll ---
        console.print("[bold]4. Waiting for completion...[/bold]")
        elapsed = asyncio.run(
            poll_until_done(
                args.workflows,
                total_tasks=total_tasks,
                interval=args.poll_interval,
                metrics_reader=metrics_reader,
            )
        )

        console.rule("[bold green]Done[/bold green]")
        console.print(f"  {args.workflows} workflows completed in {elapsed:.1f}s")
        console.print("  Cleanup: [dim]ai4s-jobq workflow purge --yes --drain-queues[/dim]")

    finally:
        # Graceful shutdown: SIGTERM coordinator and workers
        console.print("\n[dim]Shutting down subprocesses...[/dim]")
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        # Give them a few seconds to drain
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()


if __name__ == "__main__":
    main()
