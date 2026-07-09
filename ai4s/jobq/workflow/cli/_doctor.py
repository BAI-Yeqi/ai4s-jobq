# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""``ai4s-jobq workflow doctor`` — preflight health checks."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import asyncclick as click

from ai4s.jobq.workflow.cli._shared import _get_client, workflow_group
from ai4s.jobq.workflow.entities import TaskState, WorkflowState

if TYPE_CHECKING:
    from ai4s.jobq.workflow import WorkflowClient
    from ai4s.jobq.workflow.entities import TaskStatus, WorkflowStatus


@workflow_group.command("doctor")
@click.option("--json", "as_json", is_flag=True, help="Output checks as JSON.")
@click.option(
    "--stale-pending-sec",
    type=int,
    default=120,
    show_default=True,
    help="Pending workflows older than this are flagged as stuck (likely no coordinator).",
)
@click.option(
    "--stale-running-min",
    type=int,
    default=60,
    show_default=True,
    help="Running tasks older than this are flagged as stuck (likely worker died).",
)
@click.option(
    "--stale-ready-min",
    type=int,
    default=60,
    show_default=True,
    help=(
        "Ready tasks whose queue message was successfully enqueued more than "
        "this many minutes ago are flagged as likely lost. Verify workers "
        "are still consuming from the affected queues; if the message is "
        "truly lost, ``workflow retry <id>`` rearms failed tasks."
    ),
)
@click.pass_context
async def workflow_doctor(
    ctx: click.Context,
    as_json: bool,
    stale_pending_sec: int,
    stale_running_min: int,
    stale_ready_min: int,
) -> None:
    """Run preflight health checks on the workflow setup.

    Verifies that the storage account, workflow tables, and completion
    queue are reachable, and surfaces likely problems such as a missing
    coordinator or dead workers by inspecting workflow / task age.

    Exits non-zero if any check fails.
    """
    checks: _DoctorChecks = []
    queues_account = _doctor_check_config(ctx, checks)

    client = await _doctor_connect_store(ctx, checks)
    if client is not None:
        async with client:
            workflows = await _doctor_check_store(ctx, client, checks)
            if workflows is not None:
                await _doctor_check_completion_queue(ctx, queues_account, checks)
                _doctor_check_stuck_pending(workflows, stale_pending_sec, checks)
                await _doctor_check_stuck_running(client, workflows, stale_running_min, checks)
                await _doctor_check_stuck_ready(client, workflows, stale_ready_min, checks)

    _doctor_render(checks, as_json)
    if any(c["status"] == "fail" for c in checks):
        ctx.exit(1)


# --- Helpers --------------------------------------------------------------

_DoctorChecks = list[dict[str, str | None]]


def _doctor_record(
    checks: _DoctorChecks,
    name: str,
    status: str,
    message: str,
    hint: str | None = None,
) -> None:
    checks.append({"name": name, "status": status, "message": message, "hint": hint})


def _doctor_redact(value: str) -> str:
    """Mask anything that looks like a key in connection strings."""
    for pat in ("AccountKey=", "SharedAccessSignature="):
        idx = value.find(pat)
        if idx >= 0:
            return f"{value[: idx + len(pat)]}***"
    return value


def _doctor_check_config(ctx: click.Context, checks: _DoctorChecks) -> str:
    """Check 1: Workflow env vars. Returns the queues_account (or "")."""
    from ai4s.jobq.workflow.env import (
        BLOBS_ENV,
        QUEUES_ENV,
        WORKFLOW_ENV,
        WorkflowEnv,
        WorkflowEnvError,
    )

    storage = ctx.obj["storage"]
    prefix = ctx.obj["prefix"]
    if not (storage and prefix):
        _doctor_record(
            checks,
            "Workflow config",
            "fail",
            "storage or prefix not configured",
            f"Set {WORKFLOW_ENV}=<account>/<prefix> or pass STORAGE/PREFIX positionally.",
        )
        return ""
    try:
        env = WorkflowEnv.from_environ(state_account=storage, prefix=prefix)
    except WorkflowEnvError as exc:
        _doctor_record(
            checks,
            "Workflow config",
            "fail",
            str(exc),
            f"Set {WORKFLOW_ENV}=<account>/<prefix> or pass STORAGE/PREFIX positionally.",
        )
        return ""

    details = [f"account={_doctor_redact(env.state_account)}", f"prefix={env.prefix}"]
    for env_name in [WORKFLOW_ENV, QUEUES_ENV, BLOBS_ENV]:
        val = os.environ.get(env_name, "")
        if val:
            details.append(f"{env_name}={_doctor_redact(val)}")
    _doctor_record(checks, "Workflow config", "pass", ", ".join(details))
    return env.queues


async def _doctor_connect_store(ctx: click.Context, checks: _DoctorChecks) -> WorkflowClient | None:
    """Check 2a: Try to construct the workflow client."""
    from ai4s.jobq.workflow.env import WORKFLOW_ENV

    try:
        return await _get_client(ctx)
    except Exception as exc:
        _doctor_record(
            checks,
            "Workflow store",
            "fail",
            f"unreachable: {exc}",
            f"Check {WORKFLOW_ENV} points to your account and you're authenticated (`az login`).",
        )
        return None


async def _doctor_check_store(
    ctx: click.Context,
    client: WorkflowClient,
    checks: _DoctorChecks,
) -> list[WorkflowStatus] | None:
    """Check 2b: List workflows to verify store reachability."""
    from ai4s.jobq.workflow.env import WORKFLOW_ENV

    try:
        workflows = await client.list_workflows()
    except Exception as exc:
        _doctor_record(
            checks,
            "Workflow store",
            "fail",
            f"unreachable: {exc}",
            f"Check {WORKFLOW_ENV} points to your account and you're authenticated (`az login`).",
        )
        return None
    _doctor_record(
        checks,
        "Workflow store",
        "pass",
        f"reachable (prefix={ctx.obj['prefix']}, {len(workflows)} workflow(s))",
    )
    return workflows


async def _doctor_check_completion_queue(
    ctx: click.Context, queues_account: str, checks: _DoctorChecks
) -> None:
    """Check 3: Completion queue reachable."""
    if not queues_account:
        return
    from ai4s.jobq.workflow._queues import open_jobq
    from ai4s.jobq.workflow.env import QUEUES_ENV, WORKFLOW_ENV
    from ai4s.jobq.workflow.ids import completion_queue_name

    cq_name = completion_queue_name(ctx.obj["prefix"])
    try:
        async with open_jobq(cq_name, queues_account) as _q:
            _doctor_record(
                checks,
                "Completion queue",
                "pass",
                f"reachable: {cq_name} on {_doctor_redact(queues_account)}",
            )
    except Exception as exc:
        _doctor_record(
            checks,
            "Completion queue",
            "fail",
            f"unreachable: {exc}",
            f"Workers post completions here; the coordinator reads from it. "
            f"Check the queue backend ({QUEUES_ENV} override or the "
            f"{WORKFLOW_ENV} account) is reachable.",
        )


def _doctor_check_stuck_pending(
    workflows: list[WorkflowStatus], stale_pending_sec: int, checks: _DoctorChecks
) -> None:
    """Check 4: Workflows stuck in pending."""
    from datetime import timezone as _tz

    now = datetime.now(_tz.utc)
    stale_pending = [
        wf
        for wf in workflows
        if wf.status == WorkflowState.PENDING
        and (now - wf.updated_at).total_seconds() > stale_pending_sec
    ]
    if not stale_pending:
        _doctor_record(
            checks,
            "Pending workflows",
            "pass",
            f"none stuck (>{stale_pending_sec}s in 'pending')",
        )
    else:
        preview = ", ".join(wf.workflow_id for wf in stale_pending[:3])
        more = f" (+{len(stale_pending) - 3} more)" if len(stale_pending) > 3 else ""
        _doctor_record(
            checks,
            "Pending workflows",
            "warn",
            f"{len(stale_pending)} stuck in 'pending' >{stale_pending_sec}s: {preview}{more}",
            "The coordinator may not be running. Start one with "
            "`ai4s-jobq workflow coordinator` (the sweeper picks up "
            "pending workflows every ~10s).",
        )


async def _doctor_check_stuck_running(
    client: WorkflowClient,
    workflows: list[WorkflowStatus],
    stale_running_min: int,
    checks: _DoctorChecks,
) -> None:
    """Checks 5+6: Stuck running tasks and active queue summary."""
    from datetime import timezone as _tz

    now = datetime.now(_tz.utc)
    running_workflows = [wf for wf in workflows if wf.status == WorkflowState.RUNNING]
    stuck_running: list[tuple[str, str, int]] = []
    queues_in_use: dict[str, int] = {}
    sem = asyncio.Semaphore(20)

    async def _list_running(wf_id: str) -> list[TaskStatus]:
        async with sem:
            return await client.list_tasks(wf_id, status=TaskState.RUNNING)

    running_task_lists = await asyncio.gather(
        *[_list_running(wf.workflow_id) for wf in running_workflows]
    )
    for wf, tasks in zip(running_workflows, running_task_lists, strict=True):
        for t in tasks:
            if t.queue:
                queues_in_use[t.queue] = queues_in_use.get(t.queue, 0) + 1
            if t.started_at is None:
                continue
            age_min = (now - t.started_at).total_seconds() / 60
            if age_min > stale_running_min:
                stuck_running.append((wf.workflow_id, t.name, int(age_min)))

    if not stuck_running:
        _doctor_record(
            checks,
            "Running tasks",
            "pass",
            f"none stuck (>{stale_running_min}m running)",
        )
    else:
        preview = ", ".join(f"{wid}/{name} ({age}m)" for wid, name, age in stuck_running[:3])
        more = f" (+{len(stuck_running) - 3} more)" if len(stuck_running) > 3 else ""
        _doctor_record(
            checks,
            "Running tasks",
            "warn",
            f"{len(stuck_running)} task(s) running >{stale_running_min}m: {preview}{more}",
            "Worker(s) may have died. Check worker logs and verify "
            "they're still consuming from the affected queues. Tasks "
            "will be re-queued when the message visibility timeout expires.",
        )

    if queues_in_use:
        breakdown = ", ".join(f"{q}: {n}" for q, n in sorted(queues_in_use.items()))
        _doctor_record(checks, "Active queues", "info", breakdown)


async def _doctor_check_stuck_ready(
    client: WorkflowClient,
    workflows: list[WorkflowStatus],
    stale_ready_min: int,
    checks: _DoctorChecks,
) -> None:
    """Check: Workflows with READY tasks that the coordinator hasn't advanced.

    A workflow whose durable state carries READY tasks but whose
    workflow ``updated_at`` is stale by more than ``stale_ready_min``
    minutes is a strong indicator that the submit/retry crashed after
    flipping a task to READY but before pushing the queue message — or
    that the coordinator's ready-repair sweep is not running.  The
    sweep re-dispatches such workflows automatically once they cross
    the configured age threshold; if they remain stuck past that
    threshold, the coordinator may be down or wedged.
    """
    from datetime import timezone as _tz

    now = datetime.now(_tz.utc)
    threshold = timedelta(minutes=stale_ready_min)
    active = [
        wf
        for wf in workflows
        if wf.status in (WorkflowState.RUNNING, WorkflowState.PENDING)
        and now - wf.updated_at > threshold
    ]
    if not active:
        _doctor_record(
            checks,
            "Ready tasks",
            "pass",
            f"none stuck (>{stale_ready_min}m without coordinator activity)",
        )
        return

    sem = asyncio.Semaphore(20)

    async def _list_ready(wf_id: str) -> list[TaskStatus]:
        async with sem:
            return await client.list_tasks(wf_id, status=TaskState.READY)

    ready_lists = await asyncio.gather(*[_list_ready(wf.workflow_id) for wf in active])
    stuck: list[tuple[str, int, int]] = []
    for wf, tasks in zip(active, ready_lists, strict=True):
        if not tasks:
            continue
        age_min = int((now - wf.updated_at).total_seconds() / 60)
        stuck.append((wf.workflow_id, len(tasks), age_min))

    if not stuck:
        _doctor_record(
            checks,
            "Ready tasks",
            "pass",
            f"none stuck (>{stale_ready_min}m without coordinator activity)",
        )
        return

    preview = ", ".join(f"{wid} ({n} task(s), {age}m)" for wid, n, age in stuck[:3])
    more = f" (+{len(stuck) - 3} more)" if len(stuck) > 3 else ""
    _doctor_record(
        checks,
        "Ready tasks",
        "warn",
        f"{len(stuck)} workflow(s) with stale READY tasks (>{stale_ready_min}m): {preview}{more}",
        "The coordinator's ready-repair sweep re-pushes these once "
        "they cross --ready-repair-threshold-s. If they remain stuck "
        "the coordinator may be down or wedged — verify "
        "`workflow coordinator` is running.",
    )


def _doctor_render(checks: _DoctorChecks, as_json: bool) -> None:
    """Render doctor results to stdout."""
    n_pass = sum(1 for c in checks if c["status"] == "pass")
    n_warn = sum(1 for c in checks if c["status"] == "warn")
    n_fail = sum(1 for c in checks if c["status"] == "fail")

    if as_json:
        click.echo(
            json.dumps(
                {
                    "checks": checks,
                    "summary": {"pass": n_pass, "warn": n_warn, "fail": n_fail},
                },
                indent=2,
            )
        )
    else:
        glyphs = {"pass": "✔", "warn": "⚠", "fail": "✖", "info": "·"}
        styles = {"pass": "green", "warn": "yellow", "fail": "red bold", "info": "dim"}
        from rich.console import Console

        console = Console()
        for c in checks:
            status = str(c["status"])
            glyph = glyphs.get(status, "?")
            style = styles.get(status, "")
            tag = f"[{style}]{glyph}[/{style}]" if style else glyph
            console.print(f"{tag} {c['name']}: {c['message']}")
            if c["hint"]:
                console.print(f"    [dim]→ {c['hint']}[/dim]")
        parts = []
        if n_pass:
            parts.append(f"{n_pass} passed")
        if n_warn:
            parts.append(f"{n_warn} warning(s)")
        if n_fail:
            parts.append(f"{n_fail} error(s)")
        console.print(f"\nDoctor: {', '.join(parts) if parts else 'no checks ran'}.")
