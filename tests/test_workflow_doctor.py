# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


import json
from datetime import datetime, timedelta, timezone

import asyncclick as click
import pytest
from asyncclick.testing import CliRunner

from ai4s.jobq.workflow.cli import _doctor as doctor
from ai4s.jobq.workflow.entities import TaskState, TaskStatus, WorkflowState, WorkflowStatus


def _ctx(*, storage: str | None = "devstoreaccount1", prefix: str | None = "demo") -> click.Context:
    ctx = click.Context(click.Command("doctor"))
    ctx.obj = {"storage": storage, "prefix": prefix}
    return ctx


def _workflow(
    workflow_id: str,
    *,
    status: WorkflowState,
    updated_at: datetime,
) -> WorkflowStatus:
    return WorkflowStatus(
        workflow_id=workflow_id,
        name=workflow_id,
        status=status,
        total=1,
        completed=0,
        running=0,
        failed=0,
        pending=1,
        skipped=0,
        default_queue="default",
        queues_used=[],
        created_at=updated_at,
        updated_at=updated_at,
    )


def _task(
    name: str,
    *,
    started_at: datetime | None,
    queue: str | None = None,
) -> TaskStatus:
    return TaskStatus(
        name=name,
        status=TaskState.RUNNING,
        depends_on=[],
        depended_by=[],
        dep_policy="all",
        completed_deps=0,
        failed_deps=0,
        queue=queue,
        output_ref=None,
        error=None,
        started_at=started_at,
        completed_at=None,
        retries_remaining=0,
        task_timeout_s=None,
    )


def test_doctor_redact_masks_secrets() -> None:
    """Verify that doctor redaction masks secret values while leaving plain values unchanged."""
    assert (
        doctor._doctor_redact("AccountKey=topsecret;QueueEndpoint=http://example")
        == "AccountKey=***"
    ), (
        "doctor. doctor redact(\"AccountKey=topsecret;QueueEndpoint=http://example\") should equal 'AccountKey=***'"
    )
    assert (
        doctor._doctor_redact("SharedAccessSignature=token&sig=1") == "SharedAccessSignature=***"
    ), (
        "doctor. doctor redact(\"SharedAccessSignature=token&sig=1\") should equal 'SharedAccessSignature=***'"
    )
    assert doctor._doctor_redact("plain-value") == "plain-value", (
        "doctor. doctor redact(\"plain-value\") should equal 'plain-value'"
    )


def test_doctor_check_config_handles_missing_invalid_and_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that doctor config checks report missing settings, invalid blob configuration, and sanitized success output."""
    from ai4s.jobq.workflow.env import BLOBS_ENV, QUEUES_ENV

    checks: doctor._DoctorChecks = []
    assert doctor._doctor_check_config(_ctx(storage=None), checks) == "", (
        "doctor. doctor check config( ctx(storage=None), checks) should equal ''"
    )
    assert checks == [
        {
            "name": "Workflow config",
            "status": "fail",
            "message": "storage or prefix not configured",
            "hint": (
                "Set JOBQ_WORKFLOW_PREFIX=<account>/<prefix> or pass STORAGE/PREFIX positionally."
            ),
        }
    ], "doctor checks should match the expected values"

    monkeypatch.setenv(BLOBS_ENV, "missing-container")
    checks = []
    assert doctor._doctor_check_config(_ctx(), checks) == "", (
        "doctor. doctor check config( ctx(), checks) should equal ''"
    )
    assert checks[0]["status"] == "fail", "doctor check status should equal 'fail'"
    assert "Missing container" in str(checks[0]["message"]), (
        "'Missing container' should appear in str(checks[0][\"message\"])"
    )

    monkeypatch.setenv(
        QUEUES_ENV,
        "DefaultEndpointsProtocol=http;AccountKey=supersecret;QueueEndpoint=http://example",
    )
    monkeypatch.setenv(BLOBS_ENV, "blobacct/workflow-data")
    checks = []
    queues_account = doctor._doctor_check_config(_ctx(prefix="Project"), checks)
    assert queues_account.startswith("DefaultEndpointsProtocol=http;AccountKey=supersecret"), (
        "queues account should start with 'DefaultEndpointsProtocol=http;AccountKey=supersecret'"
    )
    assert checks[0]["status"] == "pass", "doctor check status should equal 'pass'"
    assert "account=devstoreaccount1" in str(checks[0]["message"]), (
        "'account=devstoreaccount1' should appear in str(checks[0][\"message\"])"
    )
    assert "prefix=Project" in str(checks[0]["message"]), (
        "'prefix=Project' should appear in str(checks[0][\"message\"])"
    )
    assert "AccountKey=***" in str(checks[0]["message"]), (
        "'AccountKey=***' should appear in str(checks[0][\"message\"])"
    )


async def test_doctor_connect_store_and_check_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that doctor store checks report reachable WorkflowClient state and unreachable-store failures."""

    class FakeClient:
        def __init__(self, workflows: list[WorkflowStatus], *, fail: bool = False) -> None:
            self._workflows = workflows
            self._fail = fail

        async def list_workflows(self) -> list[WorkflowStatus]:
            if self._fail:
                raise RuntimeError("offline")
            return self._workflows

    workflows = [
        _workflow("wf-1", status=WorkflowState.RUNNING, updated_at=datetime.now(timezone.utc))
    ]

    async def fake_get_client(_ctx: click.Context) -> FakeClient:
        return FakeClient(workflows)

    monkeypatch.setattr(doctor, "_get_client", fake_get_client)
    checks: doctor._DoctorChecks = []
    client = await doctor._doctor_connect_store(_ctx(), checks)
    assert isinstance(client, FakeClient), "workflow client should be an instance of FakeClient"
    assert checks == [], "doctor checks should match the expected values"

    listed = await doctor._doctor_check_store(_ctx(prefix="demo"), client, checks)
    assert listed == workflows, "listed workflows should equal workflow list"
    assert checks[-1] == {
        "name": "Workflow store",
        "status": "pass",
        "message": "reachable (prefix=demo, 1 workflow(s))",
        "hint": None,
    }, "store check should report a reachable workflow store"

    async def broken_get_client(_ctx: click.Context) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(doctor, "_get_client", broken_get_client)
    checks = []
    assert await doctor._doctor_connect_store(_ctx(), checks) is None, (
        "await doctor. doctor connect store( ctx(), checks) should be None"
    )
    assert checks[0]["status"] == "fail", "doctor check status should equal 'fail'"
    assert "unreachable: boom" in str(checks[0]["message"]), (
        "'unreachable: boom' should appear in str(checks[0][\"message\"])"
    )

    checks = []
    assert await doctor._doctor_check_store(_ctx(), FakeClient([], fail=True), checks) is None, (
        "await doctor. doctor check store( ctx(), FakeClient([], fail=True), checks) should be None"
    )
    assert checks[0]["status"] == "fail", "doctor check status should equal 'fail'"
    assert "unreachable: offline" in str(checks[0]["message"]), (
        "'unreachable: offline' should appear in str(checks[0][\"message\"])"
    )


async def test_doctor_check_completion_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that doctor completion-queue checks report healthy, skipped, and failing queue connections."""
    from ai4s.jobq.workflow import _queues

    class HealthyQueue:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class BrokenQueue:
        async def __aenter__(self) -> object:
            raise RuntimeError("queue down")

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    monkeypatch.setattr(_queues, "open_jobq", lambda *args, **kwargs: HealthyQueue())
    checks: doctor._DoctorChecks = []
    await doctor._doctor_check_completion_queue(
        _ctx(prefix="demo"),
        "DefaultEndpointsProtocol=http;AccountKey=secret;QueueEndpoint=http://q",
        checks,
    )
    assert checks[0]["status"] == "pass", "doctor check status should equal 'pass'"
    assert "reachable: demo-workflow-completions" in str(checks[0]["message"]), (
        "'reachable: demo-workflow-completions' should appear in str(checks[0][\"message\"])"
    )
    assert "AccountKey=***" in str(checks[0]["message"]), (
        "'AccountKey=***' should appear in str(checks[0][\"message\"])"
    )

    checks = []
    await doctor._doctor_check_completion_queue(_ctx(), "", checks)
    assert checks == [], "doctor checks should match the expected values"

    monkeypatch.setattr(_queues, "open_jobq", lambda *args, **kwargs: BrokenQueue())
    checks = []
    await doctor._doctor_check_completion_queue(_ctx(prefix="demo"), "queues", checks)
    assert checks[0]["status"] == "fail", "doctor check status should equal 'fail'"
    assert "unreachable: queue down" in str(checks[0]["message"]), (
        "'unreachable: queue down' should appear in str(checks[0][\"message\"])"
    )


def test_doctor_check_stuck_pending_warns_and_passes() -> None:
    """Check that stuck-pending detection warns on stale workflows and passes when only fresh workflows remain."""
    now = datetime.now(timezone.utc)
    checks: doctor._DoctorChecks = []
    workflows = [
        _workflow(
            f"wf-{i}",
            status=WorkflowState.PENDING,
            updated_at=now - timedelta(seconds=300 + i),
        )
        for i in range(4)
    ]
    doctor._doctor_check_stuck_pending(workflows, 120, checks)
    assert checks[0]["status"] == "warn", "doctor check status should equal 'warn'"
    assert "wf-0, wf-1, wf-2 (+1 more)" in str(checks[0]["message"]), (
        "'wf-0, wf-1, wf-2 (+1 more)' should appear in str(checks[0][\"message\"])"
    )

    checks = []
    doctor._doctor_check_stuck_pending(
        [_workflow("fresh", status=WorkflowState.PENDING, updated_at=now)],
        120,
        checks,
    )
    assert checks[0] == {
        "name": "Pending workflows",
        "status": "pass",
        "message": "none stuck (>120s in 'pending')",
        "hint": None,
    }, "fresh pending workflows should pass the stuck-pending check"


async def test_doctor_check_stuck_running_warns_and_reports_active_queues() -> None:
    """Verify that stuck-running detection warns on stale tasks and summarizes active queue usage."""
    now = datetime.now(timezone.utc)

    class FakeClient:
        async def list_tasks(self, workflow_id: str, *, status: TaskState) -> list[TaskStatus]:
            assert status == TaskState.RUNNING
            return {
                "wf-1": [
                    _task("task-a", started_at=now - timedelta(minutes=61), queue="queue-a"),
                    _task("task-b", started_at=None, queue="queue-b"),
                ],
                "wf-2": [
                    _task("task-c", started_at=now - timedelta(minutes=75), queue="queue-a"),
                    _task("task-d", started_at=now - timedelta(minutes=80), queue="queue-c"),
                    _task("task-e", started_at=now - timedelta(minutes=81), queue="queue-a"),
                ],
            }[workflow_id]

    workflows = [
        _workflow("wf-1", status=WorkflowState.RUNNING, updated_at=now),
        _workflow("wf-2", status=WorkflowState.RUNNING, updated_at=now),
        _workflow("wf-3", status=WorkflowState.PENDING, updated_at=now),
    ]

    checks: doctor._DoctorChecks = []
    await doctor._doctor_check_stuck_running(FakeClient(), workflows, 60, checks)
    assert checks[0]["status"] == "warn", "doctor check status should equal 'warn'"
    assert "wf-1/task-a (61m), wf-2/task-c (75m), wf-2/task-d (80m) (+1 more)" in str(
        checks[0]["message"]
    ), (
        "'wf-1/task-a (61m), wf-2/task-c (75m), wf-2/task-d (80m) (+1 more)' should appear in str(\n        checks[0][\"message\"]\n    )"
    )
    assert checks[1] == {
        "name": "Active queues",
        "status": "info",
        "message": "queue-a: 3, queue-b: 1, queue-c: 1",
        "hint": None,
    }, "active queue summary should report queue usage counts"


async def test_doctor_check_stuck_running_passes_when_nothing_is_stale() -> None:
    """Ensure that stuck-running detection passes when no running task exceeds the staleness threshold."""
    now = datetime.now(timezone.utc)

    class FakeClient:
        async def list_tasks(self, workflow_id: str, *, status: TaskState) -> list[TaskStatus]:
            assert workflow_id == "wf"
            assert status == TaskState.RUNNING
            return [_task("fresh", started_at=now - timedelta(minutes=5))]

    checks: doctor._DoctorChecks = []
    await doctor._doctor_check_stuck_running(
        FakeClient(),
        [_workflow("wf", status=WorkflowState.RUNNING, updated_at=now)],
        60,
        checks,
    )
    assert checks == [
        {
            "name": "Running tasks",
            "status": "pass",
            "message": "none stuck (>60m running)",
            "hint": None,
        }
    ], "non-stale running tasks should pass the stuck-running check"


def test_doctor_render_json_and_rich(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that doctor rendering produces the expected JSON summary and rich-text output."""
    json_output: list[str] = []
    monkeypatch.setattr(doctor.click, "echo", json_output.append)
    doctor._doctor_render(
        [
            {"name": "a", "status": "pass", "message": "ok", "hint": None},
            {"name": "b", "status": "warn", "message": "warn", "hint": "fix it"},
            {"name": "c", "status": "fail", "message": "bad", "hint": None},
        ],
        as_json=True,
    )
    payload = json.loads(json_output[0])
    assert payload["summary"] == {"pass": 1, "warn": 1, "fail": 1}, (
        "JSON summary should match the expected values"
    )

    printed: list[str] = []

    class FakeConsole:
        def print(self, message: str) -> None:
            printed.append(message)

    import rich.console

    monkeypatch.setattr(rich.console, "Console", FakeConsole)
    doctor._doctor_render(
        [
            {"name": "Workflow store", "status": "pass", "message": "reachable", "hint": None},
            {
                "name": "Running tasks",
                "status": "warn",
                "message": "1 stuck",
                "hint": "Check the workers.",
            },
            {"name": "Completion queue", "status": "fail", "message": "down", "hint": None},
            {"name": "Active queues", "status": "info", "message": "queue-a: 1", "hint": None},
        ],
        as_json=False,
    )
    assert printed[0] == "[green]✔[/green] Workflow store: reachable", (
        "first rendered line should show the passing workflow store check"
    )
    assert printed[1] == "[yellow]⚠[/yellow] Running tasks: 1 stuck", (
        "second rendered line should show the running-task warning"
    )
    assert printed[2] == "    [dim]→ Check the workers.[/dim]", (
        "third rendered line should show the warning hint"
    )
    assert printed[3] == "[red bold]✖[/red bold] Completion queue: down", (
        "fourth rendered line should show the failing completion queue check"
    )
    assert printed[4] == "[dim]·[/dim] Active queues: queue-a: 1", (
        "fifth rendered line should show the active queue summary"
    )
    assert printed[5] == "\nDoctor: 1 passed, 1 warning(s), 1 error(s).", (
        "summary line should report pass, warning, and error counts"
    )

    printed.clear()
    doctor._doctor_render([], as_json=False)
    assert printed[-1] == "\nDoctor: no checks ran.", (
        "final rendered line should equal '\\nDoctor: no checks ran.'"
    )


async def test_workflow_doctor_exits_non_zero_on_failed_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that the workflow doctor CLI exits non-zero when any health check fails."""
    from ai4s.jobq.cli import main

    async def broken_get_client(_ctx: click.Context) -> None:
        raise RuntimeError("store offline")

    monkeypatch.setattr(doctor, "_get_client", broken_get_client)
    result = await CliRunner(mix_stderr=False).invoke(
        main,
        ["workflow", "devstoreaccount1/demo", "doctor", "--json"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1, "command should exit with an error"
    payload = json.loads(result.output)
    assert payload["summary"] == {"pass": 1, "warn": 0, "fail": 1}, (
        "JSON summary should match the expected values"
    )
    assert payload["checks"][1]["name"] == "Workflow store", (
        "failed check name should equal 'Workflow store'"
    )
    assert payload["checks"][1]["status"] == "fail", "failed check status should equal 'fail'"


async def test_doctor_check_stuck_ready_passes_when_workflows_are_fresh() -> None:
    """A workflow updated within the stale-ready threshold is not flagged."""
    now = datetime.now(timezone.utc)

    class FakeClient:
        async def list_tasks(
            self, workflow_id: str, *, status: TaskState
        ) -> list[TaskStatus]:  # pragma: no cover - should not be called
            raise AssertionError("fresh workflows must not trigger a list_tasks call")

    workflows = [
        _workflow("wf-fresh", status=WorkflowState.RUNNING, updated_at=now),
    ]
    checks: doctor._DoctorChecks = []
    await doctor._doctor_check_stuck_ready(FakeClient(), workflows, 5, checks)
    assert checks[0]["status"] == "pass", "fresh workflows should pass"
    assert checks[0]["name"] == "Ready tasks"


async def test_doctor_check_stuck_ready_warns_when_ready_tasks_are_stale() -> None:
    """A RUNNING workflow that hasn't moved in >threshold AND has READY tasks warns."""
    now = datetime.now(timezone.utc)

    def _ready(name: str) -> TaskStatus:
        return TaskStatus(
            name=name,
            status=TaskState.READY,
            depends_on=[],
            depended_by=[],
            dep_policy="all",
            completed_deps=0,
            failed_deps=0,
            queue="q",
            output_ref=None,
            error=None,
            started_at=None,
            completed_at=None,
            retries_remaining=0,
            task_timeout_s=None,
        )

    class FakeClient:
        async def list_tasks(self, workflow_id: str, *, status: TaskState) -> list[TaskStatus]:
            assert status == TaskState.READY
            if workflow_id == "wf-stuck":
                return [_ready("a"), _ready("b")]
            return []

    workflows = [
        _workflow(
            "wf-stuck",
            status=WorkflowState.RUNNING,
            updated_at=now - timedelta(minutes=15),
        ),
        _workflow(
            "wf-no-ready",
            status=WorkflowState.RUNNING,
            updated_at=now - timedelta(minutes=15),
        ),
    ]
    checks: doctor._DoctorChecks = []
    await doctor._doctor_check_stuck_ready(FakeClient(), workflows, 5, checks)
    assert checks[0]["status"] == "warn", "stale READY tasks should warn"
    assert "wf-stuck" in str(checks[0]["message"]), "warning message should name the stuck workflow"
    assert "2 task(s)" in str(checks[0]["message"]), (
        "warning message should report the READY task count"
    )
    assert "wf-no-ready" not in str(checks[0]["message"]), (
        "workflows without READY tasks should not appear in the warning"
    )
