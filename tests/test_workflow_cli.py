# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import signal
import socket
import sys
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from types import SimpleNamespace

import asyncclick as click
import pytest
import yaml
from rich.console import Console

import ai4s.jobq.workflow.cli as workflow_cli
from ai4s.jobq.workflow.cli import workflow_group as workflow_cli_group
from ai4s.jobq.workflow.cli._shared import (
    _build_task_table,
    _format_target,
    _print_target_banner,
    _print_task_table,
    _status_to_dict,
    _styled_status,
    _table_names,
    _task_to_dict,
    workflow_group,
)
from ai4s.jobq.workflow.client import AggregateStatus
from ai4s.jobq.workflow.entities import (
    TaskState,
    TaskStatus,
    WorkflowDefinition,
    WorkflowState,
    WorkflowStatus,
    WorkflowTask,
)
from ai4s.jobq.workflow.env import LEGACY_ENV_VARS

AZURITE_TABLE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def _needs_azurite() -> bool:
    for port in (10001, 10002):
        try:
            sock = socket.create_connection(("127.0.0.1", port), timeout=1)
            sock.close()
        except OSError:
            return True
    return False


skip_without_azurite = pytest.mark.skipif(
    _needs_azurite(),
    reason="Azurite Queue or Table Storage not available on ports 10001/10002",
)


@pytest.fixture(autouse=True)
def clean_workflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "JOBQ_WORKFLOW_FILE",
        "JOBQ_WORKFLOW_PREFIX",
        "JOBQ_WORKFLOW_QUEUES",
        "JOBQ_WORKFLOW_BLOBS",
        *LEGACY_ENV_VARS,
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
async def workflow_prefix() -> str:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.data.tables.aio import TableServiceClient

    prefix = f"Cli{uuid.uuid4().hex[:8]}"
    yield prefix

    wf_table, tasks_table = _table_names(prefix)
    service = TableServiceClient.from_connection_string(AZURITE_TABLE_CONN_STR)
    for table_name in (wf_table, tasks_table):
        table = service.get_table_client(table_name)
        with suppress(ResourceNotFoundError):
            await table.delete_table()
    await service.close()


async def _invoke_workflow(*args: str):
    from asyncclick.testing import CliRunner

    return await CliRunner(mix_stderr=False).invoke(
        workflow_cli_group,
        list(args),
        catch_exceptions=False,
    )


@pytest.mark.asyncio
async def test_track_accepts_local_workflow_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    workflow = tmp_path / "workflow.json"
    workflow.write_text('{"name": "preview", "tasks": []}')
    called: dict[str, object] = {}

    def _run_with_default_queue(*, debug: bool, port: int, open_browser: bool) -> None:
        called.update(debug=debug, port=port, open_browser=open_browser)

    monkeypatch.setitem(
        sys.modules,
        "ai4s.jobq.track.app",
        SimpleNamespace(run_with_default_queue=_run_with_default_queue),
    )
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "account/prefix")

    result = await _invoke_workflow(
        "track", "--workflow-file", str(workflow), "-p", "8765", "--no-open"
    )

    assert result.exit_code == 0, result.output
    assert called == {"debug": False, "port": 8765, "open_browser": False}
    assert os.environ["JOBQ_WORKFLOW_FILE"] == str(workflow)
    assert "JOBQ_WORKFLOW_PREFIX" not in os.environ


def _context(storage: str | None = "acct", prefix: str | None = "pref") -> click.Context:
    from ai4s.jobq.workflow.env import WorkflowEnv

    obj: dict[str, object] = {
        "storage": storage,
        "prefix": prefix,
        "config": None,
        "queues_raw": None,
        "blobs_raw": None,
        "env": None,
    }
    if storage and prefix:
        obj["env"] = WorkflowEnv.from_environ(state_account=storage, prefix=prefix)
    return click.Context(workflow_group, obj=obj)


def _make_task(
    name: str,
    *,
    status: TaskState = TaskState.PENDING,
    depends_on: list[str] | None = None,
    completed_deps: int = 0,
    queue: str | None = None,
    error: str | None = None,
) -> TaskStatus:
    completed_at = (
        datetime(2024, 1, 1, tzinfo=timezone.utc) if status == TaskState.COMPLETED else None
    )
    return TaskStatus(
        name=name,
        status=status,
        depends_on=depends_on or [],
        depended_by=[],
        dep_policy="all",
        completed_deps=completed_deps,
        failed_deps=0,
        queue=queue,
        output_ref=None,
        error=error,
        started_at=None,
        completed_at=completed_at,
        retries_remaining=0,
        task_timeout_s=None,
    )


def _linear_workflow(name: str = "cli-linear") -> WorkflowDefinition:
    return WorkflowDefinition(
        name=name,
        tasks=[
            WorkflowTask(name="extract", kwargs={"cmd": "echo extract"}),
            WorkflowTask(
                name="transform",
                kwargs={"cmd": "echo transform"},
                depends_on=["extract"],
            ),
            WorkflowTask(
                name="load",
                kwargs={"cmd": "echo load"},
                depends_on=["transform"],
            ),
        ],
        default_queue="cli-default",
    )


def _prefixed_workflow(name: str = "cli-prefixed") -> WorkflowDefinition:
    return WorkflowDefinition(
        name=name,
        tasks=[
            WorkflowTask(name="prepare", kwargs={"cmd": "echo prepare"}),
            WorkflowTask(
                name="train-one",
                kwargs={"cmd": "echo one"},
                depends_on=["prepare"],
            ),
            WorkflowTask(
                name="train-two",
                kwargs={"cmd": "echo two"},
                depends_on=["prepare"],
            ),
        ],
        default_queue="cli-default",
    )


def _make_workflow_status(
    workflow_id: str,
    *,
    name: str = "workflow",
    status: WorkflowState = WorkflowState.PENDING,
    total: int = 1,
    completed: int = 0,
    running: int = 0,
    failed: int = 0,
    pending: int | None = None,
    skipped: int = 0,
    default_queue: str = "cli-default",
    queues_used: list[str] | None = None,
    tasks: dict[str, TaskStatus] | None = None,
    error: str | None = None,
) -> WorkflowStatus:
    now = datetime.now(timezone.utc)
    pending_count = total - completed - running - failed - skipped if pending is None else pending
    return WorkflowStatus(
        workflow_id=workflow_id,
        name=name,
        status=status,
        total=total,
        completed=completed,
        running=running,
        failed=failed,
        pending=pending_count,
        skipped=skipped,
        default_queue=default_queue,
        queues_used=queues_used or [default_queue],
        created_at=now,
        updated_at=now,
        error=error,
        tasks=tasks or {},
    )


class _AsyncClientBase:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


def _patch_cli_client(monkeypatch: pytest.MonkeyPatch, client: _AsyncClientBase) -> None:
    async def fake_get_client(_ctx: click.Context) -> _AsyncClientBase:
        return client

    monkeypatch.setattr(workflow_cli, "_get_client", fake_get_client)


def test_shared_status_helpers_and_target_banner(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify that shared workflow CLI helpers format statuses, targets, and the submit banner consistently."""
    ctx = _context()
    assert _styled_status("completed") == "[green]completed[/green]", (
        " styled status(\"completed\") should equal '[green]completed[/green]'"
    )
    assert _styled_status("mystery") == "mystery", (
        " styled status(\"mystery\") should equal 'mystery'"
    )
    assert _format_target(ctx) == "acct/pref", " format target(ctx) should equal 'acct/pref'"
    assert _format_target(_context(None, None)) == "<unset>/<unset>", (
        " format target( context(None, None)) should equal '<unset>/<unset>'"
    )
    assert _table_names("Demo") == ("DemoWorkflows", "DemoWorkflowTasks"), (
        ' table names("Demo") should match the expected values'
    )

    _print_target_banner(ctx, "Submit")
    captured = capsys.readouterr()
    assert "Submit" in captured.err, "the banner should mention the action"
    assert "acct" in captured.err, "the banner should mention the resolved account"
    assert "pref" in captured.err, "the banner should mention the resolved prefix"


def test_config_banner_tags_nondefault_sources() -> None:
    """The compact banner tags flag/env/file sources but not defaults."""
    from ai4s.jobq.workflow.cli._shared import _config_banner
    from ai4s.jobq.workflow.env import WorkflowEnv

    env = WorkflowEnv.from_environ(
        state_account="acct",
        prefix="pref",
        queues="sb://ns",
    )
    banner = _config_banner(env, "Submit")
    assert banner.startswith("▶ Submit ")
    assert "acct(flag)/pref(flag)" in banner
    assert "queues=sb://ns(flag)" in banner
    # blobs defaulted to the state account -> no source tag.
    assert "blobs=acct/jobq-workflow-data" in banner
    assert "blobs=acct/jobq-workflow-data(" not in banner


async def test_config_show_reports_merged_sources(tmp_path, monkeypatch) -> None:
    """`workflow config show --json` reports resolved values and their sources."""
    from asyncclick.testing import CliRunner

    for name in ("JOBQ_WORKFLOW_PREFIX", "JOBQ_WORKFLOW_QUEUES", "JOBQ_WORKFLOW_BLOBS"):
        monkeypatch.delenv(name, raising=False)
    cfg = tmp_path / "jobq.yaml"
    cfg.write_text(
        "connection:\n  storage: fileacct\n  prefix: FileProj\n",
        encoding="utf-8",
    )
    result = await CliRunner(mix_stderr=False).invoke(
        workflow_cli_group,
        ["--config", str(cfg), "config", "show", "--json"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.output)
    assert payload["storage"] == "fileacct"
    assert payload["prefix"] == "FileProj"
    assert payload["sources"]["storage"] == "file"
    assert payload["sources"]["queues"] == "default"
    assert payload["config_path"] == str(cfg)


async def test_config_init_scaffolds_file(tmp_path, monkeypatch) -> None:
    """`workflow config init` writes a jobq.yaml pre-filled from the target."""
    from asyncclick.testing import CliRunner

    out = tmp_path / "jobq.yaml"
    result = await CliRunner(mix_stderr=False).invoke(
        workflow_cli_group,
        ["acct/Proj", "config", "init", "--path", str(out)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.stderr
    assert out.is_file()
    parsed = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert parsed["connection"]["storage"] == "acct"
    assert parsed["connection"]["prefix"] == "Proj"


def test_status_and_task_to_dict_include_optional_fields() -> None:
    """Ensure that workflow and task status serialization preserves optional fields in the CLI JSON payloads."""
    now = datetime.now(timezone.utc)
    task = _make_task(
        "prepare",
        status=TaskState.RUNNING,
        depends_on=["root"],
        completed_deps=1,
        queue="gpu",
        error="task failed",
    )
    status = WorkflowStatus(
        workflow_id="wf-123",
        name="demo",
        status=WorkflowState.FAILED,
        total=2,
        completed=1,
        running=0,
        failed=1,
        pending=0,
        skipped=0,
        default_queue="gpu",
        queues_used=["gpu"],
        created_at=now,
        updated_at=now,
        error="workflow failed",
        tasks={"prepare": task},
    )

    task_dict = _task_to_dict(task)
    assert task_dict["name"] == "prepare", "task name field should equal 'prepare'"
    assert task_dict["status"] == "running", "task status field should equal 'running'"
    assert task_dict["queue"] == "gpu", "task queue field should equal 'gpu'"
    assert task_dict["error"] == "task failed", "task error field should equal 'task failed'"

    status_dict = _status_to_dict(status)
    assert status_dict["workflow_id"] == "wf-123", "workflow id field should equal 'wf-123'"
    assert status_dict["status"] == "failed", "workflow status field should equal 'failed'"
    assert status_dict["error"] == "workflow failed", (
        "workflow error field should equal 'workflow failed'"
    )
    assert status_dict["tasks"]["prepare"]["depends_on"] == ["root"], (
        "serialized task dependencies should match the expected values"
    )


def test_build_task_table_truncates_errors_and_prints_notice(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Check that non-verbose task rendering truncates long error text and prints a truncation notice."""
    long_error = "x" * 41
    tasks = [
        _make_task("extract", status=TaskState.COMPLETED),
        _make_task(
            "transform",
            status=TaskState.FAILED,
            depends_on=["extract"],
            completed_deps=1,
            queue="gpu",
            error=long_error,
        ),
    ]

    table, truncated = _build_task_table(tasks, verbose=False)
    console = Console(record=True, width=120)
    console.print(table)
    rendered = console.export_text()

    assert truncated == 1, "truncated error count should equal 1"
    assert "extract" in rendered, "'extract' should appear in rendered output"
    assert "transform" in rendered, "'transform' should appear in rendered output"
    assert f"{'x' * 37}…" in rendered, "truncated error text should appear in rendered output"

    _print_task_table(tasks, verbose=False)
    captured = capsys.readouterr()
    assert "transform" in captured.out, "'transform' should appear in stdout"
    assert "truncated" in captured.out, "'truncated' should appear in stdout"
    assert "--verbose" in captured.out, "'--verbose' should appear in stdout"


def test_build_task_table_verbose_prints_full_error(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify that verbose task rendering preserves the full error text without a truncation warning."""
    full_error = "full failure details that should stay intact"
    tasks = [
        _make_task(
            "train-one",
            status=TaskState.FAILED,
            depends_on=["prepare"],
            completed_deps=1,
            error=full_error,
        )
    ]

    table, truncated = _build_task_table(tasks, verbose=True)
    console = Console(record=True, width=120)
    console.print(table)
    rendered = console.export_text()

    assert truncated == 0, "truncated error count should equal 0"
    assert full_error in rendered, "full error text should appear in rendered output"

    _print_task_table(tasks, verbose=True)
    captured = capsys.readouterr()
    assert full_error in captured.out, "full error text should appear in stdout"
    assert "truncated" not in captured.out, "'truncated' should not appear in stdout"


async def test_workflow_validate_supports_omitted_storage_prefix(tmp_path) -> None:
    """Verify that the validate command succeeds for a valid workflow file without configured storage or prefix."""
    definition = _linear_workflow(name="yaml-validate")
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml.safe_dump(json.loads(definition.to_json())))

    result = await _invoke_workflow("validate", str(path))

    assert result.exit_code == 0, "command should exit successfully"
    assert "OK" in result.output, "'OK' should appear in command output"
    assert str(path) in result.output, "str(path) should appear in command output"


async def test_workflow_validate_reports_invalid_definition(tmp_path) -> None:
    """Ensure that the validate command reports malformed workflow definitions with a non-zero exit code."""
    path = tmp_path / "invalid.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "invalid",
                "tasks": [
                    {"name": "dup", "kwargs": {}},
                    {"name": "dup", "kwargs": {}},
                ],
            }
        )
    )

    result = await _invoke_workflow("validate", str(path))

    assert result.exit_code == 1, "command should exit with an error"
    assert "FAIL" in result.stderr, "'FAIL' should appear in stderr"
    assert "Duplicate task name" in result.stderr, "'Duplicate task name' should appear in stderr"


async def test_workflow_list_requires_configuration() -> None:
    """Verify that the list command exits with a usage error when workflow storage is not configured."""
    result = await _invoke_workflow("list")

    assert result.exit_code == 2, "command should exit with a usage error"
    combined = (result.output or "") + (result.stderr or "")
    assert "Workflow storage account and prefix not configured" in combined, (
        "'Workflow storage account and prefix not configured' should appear in combined CLI output"
    )


async def test_workflow_list_rejects_invalid_workflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that the list command rejects a malformed JOBQ_WORKFLOW_PREFIX environment value."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "not-a-storage-prefix")

    result = await _invoke_workflow("list")

    assert result.exit_code == 2, "command should exit with a usage error"
    combined = (result.output or "") + (result.stderr or "")
    assert "Missing prefix" in combined, "'Missing prefix' should appear in combined CLI output"


@skip_without_azurite
async def test_workflow_submit_single_status_and_tasks(
    workflow_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Verify that the workflow CLI and WorkflowClient submit one definition and expose matching status and task views against Azurite."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", f"devstoreaccount1/{workflow_prefix}")
    definition = _linear_workflow(name="single-submit")
    path = tmp_path / "single.json"
    path.write_text(definition.to_json())

    result = await _invoke_workflow("submit", str(path), "--id", "wf-cli-single")
    assert result.exit_code == 0, "command should exit successfully"
    assert result.output.strip() == "wf-cli-single", "command output should equal 'wf-cli-single'"
    assert 'Submitted "single-submit"' in result.stderr, (
        "'Submitted \"single-submit\"' should appear in stderr"
    )
    assert f"to devstoreaccount1/{workflow_prefix}" in result.stderr, (
        'f"to devstoreaccount1/{workflow prefix}" should appear in stderr'
    )

    status_result = await _invoke_workflow("status", "wf-cli-single", "--json")
    assert status_result.exit_code == 0, "command should exit successfully"
    status = json.loads(status_result.output)
    assert status["workflow_id"] == "wf-cli-single", (
        "status[\"workflow id\"] should equal 'wf-cli-single'"
    )
    # The new client dispatches root tasks during submit(), so the
    # workflow lands in RUNNING immediately rather than waiting for the
    # coordinator to pick it up out of PENDING.
    assert status["status"] in {"running", "pending"}, (
        f"status['status'] should be 'running' or 'pending', got {status['status']!r}"
    )
    assert status["total"] == 3, 'status["total"] should equal 3'
    assert set(status["tasks"]) == {"extract", "transform", "load"}, (
        'set(status["tasks"]) should match the expected values'
    )

    tasks_result = await _invoke_workflow("tasks", "wf-cli-single", "--json")
    assert tasks_result.exit_code == 0, "command should exit successfully"
    tasks = json.loads(tasks_result.output)
    assert [task["name"] for task in tasks] == ["extract", "load", "transform"] or {
        task["name"] for task in tasks
    } == {"extract", "transform", "load"}, "task listing should contain the submitted task names"


@skip_without_azurite
async def test_workflow_submit_batch_and_list_tasks_global(
    workflow_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Ensure that the workflow CLI and WorkflowClient submit multiple definitions and list all submitted tasks against Azurite."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", f"devstoreaccount1/{workflow_prefix}")
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(_linear_workflow(name="batch-one").to_json())
    second.write_text(_prefixed_workflow(name="batch-two").to_json())

    result = await _invoke_workflow("submit", str(first), str(second))

    assert result.exit_code == 0, "command should exit successfully"
    workflow_ids = [line for line in result.output.splitlines() if line.strip()]
    assert len(workflow_ids) == 2, "workflow ids should contain 2 items"
    assert "Submit" in result.stderr, "the banner should mention the action"
    assert f"devstoreaccount1(env)/{workflow_prefix}" in result.stderr, (
        "the compact config banner should show the resolved submit target"
    )
    assert "Submitted 2/2 workflows" in result.stderr, (
        "'Submitted 2/2 workflows' should appear in stderr"
    )

    tasks_result = await _invoke_workflow("tasks", "--json", "--limit", "0")
    assert tasks_result.exit_code == 0, "command should exit successfully"
    tasks = json.loads(tasks_result.output)
    assert len(tasks) == 6, "tasks should contain 6 items"
    assert {task["name"] for task in tasks} == {
        "extract",
        "transform",
        "load",
        "prepare",
        "train-one",
        "train-two",
    }, '{task["name"] for task in tasks} should match the expected values'


@skip_without_azurite
async def test_workflow_submit_rejects_custom_id_for_batch(
    workflow_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Verify that the submit command rejects a custom workflow ID when multiple definition files are provided."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", f"devstoreaccount1/{workflow_prefix}")
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(_linear_workflow(name="first-batch").to_json())
    second.write_text(_linear_workflow(name="second-batch").to_json())

    result = await _invoke_workflow("submit", str(first), str(second), "--id", "wf-custom")

    assert result.exit_code == 2, "command should exit with a usage error"
    combined = (result.output or "") + (result.stderr or "")
    assert "--id cannot be used with multiple files" in combined, (
        "'--id cannot be used with multiple files' should appear in combined CLI output"
    )


async def test_workflow_submit_requires_input_when_stdin_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check that the submit command errors when no files are provided and stdin contains only blank input."""
    monkeypatch.setattr(sys, "stdin", io.StringIO("\n\n"))

    result = await _invoke_workflow("submit")

    assert result.exit_code == 2, "command should exit with a usage error"
    combined = (result.output or "") + (result.stderr or "")
    assert "No definition files provided" in combined, (
        "'No definition files provided' should appear in combined CLI output"
    )


@skip_without_azurite
async def test_workflow_submit_applies_max_fan_in(
    workflow_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Ensure that submit applies max fan-in sequentialization to every workflow definition before submission."""
    import ai4s.jobq.workflow.transforms as workflow_transforms

    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", f"devstoreaccount1/{workflow_prefix}")
    calls: list[tuple[str, int]] = []

    def fake_sequentialize(definition: WorkflowDefinition, max_fan_in: int) -> WorkflowDefinition:
        calls.append((definition.name, max_fan_in))
        return definition

    monkeypatch.setattr(workflow_transforms, "sequentialize_fan_in", fake_sequentialize)

    single = tmp_path / "single.yaml"
    first = tmp_path / "first.yaml"
    second = tmp_path / "second.yaml"
    single.write_text(_linear_workflow(name="fanin-single").to_json())
    first.write_text(_linear_workflow(name="fanin-first").to_json())
    second.write_text(_prefixed_workflow(name="fanin-second").to_json())

    single_result = await _invoke_workflow(
        "submit",
        str(single),
        "--id",
        "wf-fanin-single",
        "--max-fan-in",
        "1",
    )
    batch_result = await _invoke_workflow(
        "submit",
        str(first),
        str(second),
        "--max-fan-in",
        "2",
    )

    assert single_result.exit_code == 0, "command should exit successfully"
    assert batch_result.exit_code == 0, "command should exit successfully"
    assert calls == [
        ("fanin-single", 1),
        ("fanin-first", 2),
        ("fanin-second", 2),
    ], "max fan-in should be applied to every submitted workflow"


def test_render_status_includes_skipped_tasks_and_table() -> None:
    """Verify that rendered workflow status output includes skipped counts and task table details."""
    status = _make_workflow_status(
        "wf-render",
        name="render-demo",
        status=WorkflowState.RUNNING,
        total=3,
        completed=1,
        running=1,
        pending=0,
        skipped=1,
        tasks={
            "extract": _make_task("extract", status=TaskState.COMPLETED),
            "transform": _make_task(
                "transform",
                status=TaskState.RUNNING,
                depends_on=["extract"],
                completed_deps=1,
                queue="gpu",
            ),
        },
    )

    console = Console(record=True, width=120)
    console.print(workflow_cli._render_status(status, verbose=False))
    rendered = console.export_text()

    assert "Workflow wf-render" in rendered, "'Workflow wf-render' should appear in rendered output"
    assert "render-demo" in rendered, "'render-demo' should appear in rendered output"
    assert "1 skipped" in rendered, "'1 skipped' should appear in rendered output"
    assert "transform" in rendered, "'transform' should appear in rendered output"
    assert "gpu" in rendered, "'gpu' should appear in rendered output"


@skip_without_azurite
async def test_workflow_status_renders_text_output(
    workflow_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Verify that the workflow CLI and WorkflowClient render readable status output for an Azurite-backed workflow."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", f"devstoreaccount1/{workflow_prefix}")
    path = tmp_path / "status.json"
    path.write_text(_linear_workflow(name="status-text").to_json())

    submit_result = await _invoke_workflow("submit", str(path), "--id", "wf-status-text")
    assert submit_result.exit_code == 0, "command should exit successfully"

    result = await _invoke_workflow("status", "wf-status-text")

    assert result.exit_code == 0, "command should exit successfully"
    assert "Workflow wf-status-text" in result.output, (
        "'Workflow wf-status-text' should appear in command output"
    )
    assert "Name:" in result.output, "'Name:' should appear in command output"
    assert "extract" in result.output, "'extract' should appear in command output"


async def test_workflow_watch_handles_refresh_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that workflow watch reports refresh failures and keeps polling until the workflow reaches a terminal state."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")
    statuses: list[WorkflowStatus | Exception] = [
        _make_workflow_status("wf-watch", status=WorkflowState.PENDING, total=2, pending=2),
        RuntimeError("boom"),
        _make_workflow_status(
            "wf-watch",
            status=WorkflowState.COMPLETED,
            total=2,
            completed=2,
            pending=0,
        ),
    ]

    class FakeClient(_AsyncClientBase):
        async def status(self, workflow_id: str) -> WorkflowStatus:
            assert workflow_id == "wf-watch"
            current = statuses.pop(0)
            if isinstance(current, Exception):
                raise current
            return current

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("watch", "wf-watch", "--interval", "0.01")

    assert result.exit_code == 0, "command should exit successfully"
    assert "Error refreshing: boom" in result.output, (
        "'Error refreshing: boom' should appear in command output"
    )
    assert "Workflow wf-watch reached terminal state: completed." in result.output, (
        "'Workflow wf-watch reached terminal state: completed.' should appear in command output"
    )


async def test_workflow_logs_prints_ready_to_paste_query() -> None:
    """Verify that the logs command prints a ready-to-paste KQL query for the selected workflow task."""
    result = await _invoke_workflow(
        "logs",
        "wf-logs",
        "task-a",
        "--since",
        "30m",
        "--table",
        "custom_table",
    )

    assert result.exit_code == 0, "command should exit successfully"
    assert "Paste the following KQL query" in result.stderr, (
        "'Paste the following KQL query' should appear in stderr"
    )
    assert "APPLICATIONINSIGHTS_CONNECTION_STRING" in result.stderr, (
        "'APPLICATIONINSIGHTS_CONNECTION_STRING' should appear in stderr"
    )
    assert result.output.startswith("custom_table"), (
        "command output should start with 'custom_table'"
    )
    assert 'customDimensions.workflow_id == "wf-logs"' in result.output, (
        "'customDimensions.workflow_id == \"wf-logs\"' should appear in command output"
    )
    assert 'customDimensions.task_name == "task-a"' in result.output, (
        "'customDimensions.task_name == \"task-a\"' should appear in command output"
    )
    assert "| where timestamp > ago(30m)" in result.output, (
        "'| where timestamp > ago(30m)' should appear in command output"
    )


async def test_workflow_list_text_handles_empty_and_non_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that the list command renders sensible text output for both empty and populated workflow stores."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")
    workflows = [
        [],
        [
            _make_workflow_status(
                "wf-one",
                name="list-one",
                status=WorkflowState.RUNNING,
                total=3,
                completed=1,
                running=1,
                failed=1,
                pending=0,
                skipped=1,
            )
        ],
    ]

    class FakeClient(_AsyncClientBase):
        async def list_workflows(self, status: str | None = None) -> list[WorkflowStatus]:
            assert status is None
            return workflows.pop(0)

    _patch_cli_client(monkeypatch, FakeClient())

    empty_result = await _invoke_workflow("list")
    table_result = await _invoke_workflow("list")

    assert empty_result.exit_code == 0, "command should exit successfully"
    assert empty_result.output.strip() == "No workflows found.", (
        "command output should equal 'No workflows found.'"
    )
    assert table_result.exit_code == 0, "command should exit successfully"
    assert "Workflows" in table_result.output, "'Workflows' should appear in command output"
    assert "wf-one" in table_result.output, "'wf-one' should appear in command output"
    assert "1/3 done, 1 fail, 1 skip" in table_result.output, (
        "'1/3 done, 1 fail, 1 skip' should appear in command output"
    )


async def test_workflow_tasks_text_and_limit_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that the tasks command handles empty results, global aggregation, and limit truncation in text and JSON modes."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")
    task_map = {
        "wf-a": [
            _make_task("extract", status=TaskState.COMPLETED),
            _make_task("transform", status=TaskState.RUNNING, depends_on=["extract"]),
        ],
        "wf-empty": [],
        "wf-none": [],
    }
    workflows = [
        _make_workflow_status("wf-a", name="wf-a", total=2, completed=1, running=1, pending=0),
        _make_workflow_status("wf-empty", name="wf-empty"),
    ]

    class FakeClient(_AsyncClientBase):
        async def list_tasks(
            self,
            workflow_id: str,
            *,
            status: str | None = None,
            queue: str | None = None,
            name_prefix: str | None = None,
        ) -> list[TaskStatus]:
            assert status is None
            assert queue is None
            assert name_prefix is None
            return list(task_map[workflow_id])

        async def list_workflows(self, *, status: str | None = None) -> list[WorkflowStatus]:
            assert status is None
            return workflows

    _patch_cli_client(monkeypatch, FakeClient())

    single_empty = await _invoke_workflow("tasks", "wf-empty")
    limited = await _invoke_workflow("tasks", "--limit", "1")
    limited_json = await _invoke_workflow("tasks", "--json", "--limit", "1")

    class EmptyClient(_AsyncClientBase):
        async def list_tasks(
            self,
            workflow_id: str,
            *,
            status: str | None = None,
            queue: str | None = None,
            name_prefix: str | None = None,
        ) -> list[TaskStatus]:
            return []

        async def list_workflows(self, *, status: str | None = None) -> list[WorkflowStatus]:
            assert status is None
            return [_make_workflow_status("wf-none", name="wf-none")]

    _patch_cli_client(monkeypatch, EmptyClient())
    global_empty = await _invoke_workflow("tasks")

    assert single_empty.exit_code == 0, "command should exit successfully"
    assert single_empty.output.strip() == "No tasks found.", (
        "command output should equal 'No tasks found.'"
    )
    assert limited.exit_code == 0, "command should exit successfully"
    assert "extract" in limited.output, "'extract' should appear in command output"
    assert "showing first 1" in limited.output, "'showing first 1' should appear in command output"
    assert limited_json.exit_code == 0, "command should exit successfully"
    assert "extract" in limited_json.output, "'extract' should appear in JSON output"
    assert global_empty.exit_code == 0, "command should exit successfully"
    assert global_empty.output.strip() == "No tasks found.", (
        "command output should equal 'No tasks found.'"
    )


async def test_workflow_tasks_multiple_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Passing several workflow IDs lists tasks for each and tags JSON output."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")
    task_map = {
        "wf-a": [_make_task("extract", status=TaskState.COMPLETED)],
        "wf-b": [_make_task("train", status=TaskState.RUNNING)],
    }

    class FakeClient(_AsyncClientBase):
        async def list_tasks(
            self,
            workflow_id: str,
            *,
            status: str | None = None,
            queue: str | None = None,
            name_prefix: str | None = None,
        ) -> list[TaskStatus]:
            return list(task_map[workflow_id])

        async def list_workflows(self, *, status: str | None = None) -> list[WorkflowStatus]:
            raise AssertionError("explicit IDs should not trigger a global scan")

    _patch_cli_client(monkeypatch, FakeClient())

    text_result = await _invoke_workflow("tasks", "wf-a", "wf-b")
    json_result = await _invoke_workflow("tasks", "wf-a", "wf-b", "--json")

    assert text_result.exit_code == 0, "command should exit successfully"
    assert "wf-a:" in text_result.output, "the per-workflow header for wf-a should appear"
    assert "wf-b:" in text_result.output, "the per-workflow header for wf-b should appear"
    assert "extract" in text_result.output, "wf-a's task should appear"
    assert "train" in text_result.output, "wf-b's task should appear"

    assert json_result.exit_code == 0, "command should exit successfully"
    payload = json.loads(json_result.output)
    assert {entry["workflow_id"] for entry in payload} == {"wf-a", "wf-b"}, (
        "each JSON entry should be tagged with its workflow_id"
    )
    assert len(payload) == 2, "both workflows' tasks should be present"


async def test_workflow_retry_renders_reset_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that the retry command prints the reset summary returned by the workflow client."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")

    class FakeClient(_AsyncClientBase):
        async def retry(self, workflow_id: str) -> dict[str, int]:
            assert workflow_id == "wf-retry"
            return {
                "reset": 3,
                "now_ready": 2,
                "still_pending": 1,
            }

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("retry", "wf-retry")

    assert result.exit_code == 0, "command should exit successfully"
    assert (
        "Workflow wf-retry in acct/pref: reset 3 task(s) → "
        "2 now ready, 1 still pending." in result.output
    ), "retry output should summarize the reset results"


async def test_workflow_retry_handles_empty_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure that retry prints a no-op message when no tasks are eligible."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")

    class FakeClient(_AsyncClientBase):
        async def retry(self, workflow_id: str) -> dict[str, int]:
            return {"reset": 0, "now_ready": 0, "still_pending": 0}

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("retry", "wf-noop")

    assert result.exit_code == 0, "command should exit successfully"
    assert "no tasks to retry" in result.output, (
        "no-op retry message should appear in command output"
    )


async def test_workflow_cancel_text_says_cancellation_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancel command must say 'Cancellation requested', not 'Cancelled'."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")

    class FakeClient(_AsyncClientBase):
        async def cancel(self, workflow_id: str) -> None:
            assert workflow_id == "wf-cancel"

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("cancel", "wf-cancel")

    assert result.exit_code == 0, "command should exit successfully"
    assert "Cancellation requested" in result.output, (
        "cancel output should say 'Cancellation requested', not 'Cancelled'"
    )
    assert "Cancelled workflow" not in result.output, (
        "cancel output must not imply the workflow is already terminal"
    )


async def test_workflow_cancel_json_uses_cancel_requested_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancel --json output must use 'cancel_requested' key, not 'cancelled'."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")

    class FakeClient(_AsyncClientBase):
        async def cancel(self, workflow_id: str) -> None:
            pass

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("cancel", "--json", "wf-cancel-json")

    assert result.exit_code == 0, "command should exit successfully"
    data = json.loads(result.output)
    assert data.get("cancel_requested") is True, "JSON output must contain 'cancel_requested: true'"
    assert "cancelled" not in data, "JSON output must not contain the misleading 'cancelled' key"
    assert data.get("workflow_id") == "wf-cancel-json"


async def test_workflow_list_shows_cancelling_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Workflow list must display 'cancelling' for workflows awaiting worker shutdown."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")

    class FakeClient(_AsyncClientBase):
        async def list_workflows(self, status: str | None = None) -> list[WorkflowStatus]:
            return [
                _make_workflow_status(
                    "wf-cancelling",
                    name="cancelling-wf",
                    status=WorkflowState.CANCELLING,
                    total=3,
                    running=1,
                    completed=0,
                    failed=0,
                    pending=0,
                )
            ]

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("list")

    assert result.exit_code == 0, "command should exit successfully"
    assert "cancelling" in result.output, (
        "'cancelling' should appear in list output for a workflow with CANCELLING status"
    )


async def test_workflow_list_styled_status_cancelling() -> None:
    """cancelling status must be styled differently from running and cancelled."""
    from ai4s.jobq.workflow.cli._shared import _styled_status

    styled_running = _styled_status("running")
    styled_cancelled = _styled_status("cancelled")
    styled_cancelling = _styled_status("cancelling")

    assert styled_cancelling != styled_running, (
        "'cancelling' should have a different style from 'running'"
    )
    assert styled_cancelling != styled_cancelled, (
        "'cancelling' should have a different style from 'cancelled'"
    )
    assert "cancelling" in styled_cancelling, (
        "the status value 'cancelling' must appear in the styled output"
    )


async def test_workflow_summary_renders_text_output(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that the summary command renders workflow and task aggregates in text mode."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "acct/pref")

    class FakeClient(_AsyncClientBase):
        async def summary(self) -> AggregateStatus:
            return AggregateStatus(
                workflows={"pending": 2, "running": 1},
                total_tasks=10,
                completed_tasks=4,
                running_tasks=3,
                failed_tasks=2,
                pending_tasks=0,
                skipped_tasks=1,
            )

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("summary")

    assert result.exit_code == 0, "command should exit successfully"
    assert "Workflows" in result.output, "'Workflows' should appear in command output"
    assert "Tasks" in result.output, "'Tasks' should appear in command output"
    assert "1 skipped" in result.output, "'1 skipped' should appear in command output"
    assert "40% complete — 10 total tasks" in result.output, (
        "completion percentage should appear in command output"
    )


def test_confirm_purge_previews_queues_and_prompt(capsys: pytest.CaptureFixture[str]) -> None:
    """Check that purge confirmation previews discovered queues and uses the destructive confirmation prompt."""
    prompts: list[str] = []

    def fake_confirm(message: str, *, abort: bool) -> None:
        assert abort is True
        prompts.append(message)

    original_confirm = click.confirm
    click.confirm = fake_confirm
    try:
        workflow_cli._confirm_purge(
            storage="acct",
            wf_table="PrefWorkflows",
            task_table="PrefWorkflowTasks",
            purge_all=False,
            drain_queues=True,
            queues_account="queue-acct",
            discovered_queues=[f"queue-{index}" for index in range(8)],
        )
        workflow_cli._confirm_purge(
            storage="acct",
            wf_table="PrefWorkflows",
            task_table="PrefWorkflowTasks",
            purge_all=False,
            drain_queues=True,
            queues_account="queue-acct",
            discovered_queues=[],
        )
    finally:
        click.confirm = original_confirm

    captured = capsys.readouterr()
    assert "queue-0, queue-1, queue-2, queue-3, queue-4, queue-5, … (+2 more)" in captured.err, (
        "queue preview should summarize extra discovered queues"
    )
    assert "Queues:          (none discovered)" in captured.err, (
        "empty queue preview should be shown when no queues are discovered"
    )
    assert prompts == [
        "This will permanently delete terminal (completed/failed/cancelled) workflow data "
        "from those tables AND drain those queues. Continue?",
        "This will permanently delete terminal (completed/failed/cancelled) workflow data "
        "from those tables AND drain those queues. Continue?",
    ], "purge confirmation should prompt with the destructive warning twice"


async def test_workflow_purge_reports_drop_tables_and_queue_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that the purge command reports deleted rows, dropped tables, and queue drain failures."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/pref")

    class FakeClient(_AsyncClientBase):
        async def discover_queues(self) -> list[str]:
            return ["alpha", "beta"]

        async def purge(
            self, *, drop_tables: bool, terminal_only: bool = True, on_progress
        ) -> dict[str, int]:
            assert drop_tables is True
            on_progress("workflow rows", 2)
            on_progress("task rows", 5)
            return {"workflows": 2, "tasks": 5}

        async def drain_queues(self, **kwargs) -> dict[str, int]:
            assert kwargs["queues_account"] == "devstoreaccount1"
            assert kwargs["queue_names"] == ["alpha", "beta"]
            kwargs["on_progress"]("alpha", 3)
            kwargs["on_progress"]("beta", -1)
            return {"alpha": 3, "beta": -1}

    _patch_cli_client(monkeypatch, FakeClient())

    result = await _invoke_workflow("purge", "--yes", "--drop-tables", "--drain-queues")

    assert result.exit_code == 0, "command should exit successfully"
    assert "Deleted 2 workflow rows and 5 task rows." in result.output, (
        "deleted row counts should appear in command output"
    )
    assert "Tables dropped (will be recreated on next submit)." in result.output, (
        "table drop summary should appear in command output"
    )
    assert (
        "Drained 1 queue(s) (3 approx message(s)). 1 queue(s) failed — see logs." in result.output
    ), "queue drain summary should appear in command output"


async def test_workflow_coordinator_renders_banner_and_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure that the coordinator CLI builds a Coordinator with the requested runtime options and renders the startup banner."""
    import ai4s.jobq.logging_utils as logging_utils
    import ai4s.jobq.workflow.coordinator as workflow_coordinator
    import ai4s.jobq.workflow.env as workflow_env

    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/pref")
    root_logger = logging.getLogger()
    monkeypatch.setattr(root_logger, "handlers", [])
    setup_calls: list[tuple[str, int, int]] = []
    coordinator_kwargs: dict[str, object] = {}
    ran: list[bool] = []

    def fake_setup_logging(name: str, *, internal_log_level: int, base_log_level: int) -> None:
        setup_calls.append((name, internal_log_level, base_log_level))

    def fake_from_environ(
        cls, *, state_account=None, prefix=None, queues=None, blobs=None, config=None
    ):
        return SimpleNamespace(
            queues="devstoreaccount1",
            state_account=state_account or "devstoreaccount1",
            prefix=prefix or "pref",
            blob_account=state_account or "devstoreaccount1",
            blob_container="jobq-workflow-data",
            config_path=None,
            coordinator={},
            sources={"storage": "env", "prefix": "env", "queues": "default", "blobs": "default"},
        )

    class FakeCoordinator(_AsyncClientBase):
        @classmethod
        async def from_environment(cls, **kwargs):
            coordinator_kwargs.update(kwargs)
            return cls()

        async def run(self) -> None:
            ran.append(True)

        def stop(self) -> None:
            pass

        @property
        def stats(self):
            from ai4s.jobq.workflow.coordinator import _Stats

            return _Stats()

    monkeypatch.setattr(logging_utils, "setup_logging", fake_setup_logging)
    monkeypatch.setattr(workflow_env.WorkflowEnv, "from_environ", classmethod(fake_from_environ))
    monkeypatch.setattr(workflow_coordinator, "Coordinator", FakeCoordinator)

    result = await _invoke_workflow(
        "coordinator",
        "--batch-size",
        "16",
        "--visibility-timeout-s",
        "45",
        "--idle-sleep-s",
        "0.25",
        "--cancel-poll-interval-s",
        "2.0",
        "--flush-retry-limit",
        "5",
        "--ready-sweep-interval-s",
        "30",
        "--ready-repair-threshold-s",
        "120",
    )

    assert result.exit_code == 0, "command should exit successfully"
    assert setup_calls == [("workflow-coordinator", logging.INFO, logging.WARNING)], (
        "logging should be initialized for the coordinator command"
    )
    assert ran == [True], "coordinator run marker should match the expected values"
    coordinator_kwargs.pop("config", None)
    assert coordinator_kwargs == {
        "state_account": "devstoreaccount1",
        "prefix": "pref",
        "batch_size": 16,
        "visibility_timeout_s": 45.0,
        "idle_sleep_s": 0.25,
        "cancel_poll_interval_s": 2.0,
        "flush_retry_limit": 5,
        "ready_sweep_interval_s": 30.0,
        "ready_repair_threshold_s": 120.0,
        "running_timeout_s": None,
        "running_sweep_interval_s": 60.0,
    }, "coordinator should receive the requested runtime options"
    assert "Coordinator target:" in result.stderr, "'Coordinator target:' should appear in stderr"
    assert "devstoreaccount1 (Storage Queue)" in result.stderr, (
        "queue backend description should appear in stderr"
    )
    assert "Batch size:      16" in result.stderr, "'Batch size:      16' should appear in stderr"
    assert "Visibility:      45.0s" in result.stderr, (
        "'Visibility:      45.0s' should appear in stderr"
    )
    assert "Idle sleep:      0.25s" in result.stderr, (
        "'Idle sleep:      0.25s' should appear in stderr"
    )
    assert "Cancel poll:     2.0s" in result.stderr, (
        "'Cancel poll:     2.0s' should appear in stderr"
    )
    assert "Flush retry:     5" in result.stderr, "'Flush retry:     5' should appear in stderr"
    assert "Ready repair:    every 30.0s (threshold 120.0s)" in result.stderr, (
        "ready-repair banner should appear in stderr"
    )
    assert "Running timeout: disabled" in result.stderr, (
        "running-timeout banner should appear in stderr"
    )
    assert "Starting workflow coordinator…" in result.stderr, (
        "startup message should appear in stderr"
    )


@pytest.mark.parametrize("shutdown_signal", [signal.SIGINT, signal.SIGTERM])
async def test_workflow_coordinator_signal_requests_clean_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    shutdown_signal: signal.Signals,
) -> None:
    """SIGINT and SIGTERM request a graceful stop and restore the prior handlers."""
    installed: dict[signal.Signals, object] = {}
    restored: dict[signal.Signals, object] = {}
    previous_handlers = {
        signal.SIGINT: object(),
        signal.SIGTERM: object(),
    }

    def fake_signal(sig: signal.Signals, handler: object) -> object:
        if sig in installed:
            restored[sig] = handler
        else:
            installed[sig] = handler
        return previous_handlers[sig]

    class FakeCoordinator:
        stop_calls = 0

        def stop(self) -> None:
            self.stop_calls += 1

    monkeypatch.setattr(workflow_cli.signal, "signal", fake_signal)
    coord = FakeCoordinator()

    with workflow_cli._coordinator_signal_handlers(coord):
        handler = installed[shutdown_signal]
        assert callable(handler)
        handler(shutdown_signal, None)
        await asyncio.sleep(0)
        assert coord.stop_calls == 1

    assert restored == previous_handlers
    assert f"Received {shutdown_signal.name}; shutting down coordinator…" in capsys.readouterr().err


async def test_workflow_coordinator_rejects_service_bus_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that the coordinator command rejects a Service Bus queue backend."""
    import ai4s.jobq.workflow.env as workflow_env

    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/pref")
    monkeypatch.setattr(
        workflow_env.WorkflowEnv,
        "from_environ",
        classmethod(
            lambda cls, **kwargs: SimpleNamespace(
                queues="sb://namespace",
                state_account=kwargs.get("state_account") or "devstoreaccount1",
                prefix=kwargs.get("prefix") or "pref",
            )
        ),
    )

    result = await _invoke_workflow("coordinator")

    assert result.exit_code == 2, "command should exit with a usage error"
    combined = (result.output or "") + (result.stderr or "")
    assert "Coordinator requires a Storage Queue backend" in combined, (
        "Service Bus rejection message should appear in combined CLI output"
    )


@skip_without_azurite
async def test_workflow_doctor_renders_text_output(
    workflow_prefix: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that the workflow doctor CLI and WorkflowClient render text diagnostics against Azurite."""
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", f"devstoreaccount1/{workflow_prefix}")

    result = await _invoke_workflow("doctor")

    assert result.exit_code == 0, "command should exit successfully"
    assert "Workflow config" in result.output, "'Workflow config' should appear in command output"
    assert "Workflow store" in result.output, "'Workflow store' should appear in command output"
    assert "Completion queue" in result.output, "'Completion queue' should appear in command output"
    assert "Doctor:" in result.output, "'Doctor:' should appear in command output"
