# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Live-Azurite tests for the new :mod:`ai4s.jobq.workflow.worker`.

The new worker is a slim drop-in over :class:`ShellCommandProcessor`:

* Reads coordinator-injected metadata kwargs
  (``__workflow_id`` / ``__workflow_task`` / ``__attempt_no`` /
  ``__upstream_output_refs``).
* Publishes a :class:`WorkflowCompletion` on every outcome (success or
  failure) — the coordinator owns retry decisions.
* Stashes large outputs to :class:`WorkflowPersistence`'s output
  container; preserves the legacy file-stash flow.
* Polls cancel via :meth:`WorkflowPersistence.get_cancel_requested`.

These tests exercise the worker against a real Azurite instance and
verify the wire-level contract (completion JSON body shape, attempt
stamping, output stash, cancel termination).
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import textwrap
import uuid
from contextlib import suppress
from typing import AsyncIterator

import pytest

from ai4s.jobq.workflow.entities import (
    WorkflowCompletion,
    WorkflowDefinition,
    WorkflowTask,
)
from ai4s.jobq.workflow.ids import completion_queue_name
from ai4s.jobq.workflow.persistence import WorkflowPersistence

AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def _port_up(port: int) -> bool:
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=1)
        s.close()
    except OSError:
        return False
    return True


def _all_azurite_up() -> bool:
    return all(_port_up(p) for p in (10000, 10001, 10002))


skip_without_azurite = pytest.mark.skipif(
    not _all_azurite_up(),
    reason="Azurite blob/queue/table must be running on 10000/10001/10002",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def worker_prefix() -> str:
    return f"Wrk{uuid.uuid4().hex[:10]}"


@pytest.fixture
def workflow_env(worker_prefix: str, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", f"devstoreaccount1/{worker_prefix}")
    monkeypatch.setenv("JOBQ_WORKFLOW_QUEUES", "devstoreaccount1")
    monkeypatch.setenv("JOBQ_WORKFLOW_BLOBS", "devstoreaccount1/jobq-workflow-data")
    monkeypatch.setenv("JOBQ_CANCEL_POLL_INTERVAL", "1")
    monkeypatch.setenv("JOBQ_COMPLETION_SEND_MAX_ATTEMPTS", "3")
    return worker_prefix


@pytest.fixture
async def persistence(worker_prefix: str) -> AsyncIterator[WorkflowPersistence]:
    p = await WorkflowPersistence.from_connection_string(AZURITE_CONN_STR, prefix=worker_prefix)
    try:
        yield p
    finally:
        with suppress(Exception):
            await p.drop_resources()
        await p.close()


@pytest.fixture
async def submitted_workflow(
    persistence: WorkflowPersistence,
) -> tuple[str, str]:
    """Create a minimal one-task workflow and return (workflow_id, task_queue)."""
    queue_name = f"q-{uuid.uuid4().hex[:8]}"
    defn = WorkflowDefinition(
        name="single",
        tasks=[WorkflowTask(name="solo", kwargs={"foo": "bar"})],
        default_queue=queue_name,
    )
    defn.validate()
    wf_id = uuid.uuid4().hex
    await persistence.submit(wf_id, defn)
    return wf_id, queue_name


async def _drain_completion_queue(prefix: str) -> list[WorkflowCompletion]:
    """Pull every message off the completion queue and decode it."""
    from azure.storage.queue.aio import QueueClient

    queue = completion_queue_name(prefix)
    out: list[WorkflowCompletion] = []
    async with QueueClient.from_connection_string(AZURITE_CONN_STR, queue) as client:
        while True:
            batch = [msg async for msg in client.receive_messages(messages_per_page=32)]
            if not batch:
                break
            for msg in batch:
                task = json.loads(msg.content)
                kwargs = task["kwargs"]
                if isinstance(kwargs, str):
                    kwargs = json.loads(kwargs)
                body = kwargs["__completion_body"]
                out.append(WorkflowCompletion.deserialize(body))
                await client.delete_message(msg)
    return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@skip_without_azurite
async def test_worker_init_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without JOBQ_WORKFLOW_PREFIX the worker constructor must fail loudly."""
    monkeypatch.delenv("JOBQ_WORKFLOW_PREFIX", raising=False)
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    with pytest.raises(ValueError, match="JOBQ_WORKFLOW_PREFIX"):
        WorkflowShellCommandProcessor()


@skip_without_azurite
async def test_worker_passthrough_non_workflow_task(
    workflow_env: str,
) -> None:
    """Tasks without workflow metadata run as a plain shell command."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    async with WorkflowShellCommandProcessor() as proc:
        ret = await proc(cmd="true", _job_id="job-1")
    assert ret == 0


@skip_without_azurite
async def test_worker_publishes_success_completion(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
) -> None:
    """A successful workflow task publishes a success completion with attempt_no."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    async with WorkflowShellCommandProcessor() as proc:
        ret = await proc(
            cmd="true",
            _job_id="job-1",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=1,
            __upstream_output_refs={},
        )
    assert ret == 0

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    c = completions[0]
    assert c.workflow_id == wf_id
    assert c.task_name == "solo"
    assert c.success is True
    assert c.attempt_no == 1
    assert c.error is None
    assert c.output_ref is None  # no $JOBQ_OUTPUT_FILE writes


@skip_without_azurite
async def test_worker_publishes_failure_completion(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
) -> None:
    """A failing shell command still publishes a completion (no broker retry)."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    async with WorkflowShellCommandProcessor() as proc:
        ret = await proc(
            cmd="exit 7",
            _job_id="job-fail",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=2,
            __upstream_output_refs={},
        )
    # Worker always returns 0 — jobq deletes the message, coordinator
    # decides whether to rearm.
    assert ret == 0

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    c = completions[0]
    assert c.success is False
    assert c.attempt_no == 2
    assert c.error is not None
    assert "exit 7" in c.error or "return code 7" in c.error


@skip_without_azurite
async def test_worker_inline_output(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
) -> None:
    """A small JSON output is published inline in the completion message."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    cmd = 'python -c \'import json,os; open(os.environ["JOBQ_OUTPUT_FILE"],"w").write(json.dumps({"score": 0.95}))\''
    async with WorkflowShellCommandProcessor() as proc:
        await proc(
            cmd=cmd,
            _job_id="job-out",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=1,
            __upstream_output_refs={},
        )

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    c = completions[0]
    assert c.success is True
    assert c.output_ref is not None
    assert json.loads(c.output_ref) == {"score": 0.95}


@skip_without_azurite
async def test_worker_large_output_stashed_to_blob(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
    persistence: WorkflowPersistence,
) -> None:
    """An output above the inline threshold is stashed to the output container."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    # 64 KB of "x" forces the blob stash path.
    cmd = (
        "python -c 'import json,os; "
        'open(os.environ["JOBQ_OUTPUT_FILE"],"w").write(json.dumps({"blob":"x"*65536}))\''
    )
    async with WorkflowShellCommandProcessor() as proc:
        await proc(
            cmd=cmd,
            _job_id="job-big",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=1,
            __upstream_output_refs={},
        )

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    c = completions[0]
    assert c.success is True
    assert c.output_ref is not None
    assert c.output_ref.startswith("blob:")
    # Round-trip through persistence.fetch_output (blob exists, JSON decodes).
    decoded = await persistence.fetch_output(c.output_ref)
    assert decoded == {"blob": "x" * 65536}


@skip_without_azurite
async def test_worker_subprocess_sees_workflow_env(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
) -> None:
    """Subprocess inherits JOBQ_WORKFLOW_ID/TASK/ATTEMPT and the upstream refs file."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    cmd = textwrap.dedent(
        """
        python -c '
        import json, os
        out = {
            "wf": os.environ["JOBQ_WORKFLOW_ID"],
            "task": os.environ["JOBQ_WORKFLOW_TASK"],
            "attempt": os.environ["JOBQ_WORKFLOW_ATTEMPT"],
            "refs_file": os.environ["JOBQ_WORKFLOW_UPSTREAM_REFS"],
            "refs": json.load(open(os.environ["JOBQ_WORKFLOW_UPSTREAM_REFS"])),
        }
        open(os.environ["JOBQ_OUTPUT_FILE"],"w").write(json.dumps(out))
        '
        """
    ).strip()
    async with WorkflowShellCommandProcessor() as proc:
        await proc(
            cmd=cmd,
            _job_id="job-env",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=3,
            __upstream_output_refs={"upstream-a": "inline-output", "upstream-b": None},
        )

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    out = json.loads(completions[0].output_ref or "{}")
    assert out["wf"] == wf_id
    assert out["task"] == "solo"
    assert out["attempt"] == "3"
    assert out["refs"] == {"upstream-a": "inline-output", "upstream-b": None}


@skip_without_azurite
async def test_worker_cancellation_terminates_subprocess(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
    persistence: WorkflowPersistence,
) -> None:
    """Setting cancel_requested makes the poller kill the subprocess."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow

    async def _flip_cancel() -> None:
        await asyncio.sleep(1.5)
        await persistence.request_cancel(wf_id)

    flipper = asyncio.create_task(_flip_cancel())

    async with WorkflowShellCommandProcessor() as proc:
        await proc(
            cmd="sleep 30",
            _job_id="job-cancel",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=1,
            __upstream_output_refs={},
        )
    await flipper

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    c = completions[0]
    # The subprocess was killed by the poller, so the command "failed".
    # We don't assert on a particular error string — different signals
    # produce different messages — but the completion must reflect the
    # failure outcome.
    assert c.success is False
    assert c.attempt_no == 1


@skip_without_azurite
async def test_worker_skips_already_cancelled_workflow(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
    persistence: WorkflowPersistence,
) -> None:
    """A workflow cancelled before the task starts must publish a failed completion
    immediately without launching the subprocess."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    await persistence.request_cancel(wf_id)

    async with WorkflowShellCommandProcessor() as proc:
        ret = await proc(
            cmd="sleep 30",
            _job_id="job-pre-cancel",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=1,
            __upstream_output_refs={},
        )

    assert ret == 0

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    c = completions[0]
    assert c.success is False
    assert c.error == "workflow cancelled"
    assert c.attempt_no == 1


@skip_without_azurite
async def test_lazy_workflow_context_resolves_upstreams(
    workflow_env: str,
    persistence: WorkflowPersistence,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: object,
) -> None:
    """``get_workflow_context`` reads refs file and resolves outputs via persistence."""
    from ai4s.jobq.workflow.worker import get_workflow_context

    wf_id = uuid.uuid4().hex

    # Stash an upstream output on blob.
    blob_ref = await persistence.stash_output(wf_id, "upstream", b'{"value": 42}')

    refs_file = os.path.join(str(tmp_path), "refs.json")

    def _write_refs() -> None:
        with open(refs_file, "w") as f:
            json.dump({"upstream": blob_ref, "inline": '{"x": 1}'}, f)

    await asyncio.to_thread(_write_refs)
    monkeypatch.setenv("JOBQ_WORKFLOW_ID", wf_id)
    monkeypatch.setenv("JOBQ_WORKFLOW_TASK", "consumer")
    monkeypatch.setenv("JOBQ_WORKFLOW_UPSTREAM_REFS", refs_file)

    ctx = get_workflow_context()
    async with ctx:
        upstream = await ctx.get_upstream_output("upstream")
        inline = await ctx.get_upstream_output("inline")
        assert upstream == {"value": 42}
        assert inline == {"x": 1}

        with pytest.raises(KeyError, match="not a direct upstream"):
            await ctx.get_upstream_output("does-not-exist")


@skip_without_azurite
async def test_lazy_workflow_context_is_cancelled(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
    persistence: WorkflowPersistence,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ctx.is_cancelled`` reflects the persistence flag in real time."""
    from ai4s.jobq.workflow.worker import get_workflow_context

    wf_id, _ = submitted_workflow
    monkeypatch.setenv("JOBQ_WORKFLOW_ID", wf_id)
    monkeypatch.setenv("JOBQ_WORKFLOW_TASK", "solo")
    # No upstream refs file needed.

    ctx = get_workflow_context()
    async with ctx:
        assert await ctx.is_cancelled() is False
        await persistence.request_cancel(wf_id)
        assert await ctx.is_cancelled() is True


@skip_without_azurite
async def test_get_real_upstream_tasks_walks_merge_chain(
    workflow_env: str,
    persistence: WorkflowPersistence,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``get_real_upstream_tasks`` recovers original roots behind merge nodes."""
    from ai4s.jobq.workflow.transforms import sequentialize_fan_in
    from ai4s.jobq.workflow.worker import get_workflow_context

    queue = f"q-{uuid.uuid4().hex[:8]}"
    roots = [WorkflowTask(name=f"root-{i}", kwargs={}) for i in range(7)]
    leaf = WorkflowTask(name="leaf", kwargs={}, depends_on=[r.name for r in roots])
    defn = WorkflowDefinition(name="fanin", tasks=[*roots, leaf], default_queue=queue)
    transformed = sequentialize_fan_in(defn, max_fan_in=3)
    transformed.validate()
    wf_id = uuid.uuid4().hex
    await persistence.submit(wf_id, transformed)

    monkeypatch.setenv("JOBQ_WORKFLOW_ID", wf_id)
    monkeypatch.setenv("JOBQ_WORKFLOW_TASK", "leaf")

    ctx = get_workflow_context()
    async with ctx:
        names = await ctx.get_real_upstream_tasks()
    assert names == sorted(r.name for r in roots)


@skip_without_azurite
async def test_get_real_upstream_tasks_no_merge_returns_direct_parents(
    workflow_env: str,
    persistence: WorkflowPersistence,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no merge nodes are present the helper returns direct parents."""
    from ai4s.jobq.workflow.worker import get_workflow_context

    queue = f"q-{uuid.uuid4().hex[:8]}"
    defn = WorkflowDefinition(
        name="plain",
        tasks=[
            WorkflowTask(name="a", kwargs={}),
            WorkflowTask(name="b", kwargs={}),
            WorkflowTask(name="leaf", kwargs={}, depends_on=["a", "b"]),
        ],
        default_queue=queue,
    )
    defn.validate()
    wf_id = uuid.uuid4().hex
    await persistence.submit(wf_id, defn)

    monkeypatch.setenv("JOBQ_WORKFLOW_ID", wf_id)
    monkeypatch.setenv("JOBQ_WORKFLOW_TASK", "leaf")

    ctx = get_workflow_context()
    async with ctx:
        names = await ctx.get_real_upstream_tasks()
    assert names == ["a", "b"]


@skip_without_azurite
async def test_lazy_workflow_context_missing_env_raises(
    workflow_env: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_workflow_context() needs JOBQ_WORKFLOW_ID/TASK to be set."""
    monkeypatch.delenv("JOBQ_WORKFLOW_ID", raising=False)
    monkeypatch.delenv("JOBQ_WORKFLOW_TASK", raising=False)
    from ai4s.jobq.workflow.worker import get_workflow_context

    with pytest.raises(ValueError, match="JOBQ_WORKFLOW_ID"):
        get_workflow_context()


@skip_without_azurite
async def test_worker_handles_invalid_output_file(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
) -> None:
    """Bad JSON in the output file does not crash the worker."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    cmd = 'python -c \'import os; open(os.environ["JOBQ_OUTPUT_FILE"],"w").write("not json {{{")\''
    async with WorkflowShellCommandProcessor() as proc:
        await proc(
            cmd=cmd,
            _job_id="job-badjson",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __attempt_no=1,
            __upstream_output_refs={},
        )

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    # Command succeeded (write completed); output_ref is discarded.
    assert completions[0].success is True
    assert completions[0].output_ref is None


@skip_without_azurite
async def test_worker_attempt_no_optional(
    workflow_env: str,
    submitted_workflow: tuple[str, str],
) -> None:
    """A workflow task without __attempt_no still publishes a completion."""
    from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

    wf_id, _ = submitted_workflow
    async with WorkflowShellCommandProcessor() as proc:
        await proc(
            cmd="true",
            _job_id="job-noattempt",
            __workflow_id=wf_id,
            __workflow_task="solo",
            __upstream_output_refs={},
        )

    completions = await _drain_completion_queue(workflow_env)
    assert len(completions) == 1
    c = completions[0]
    assert c.success is True
    assert c.attempt_no is None
