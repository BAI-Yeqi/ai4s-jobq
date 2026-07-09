# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for per-task log custom_dimensions in ShellCommandProcessor.

Verifies that workflow_id / task_name (and any other caller-supplied
dimensions) reach ``log_from_queue`` correctly even when many tasks
run in parallel through the same process pool — the binding lives on
each queue message rather than in any process-wide state.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

import pytest

from ai4s.jobq.work import ShellCommandProcessor


@pytest.mark.asyncio
async def test_dimensions_flow_through_log_records(caplog):
    """Single task: dimensions reach the log record as custom_dimensions."""
    job_id = uuid.uuid4().hex[:8]
    dims = {"workflow_id": "wf-abc", "task_name": "t1"}

    async with ShellCommandProcessor(num_workers=1) as proc:
        with caplog.at_level(logging.INFO, logger=f"task.{job_id}"):
            await proc(cmd="echo hello-world", _job_id=job_id, _log_dimensions=dims)
            # Give log_from_queue a moment to drain.
            await asyncio.sleep(0.5)

    matches = [
        r for r in caplog.records if r.name == f"task.{job_id}" and "hello-world" in r.message
    ]
    assert matches, (
        f"Expected hello-world in task.{job_id}; saw: {[r.message for r in caplog.records]}"
    )
    record = matches[0]
    cdim = getattr(record, "custom_dimensions", None)
    assert cdim is not None, "Record carried no custom_dimensions"
    assert cdim.get("workflow_id") == "wf-abc"
    assert cdim.get("task_name") == "t1"


@pytest.mark.asyncio
async def test_dimensions_isolated_across_parallel_tasks(caplog):
    """Multiple tasks running in parallel each carry their own dimensions.

    Regression guard: an earlier draft used a process-wide registry
    keyed by job_id. Ensure dimensions stay correctly bound to each
    task's own log lines even when other tasks are concurrently
    starting and finishing.
    """
    n_tasks = 6
    job_ids = [uuid.uuid4().hex[:8] for _ in range(n_tasks)]

    async with ShellCommandProcessor(num_workers=4) as proc:
        with caplog.at_level(logging.INFO, logger="task"):
            await asyncio.gather(
                *[
                    proc(
                        cmd=f"echo line-for-{i}",
                        _job_id=job_ids[i],
                        _log_dimensions={"workflow_id": f"wf-{i}", "task_name": f"task-{i}"},
                    )
                    for i in range(n_tasks)
                ]
            )
            # Allow log_from_queue to drain everything.
            await asyncio.sleep(0.8)

    # Every job_id should have at least one matching record carrying *its own* dims.
    for i, job_id in enumerate(job_ids):
        records = [
            r for r in caplog.records if r.name == f"task.{job_id}" and f"line-for-{i}" in r.message
        ]
        assert records, f"No log line for task {i} (job_id={job_id})"
        for r in records:
            cdim = getattr(r, "custom_dimensions", None)
            assert cdim is not None, f"Task {i}: missing custom_dimensions"
            assert cdim.get("workflow_id") == f"wf-{i}", (
                f"Cross-contamination: task {i} got workflow_id={cdim.get('workflow_id')}"
            )
            assert cdim.get("task_name") == f"task-{i}"


@pytest.mark.asyncio
async def test_no_dimensions_without_explicit_arg(caplog):
    """Backwards compat: ShellCommandProcessor without _log_dimensions
    produces records with no custom_dimensions attribute attached."""
    job_id = uuid.uuid4().hex[:8]

    async with ShellCommandProcessor(num_workers=1) as proc:
        with caplog.at_level(logging.INFO, logger=f"task.{job_id}"):
            await proc(cmd="echo plain-output", _job_id=job_id)
            await asyncio.sleep(0.5)

    matches = [
        r for r in caplog.records if r.name == f"task.{job_id}" and "plain-output" in r.message
    ]
    assert matches
    record = matches[0]
    assert getattr(record, "custom_dimensions", None) is None
