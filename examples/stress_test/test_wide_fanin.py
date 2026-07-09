# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Stress test: 60 000 root tasks fanning into a single leaf.

Validates that the workflow engine can handle extreme fan-in DAGs where
a single merge task depends on tens of thousands of independent roots.
This exercises:

- Batch insertion of large task sets (600+ Table Storage transactions).
- ``definition_json`` omission for large workflows (>60 KB).
- ``depends_on`` truncation for tasks with >~4000 parents.
- ``total_deps`` integer field for dep-policy evaluation.
- ``sequentialize_fan_in`` transform to bound coordinator load.

Usage::

    # Start Azurite Table Storage (port 10002)
    npx azurite-table --skipApiVersionCheck --inMemoryPersistence --tablePort 10002 &

    # Run the test (takes ~1-3 minutes against Azurite)
    pytest examples/stress_test/test_wide_fanin.py -x -v

    # Run against live Azure (requires JOBQ_WORKFLOW_PREFIX=account/prefix):
    JOBQ_WORKFLOW_PREFIX=haschulzbackup/StressTestBig \
        pytest examples/stress_test/test_wide_fanin.py -x -v --run-live
"""

from __future__ import annotations

import os
import uuid

import pytest

from ai4s.jobq.workflow.entities import (
    WorkflowDefinition,
    WorkflowTask,
)
from ai4s.jobq.workflow.transforms import sequentialize_fan_in

# ---------------------------------------------------------------------------
# Azurite Table Storage connection
# ---------------------------------------------------------------------------

AZURITE_TABLE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def _needs_azurite() -> bool:
    try:
        import socket

        s = socket.create_connection(("127.0.0.1", 10002), timeout=1)
        s.close()
    except Exception:
        return True
    return False


skip_without_azurite = pytest.mark.skipif(
    _needs_azurite(),
    reason="Azurite Table Storage not available on port 10002",
)

NUM_ROOTS = 60_000
NUM_ROOTS_LOCAL = 5_000  # Azurite can't sustain 60k at full concurrency


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def store():
    """Create a WorkflowStore with unique table names per test."""
    from ai4s.jobq.workflow.store import WorkflowStore, _table_names
    from azure.data.tables.aio import TableServiceClient

    suffix = uuid.uuid4().hex[:8]
    wf_base, tasks_base = _table_names()
    wf_table = f"{wf_base}{suffix}"
    tasks_table = f"{tasks_base}{suffix}"

    service = TableServiceClient.from_connection_string(AZURITE_TABLE_CONN_STR)
    wf_client = service.get_table_client(wf_table)
    tasks_client = service.get_table_client(tasks_table)
    await wf_client.create_table()
    await tasks_client.create_table()

    s = WorkflowStore(wf_client, tasks_client, service_client=service)
    yield s

    await wf_client.delete_table()
    await tasks_client.delete_table()
    await service.close()


@pytest.fixture
async def live_store():
    """Create a WorkflowStore pointing at the live Azure account.

    Requires JOBQ_WORKFLOW_PREFIX=account/prefix (e.g. haschulzbackup/StressTestBig).
    """
    from ai4s.jobq.workflow.client import WorkflowClient

    wf_spec = os.environ.get("JOBQ_WORKFLOW_PREFIX", "")
    if not wf_spec:
        pytest.skip("JOBQ_WORKFLOW_PREFIX not set")

    client = await WorkflowClient.from_environment()
    async with client:
        yield client._store


# ---------------------------------------------------------------------------
# Workflow definition
# ---------------------------------------------------------------------------


def _wide_fanin_wf(num_roots: int = NUM_ROOTS, *, max_fan_in: int = 1000) -> WorkflowDefinition:
    """Build a DAG with *num_roots* independent roots → 1 leaf.

    Applies :func:`sequentialize_fan_in` to bound the fan-in to
    *max_fan_in*, inserting dummy merge nodes that chain batches
    sequentially.  The default (1000) keeps the per-task
    ``depended_by``/``dependencies`` JSON well under Azure Tables' 64 KB
    per-property limit while still keeping the 60k case at ~60 batches
    instead of ~600.
    """
    root_names = [f"root-{i}" for i in range(num_roots)]
    tasks = [WorkflowTask(name=name, kwargs={"i": i}) for i, name in enumerate(root_names)]
    tasks.append(
        WorkflowTask(
            name="leaf",
            kwargs={"step": "merge"},
            depends_on=root_names,
        )
    )
    raw = WorkflowDefinition(
        name="wide-fanin-60k",
        tasks=tasks,
        default_queue="stress-test",
        default_task_timeout_s=120,
    )
    return sequentialize_fan_in(raw, max_fan_in=max_fan_in)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@skip_without_azurite
class TestWideFanIn:
    """60 000 roots → 1 leaf: extreme fan-in stress test."""

    @pytest.mark.timeout(600)
    async def test_wide_fanin_submits_correctly(self, store):
        """Submit a 5k-root workflow and verify task counts and structure."""
        defn = _wide_fanin_wf(NUM_ROOTS_LOCAL)
        wf_id = uuid.uuid4().hex[:12]
        total_tasks = len(defn.tasks)

        await store.submit_workflow(wf_id, defn)

        status = await store.get_workflow_status(wf_id, include_tasks=False)
        assert status.total == total_tasks
        # Only root tasks with no dependencies should be READY
        assert status.running > 0  # roots are ready
        assert status.pending > 0  # merge + leaf are pending


@pytest.mark.live
class TestWideFanInLive:
    """60 000 roots → 1 leaf against live Azure Table Storage."""

    @pytest.mark.timeout(900)
    async def test_wide_fanin_submits_live(self, live_store):
        """Submit a 60k-root workflow against real Azure and verify structure."""
        defn = _wide_fanin_wf()
        wf_id = uuid.uuid4().hex[:12]
        total_tasks = len(defn.tasks)

        await live_store.submit_workflow(wf_id, defn)

        status = await live_store.get_workflow_status(wf_id, include_tasks=False)
        assert status.total == total_tasks
        assert status.running > 0
        assert status.pending > 0
