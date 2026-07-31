# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from datetime import datetime, timedelta, timezone

from ai4s.jobq.track.components.workflow_overview import (
    _aggregate_stats,
    _data_conditional,
    _format_velocity,
    _workflow_duration_seconds,
    _workflow_rows,
)
from ai4s.jobq.workflow.entities import WorkflowState, WorkflowStatus


def test_overview_helpers_accept_workflow_status() -> None:
    updated_at = datetime.now(timezone.utc)
    workflow = WorkflowStatus(
        workflow_id="workflow-1",
        name="preview",
        status=WorkflowState.COMPLETED,
        total=4,
        completed=2,
        running=0,
        failed=1,
        pending=0,
        skipped=1,
        default_queue="queue",
        queues_used=["queue"],
        created_at=updated_at - timedelta(seconds=10),
        updated_at=updated_at,
    )

    assert _workflow_rows([workflow])[0] | {"created_at": "", "updated_at": ""} == {
        "id": "workflow-1",
        "workflow_id": "workflow-1",
        "name": "preview",
        "status": "completed",
        "progress": "2/4",
        "velocity": "24.0 /min",
        "running": 0,
        "failed": 1,
        "created_at": "",
        "updated_at": "",
    }
    assert _workflow_duration_seconds(workflow) == 10
    assert _format_velocity(workflow) == "24.0 /min"
    assert _data_conditional([workflow])
    assert _aggregate_stats([workflow]) is not None
