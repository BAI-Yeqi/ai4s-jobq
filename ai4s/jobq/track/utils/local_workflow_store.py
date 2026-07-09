# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""In-memory workflow store backed by a local workflow definition file.

This adapter provides the small async API surface used by the Track dashboard
so a user can visualize a workflow DAG before submitting it.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ai4s.jobq.workflow.entities import (
    TaskState,
    TaskStatus,
    WorkflowDefinition,
    WorkflowState,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class LocalWorkflowStore:
    """Async store facade for a single workflow definition from disk."""

    def __init__(self, workflow_status: WorkflowStatus, task_commands: dict[str, str]) -> None:
        self._workflow_status = workflow_status
        self._task_commands = task_commands

    @classmethod
    def from_file(cls, path: str) -> LocalWorkflowStore:
        workflow_path = Path(path)
        definition = _load_definition_for_preview(workflow_path)
        definition.validate()

        now = datetime.now(UTC)
        mtime = datetime.fromtimestamp(workflow_path.stat().st_mtime, tz=UTC)
        created_at = min(now, mtime)
        updated_at = max(now, mtime)

        children = definition.child_map
        queues = sorted({task.queue or definition.default_queue for task in definition.tasks})
        task_commands = {
            task.name: str(task.kwargs.get("cmd", ""))
            for task in definition.tasks
            if isinstance(task.kwargs.get("cmd", ""), str)
        }

        tasks = {
            task.name: TaskStatus(
                name=task.name,
                status=TaskState.PENDING,
                depends_on=list(task.depends_on),
                depended_by=list(children.get(task.name, [])),
                dep_policy=str(task.dep_policy),
                completed_deps=0,
                failed_deps=0,
                queue=task.queue or definition.default_queue,
                output_ref=None,
                error=None,
                started_at=None,
                completed_at=None,
                retries_remaining=task.num_retries,
                task_timeout_s=task.timeout_s or definition.default_task_timeout_s,
                updated_at=updated_at,
                fan_out_at=None,
                attempt_no=0,
                fan_out_done=False,
            )
            for task in definition.tasks
        }

        workflow_id = f"local::{workflow_path.stem}"
        status = WorkflowStatus(
            workflow_id=workflow_id,
            name=definition.name,
            status=WorkflowState.PENDING,
            total=len(definition.tasks),
            completed=0,
            running=0,
            failed=0,
            pending=len(definition.tasks),
            skipped=0,
            default_queue=definition.default_queue,
            queues_used=queues,
            created_at=created_at,
            updated_at=updated_at,
            error=None,
            tasks=tasks,
        )
        return cls(status, task_commands)

    def get_task_command_map(self) -> dict[str, str]:
        """Return task -> command mapping for preview UIs (local file mode)."""
        return dict(self._task_commands)

    async def list_workflows(
        self,
        *,
        status: WorkflowState | str | None = None,
        limit: int | None = None,
        updated_after: datetime | None = None,
    ) -> list[WorkflowStatus]:
        if not self._matches_workflow_status_filter(status):
            return []
        if updated_after is not None and self._workflow_status.updated_at < updated_after:
            return []
        if limit is not None and limit <= 0:
            return []
        return [self._clone_workflow(include_tasks=False)]

    async def list_recent_terminal(
        self,
        *,
        limit: int = 50,
        status: WorkflowState | str | None = None,
    ) -> list[WorkflowStatus]:
        _ = limit
        if status is None:
            return []
        terminal = {WorkflowState.COMPLETED, WorkflowState.FAILED, WorkflowState.CANCELLED}
        requested = str(status).lower().split(".")[-1]
        if requested in {str(s) for s in terminal}:
            return []
        return []

    async def get_workflow_status(
        self,
        workflow_id: str,
        *,
        include_tasks: bool = True,
    ) -> WorkflowStatus | None:
        if workflow_id != self._workflow_status.workflow_id:
            return None
        return self._clone_workflow(include_tasks=include_tasks)

    async def list_tasks(
        self,
        workflow_id: str,
        *,
        status: TaskState | str | None = None,
        queue: str | None = None,
        name_prefix: str | None = None,
    ) -> list[TaskStatus]:
        if workflow_id != self._workflow_status.workflow_id:
            return []

        wanted_status = None if status is None else str(status).lower().split(".")[-1]
        out: list[TaskStatus] = []

        for task in self._workflow_status.tasks.values():
            task_state = str(task.status).lower().split(".")[-1]
            if wanted_status is not None and task_state != wanted_status:
                continue
            if queue is not None and task.queue != queue:
                continue
            if name_prefix is not None and not task.name.startswith(name_prefix):
                continue
            out.append(
                replace(task, depends_on=list(task.depends_on), depended_by=list(task.depended_by))
            )

        return out

    async def count_active_workflows_by_status(
        self,
        *,
        on_progress: Callable[[dict[str, int]], None] | None = None,
    ) -> dict[str, int]:
        counts = {"running": 0, "pending": 1}
        if on_progress is not None:
            on_progress(dict(counts))
        return counts

    def _clone_workflow(self, *, include_tasks: bool) -> WorkflowStatus:
        tasks = {}
        if include_tasks:
            tasks = {
                name: replace(
                    task, depends_on=list(task.depends_on), depended_by=list(task.depended_by)
                )
                for name, task in self._workflow_status.tasks.items()
            }
        return replace(
            self._workflow_status,
            queues_used=list(self._workflow_status.queues_used),
            tasks=tasks,
        )

    def _matches_workflow_status_filter(self, status: WorkflowState | str | None) -> bool:
        if status is None:
            return True
        normalized = str(status).lower().split(".")[-1]
        return normalized == str(self._workflow_status.status)


def _load_definition_for_preview(path: Path) -> WorkflowDefinition:
    """Load a workflow definition with relaxed aliases used by external generators.

    The dashboard preview accepts ``dep_policy: one_success`` as an alias of
    ``any`` so existing DAG generators can be visualized without resubmitting.
    """
    with path.open() as f:
        if path.suffix in {".yaml", ".yml"}:
            import yaml

            payload: Any = yaml.safe_load(f)
        else:
            payload = json.load(f)

    if isinstance(payload, dict):
        tasks = payload.get("tasks")
        if isinstance(tasks, list):
            for task in tasks:
                if isinstance(task, dict) and task.get("dep_policy") == "one_success":
                    task["dep_policy"] = "any"

    return WorkflowDefinition.from_json(json.dumps(payload))
