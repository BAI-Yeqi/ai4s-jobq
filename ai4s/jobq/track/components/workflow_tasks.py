# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import dash_bootstrap_components as dbc
from dash import Input, Output, dash_table, dcc, html
from dash.exceptions import PreventUpdate

if TYPE_CHECKING:
    from datetime import datetime

from ..utils.workflow_store import get_store, run, run_many_limited

LOG = logging.getLogger(__name__)

_MAX_ROWS = 500
_MAX_WORKFLOWS_SCANNED = 50
_STATUS_COLORS = {
    "completed": "#859900",
    "running": "#268bd2",
    "pending": "#b58900",
    "failed": "#dc322f",
    "cancelled": "#93a1a1",
    "skipped": "#6c71c4",
}

_WORKFLOW_STATUS_OPTIONS = [
    {"label": "All workflows", "value": ""},
    {"label": "Pending", "value": "pending"},
    {"label": "Running", "value": "running"},
    {"label": "Completed", "value": "completed"},
    {"label": "Failed", "value": "failed"},
    {"label": "Cancelled", "value": "cancelled"},
]

_TASK_STATUS_OPTIONS = [
    {"label": "All tasks", "value": ""},
    {"label": "Pending", "value": "pending"},
    {"label": "Running", "value": "running"},
    {"label": "Completed", "value": "completed"},
    {"label": "Failed", "value": "failed"},
    {"label": "Cancelled", "value": "cancelled"},
    {"label": "Skipped", "value": "skipped"},
]


def _label(text: str) -> html.Div:
    return html.Div(text, className="small text-muted mb-1")


def _format_datetime(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def _format_error(value: str | None) -> str:
    if not value:
        return ""
    single_line = " ".join(value.split())
    if len(single_line) <= 160:
        return single_line
    return f"{single_line[:159]}…"


def _status_styles() -> list[dict[str, object]]:
    return [
        {
            "if": {"filter_query": f'{{status}} = "{status}"'},
            "borderLeft": f"4px solid {color}",
            "color": color,
        }
        for status, color in _STATUS_COLORS.items()
    ]


def layout() -> html.Div:
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            _label("Workflow status"),
                            dcc.Dropdown(
                                id="wf-tasks-wf-status",
                                options=_WORKFLOW_STATUS_OPTIONS,  # type: ignore[arg-type]
                                value="running",
                                clearable=False,
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            _label("Task status"),
                            dcc.Dropdown(
                                id="wf-tasks-task-status",
                                options=_TASK_STATUS_OPTIONS,  # type: ignore[arg-type]
                                value="",
                                clearable=False,
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            _label("Queue filter"),
                            dcc.Input(
                                id="wf-tasks-queue-filter",
                                type="text",
                                placeholder="Queue name",
                                debounce=True,
                                className="form-control",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            _label("Name prefix"),
                            dcc.Input(
                                id="wf-tasks-name-filter",
                                type="text",
                                placeholder="Task name prefix",
                                debounce=True,
                                className="form-control",
                            ),
                        ],
                        md=3,
                    ),
                ],
                className="g-2 mb-3",
            ),
            dash_table.DataTable(  # type: ignore[attr-defined]
                id="wf-tasks-table",
                columns=[
                    {"name": "Workflow", "id": "workflow_id"},
                    {"name": "Task", "id": "task"},
                    {"name": "Status", "id": "status"},
                    {"name": "Queue", "id": "queue"},
                    {"name": "Started", "id": "started"},
                    {"name": "Error", "id": "error"},
                ],
                data=[],
                page_action="native",
                page_size=20,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#eee8d5",
                    "border": "1px solid #93a1a1",
                    "color": "#586e75",
                    "fontWeight": "600",
                },
                style_cell={
                    "backgroundColor": "#fdf6e3",
                    "border": "1px solid #eee8d5",
                    "color": "#657b83",
                    "fontFamily": "Segoe UI, sans-serif",
                    "fontSize": "13px",
                    "padding": "8px",
                    "textAlign": "left",
                    "maxWidth": 0,
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                },
                style_cell_conditional=[
                    {"if": {"column_id": "workflow_id"}, "width": "18%"},
                    {"if": {"column_id": "task"}, "width": "22%"},
                    {"if": {"column_id": "status"}, "width": "10%"},
                    {"if": {"column_id": "queue"}, "width": "15%"},
                    {"if": {"column_id": "started"}, "width": "15%"},
                    {"if": {"column_id": "error"}, "width": "20%"},
                ],
                style_data_conditional=_status_styles(),
            ),
        ]
    )


def register_callbacks(app) -> None:
    @app.callback(
        Output("wf-tasks-table", "data"),
        Input("wf-interval", "n_intervals"),
        Input("wf-tasks-wf-status", "value"),
        Input("wf-tasks-task-status", "value"),
        Input("wf-tasks-queue-filter", "value"),
        Input("wf-tasks-name-filter", "value"),
    )
    def update_workflow_tasks(_, workflow_status, task_status, queue_filter, name_filter):
        store = get_store()
        if store is None:
            raise PreventUpdate

        workflow_filter = (workflow_status or "").strip() or None
        task_filter = (task_status or "").strip() or None
        queue_value = (queue_filter or "").strip() or None
        name_prefix = (name_filter or "").strip() or None

        try:
            workflows = run(
                store.list_workflows(status=workflow_filter, limit=_MAX_WORKFLOWS_SCANNED)
            )
        except Exception as exc:
            LOG.warning("Failed to list workflows for dashboard", exc_info=True)
            raise PreventUpdate from exc

        rows: list[dict[str, str]] = []

        # Process workflows in chunks with concurrent task listing,
        # preserving the early-exit when we hit _MAX_ROWS.
        chunk_size = 8

        async def _safe_list_tasks(wf):
            try:
                tasks = await store.list_tasks(
                    wf.workflow_id,
                    status=task_filter,
                    queue=queue_value,
                    name_prefix=name_prefix,
                )
                return wf, tasks
            except Exception:
                LOG.warning("Failed to list tasks for workflow %s", wf.workflow_id, exc_info=True)
                return wf, None

        for chunk_start in range(0, len(workflows), chunk_size):
            if len(rows) >= _MAX_ROWS:
                break

            chunk = workflows[chunk_start : chunk_start + chunk_size]
            chunk_results = run_many_limited(*(_safe_list_tasks(wf) for wf in chunk))

            for _wf, tasks in chunk_results:
                if tasks is None:
                    continue
                for task in tasks:
                    rows.append(
                        {
                            "workflow_id": _wf.workflow_id,
                            "task": task.name,
                            "status": str(task.status),
                            "queue": task.queue or "",
                            "started": _format_datetime(task.started_at),
                            "error": _format_error(task.error),
                        }
                    )
                    if len(rows) >= _MAX_ROWS:
                        break

        return rows
