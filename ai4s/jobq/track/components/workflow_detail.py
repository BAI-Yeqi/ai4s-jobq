# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Protocol

import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback_context, dash_table, dcc, html
from dash.exceptions import PreventUpdate

from ..utils.workflow_store import get_store, run

LOG = logging.getLogger(__name__)

_STATUS_COLORS = {
    "completed": "#859900",
    "running": "#268bd2",
    "pending": "#b58900",
    "failed": "#dc322f",
    "cancelled": "#93a1a1",
    "skipped": "#6c71c4",
}

_TASK_STATUS_COLORS = {
    "completed": _STATUS_COLORS["completed"],
    "running": _STATUS_COLORS["running"],
    "ready": _STATUS_COLORS["pending"],
    "ready_pending_budget": _STATUS_COLORS["pending"],
    "pending": _STATUS_COLORS["pending"],
    "failed": _STATUS_COLORS["failed"],
    "upstream_failed": _STATUS_COLORS["failed"],
    "cancelled": _STATUS_COLORS["cancelled"],
    "skipped": _STATUS_COLORS["skipped"],
}

_TASK_STATUS_ORDER = {
    "running": 0,
    "ready": 1,
    "ready_pending_budget": 1,
    "pending": 1,
    "completed": 2,
    "failed": 3,
    "upstream_failed": 3,
    "cancelled": 4,
    "skipped": 5,
}


class _TaskStatusLike(Protocol):
    name: str
    status: object
    depends_on: list[str]
    dep_policy: str
    completed_deps: int
    failed_deps: int
    queue: str | None
    error: str | None
    started_at: datetime | None
    completed_at: datetime | None
    retries_remaining: int


class _WorkflowStatusLike(Protocol):
    workflow_id: str
    name: str
    status: object
    total: int
    completed: int
    running: int
    failed: int
    pending: int
    skipped: int
    created_at: datetime
    updated_at: datetime
    error: str | None
    tasks: dict[str, _TaskStatusLike]


def layout() -> html.Div:
    return html.Div(
        [
            dbc.Card(
                dbc.CardBody(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Label("Workflow ID"),
                                        dcc.Input(
                                            id="wf-detail-id-input",
                                            type="text",
                                            placeholder="Enter workflow ID",
                                            style={"width": "100%"},
                                        ),
                                    ],
                                    md=8,
                                    lg=9,
                                ),
                                dbc.Col(
                                    [
                                        dbc.Label("Load"),
                                        dbc.Button(
                                            "Load",
                                            id="wf-detail-load-btn",
                                            color="primary",
                                            className="w-100",
                                        ),
                                    ],
                                    md=4,
                                    lg=3,
                                ),
                            ],
                            className="g-3 align-items-end mb-3",
                        ),
                        html.Div(id="wf-detail-summary", className="mb-3"),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Label("Filter tasks by status"),
                                        dcc.Dropdown(
                                            id="wf-detail-task-status-filter",
                                            options=[  # type: ignore[arg-type]
                                                {"label": "All", "value": ""},
                                                {
                                                    "label": "Active (running + ready + pending)",
                                                    "value": "active",
                                                },
                                                {"label": "Running", "value": "running"},
                                                {
                                                    "label": "Pending / Ready",
                                                    "value": "pending",
                                                },
                                                {"label": "Completed", "value": "completed"},
                                                {"label": "Failed", "value": "failed"},
                                                {
                                                    "label": "Upstream failed",
                                                    "value": "upstream_failed",
                                                },
                                                {"label": "Cancelled", "value": "cancelled"},
                                                {"label": "Skipped", "value": "skipped"},
                                            ],
                                            value="",
                                            clearable=False,
                                        ),
                                    ],
                                    md=6,
                                    lg=4,
                                ),
                            ],
                            className="mb-2",
                        ),
                        dash_table.DataTable(  # type: ignore[attr-defined]
                            id="wf-detail-task-table",
                            data=[],
                            columns=_table_columns(),
                            page_size=20,
                            sort_action="native",
                            style_as_list_view=True,
                            style_table={"overflowX": "auto"},
                            style_header={
                                "backgroundColor": "#eee8d5",
                                "border": "0",
                                "color": "#586e75",
                                "fontWeight": "bold",
                            },
                            style_cell={
                                "backgroundColor": "#fdf6e3",
                                "border": "0",
                                "color": "#586e75",
                                "fontFamily": "var(--bs-body-font-family)",
                                "fontSize": "0.95rem",
                                "padding": "0.5rem 0.75rem",
                                "textAlign": "left",
                                "whiteSpace": "normal",
                                "height": "auto",
                            },
                            style_cell_conditional=[
                                {
                                    "if": {"column_id": "task"},
                                    "minWidth": "160px",
                                    "width": "160px",
                                },
                                {
                                    "if": {"column_id": "status"},
                                    "minWidth": "140px",
                                    "width": "140px",
                                },
                                {
                                    "if": {"column_id": "queue"},
                                    "minWidth": "160px",
                                    "width": "160px",
                                },
                                {
                                    "if": {"column_id": "dependencies"},
                                    "minWidth": "260px",
                                    "width": "260px",
                                },
                                {
                                    "if": {"column_id": "started_at"},
                                    "minWidth": "150px",
                                    "width": "150px",
                                },
                                {
                                    "if": {"column_id": "completed_at"},
                                    "minWidth": "150px",
                                    "width": "150px",
                                },
                                {
                                    "if": {"column_id": "error"},
                                    "minWidth": "260px",
                                    "width": "260px",
                                },
                                {
                                    "if": {"column_id": "retries_remaining"},
                                    "minWidth": "90px",
                                    "width": "90px",
                                },
                            ],
                            style_data_conditional=_task_status_styles(),
                        ),
                    ]
                ),
                className="shadow-sm",
            ),
        ]
    )


def register_callbacks(app) -> None:
    @app.callback(
        Output("wf-selected-workflow-id", "data", allow_duplicate=True),
        Input("wf-overview-table", "active_cell"),
        prevent_initial_call=True,
    )
    def store_selected_workflow(
        active_cell: dict[str, object] | None,
    ) -> str:
        if not active_cell:
            raise PreventUpdate

        # The overview table sets "id" on each row to the workflow_id,
        # so active_cell["row_id"] is always correct regardless of
        # pagination, sorting, or filtering.
        workflow_id = active_cell.get("row_id")
        if not isinstance(workflow_id, str) or not workflow_id.strip():
            raise PreventUpdate

        return workflow_id.strip()

    @app.callback(
        Output("wf-detail-summary", "children"),
        Output("wf-detail-task-table", "data"),
        Input("wf-selected-workflow-id", "data"),
        Input("wf-detail-load-btn", "n_clicks"),
        Input("wf-interval", "n_intervals"),
        Input("wf-detail-task-status-filter", "value"),
        State("wf-detail-id-input", "value"),
    )
    def load_workflow_detail(
        selected_workflow_id: str | None,
        _n_clicks: int | None,
        _n_intervals: int,
        task_status_filter: str | None,
        input_workflow_id: str | None,
    ) -> tuple[list[object], list[dict[str, object]]]:
        store = get_store()
        if store is None:
            raise PreventUpdate

        trigger_id = _trigger_id()
        workflow_id = _resolve_workflow_id(trigger_id, selected_workflow_id, input_workflow_id)
        if workflow_id is None:
            if trigger_id == "wf-detail-load-btn":
                return [dbc.Alert("Enter a workflow ID.", color="warning", className="mb-0")], []
            raise PreventUpdate

        try:
            status = run(store.get_workflow_status(workflow_id, include_tasks=True))
        except Exception as exc:
            LOG.exception("Failed to load workflow detail for %s", workflow_id)
            return [
                dbc.Alert(
                    f"Failed to load workflow {workflow_id}: {exc}",
                    color="danger",
                    className="mb-0",
                )
            ], []

        return _summary_children(status), _task_rows(
            status.tasks, task_status_filter or "", default_queue=status.default_queue
        )

    @app.callback(
        Output("wf-detail-task-status-filter", "value"),
        Input("wf-detail-card-total", "n_clicks"),
        Input("wf-detail-card-completed", "n_clicks"),
        Input("wf-detail-card-running", "n_clicks"),
        Input("wf-detail-card-failed", "n_clicks"),
        Input("wf-detail-card-pending", "n_clicks"),
        Input("wf-detail-card-skipped", "n_clicks"),
        prevent_initial_call=True,
    )
    def _brush_task_filter(
        _t: int | None,
        _c: int | None,
        _r: int | None,
        _f: int | None,
        _p: int | None,
        _s: int | None,
    ) -> str:
        from dash import ctx

        triggered = getattr(ctx, "triggered_id", None)
        mapping = {
            "wf-detail-card-total": "",
            "wf-detail-card-completed": "completed",
            "wf-detail-card-running": "running",
            "wf-detail-card-failed": "failed",
            "wf-detail-card-pending": "active",
            "wf-detail-card-skipped": "skipped",
        }
        if not isinstance(triggered, str) or triggered not in mapping:
            raise PreventUpdate
        return mapping[triggered]


def _summary_children(status: _WorkflowStatusLike) -> list[object]:
    status_text = str(status.status)
    status_color = _STATUS_COLORS.get(status_text, "#586e75")

    header = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H4(status.name, className="mb-1"),
                                html.Div(
                                    [
                                        html.Span("Workflow ID: ", className="fw-semibold"),
                                        html.Code(status.workflow_id),
                                    ],
                                    className="mb-1",
                                ),
                                html.Div(
                                    [
                                        html.Span("Created: ", className="fw-semibold"),
                                        html.Span(_format_datetime(status.created_at)),
                                        html.Span("  •  ", className="mx-2"),
                                        html.Span("Updated: ", className="fw-semibold"),
                                        html.Span(_format_datetime(status.updated_at)),
                                    ],
                                    className="text-muted small",
                                ),
                            ],
                            md=9,
                        ),
                        dbc.Col(
                            html.Div(
                                [
                                    html.Div(
                                        "Status", className="text-uppercase small fw-semibold mb-1"
                                    ),
                                    html.Div(
                                        status_text,
                                        className="fw-bold",
                                        style={"color": status_color, "fontSize": "1.1rem"},
                                    ),
                                ],
                                className="text-md-end",
                            ),
                            md=3,
                        ),
                    ],
                    className="g-3",
                ),
                dbc.Alert(status.error, color="danger", className="mt-3 mb-0")
                if status.error
                else None,
            ]
        ),
        className="mb-3 border-0 shadow-sm",
        style={"backgroundColor": "#fdf6e3"},
    )

    cards = dbc.Row(
        [
            _summary_card(
                "Total",
                status.total,
                "#586e75",
                card_id="wf-detail-card-total",
                brush_status="",
            ),
            _summary_card(
                "Completed",
                status.completed,
                _STATUS_COLORS["completed"],
                card_id="wf-detail-card-completed",
                brush_status="completed",
            ),
            _summary_card(
                "Running",
                status.running,
                _STATUS_COLORS["running"],
                card_id="wf-detail-card-running",
                brush_status="running",
            ),
            _summary_card(
                "Failed",
                status.failed,
                _STATUS_COLORS["failed"],
                card_id="wf-detail-card-failed",
                brush_status="failed",
            ),
            _summary_card(
                "Pending",
                status.pending,
                _STATUS_COLORS["pending"],
                card_id="wf-detail-card-pending",
                brush_status="active",
            ),
            _summary_card(
                "Skipped",
                status.skipped,
                _STATUS_COLORS["skipped"],
                card_id="wf-detail-card-skipped",
                brush_status="skipped",
            ),
        ],
        className="g-3",
    )

    return [header, cards]


def _summary_card(
    label: str,
    value: int,
    color: str,
    *,
    card_id: str | None = None,
    brush_status: str | None = None,
) -> dbc.Col:
    tooltip = (
        "Click to show all tasks"
        if brush_status == ""
        else (f"Click to filter the task list to {brush_status}" if brush_status else None)
    )
    body_children: list[object] = [
        html.Div(str(value), className="display-6 fw-bold mb-1"),
        html.Div(label, className="text-uppercase small fw-semibold"),
    ]
    card = dbc.Card(
        dbc.CardBody(body_children),
        className="h-100 border-0",
        style={"backgroundColor": color, "color": "#fdf6e3"},
    )
    if card_id is not None:
        card = html.Div(
            card,
            id=card_id,
            n_clicks=0,
            style={"cursor": "pointer", "height": "100%"},
            title=tooltip or "",
        )
    return dbc.Col(card, xs=6, md=4, lg=2)


def _table_columns() -> list[dict[str, str]]:
    return [
        {"name": "Task", "id": "task"},
        {"name": "Status", "id": "status"},
        {"name": "Queue", "id": "queue"},
        {"name": "Dependencies", "id": "dependencies"},
        {"name": "Started", "id": "started_at"},
        {"name": "Completed", "id": "completed_at"},
        {"name": "Error", "id": "error"},
        {"name": "Retries Left", "id": "retries_remaining"},
    ]


def _task_rows(
    tasks: dict[str, _TaskStatusLike],
    status_filter: str = "",
    *,
    default_queue: str = "",
) -> list[dict[str, object]]:
    sorted_tasks = sorted(tasks.values(), key=_task_sort_key)
    if status_filter:
        if status_filter == "active":
            allowed = {"running", "ready", "ready_pending_budget", "pending"}
            sorted_tasks = [t for t in sorted_tasks if str(t.status) in allowed]
        elif status_filter == "pending":
            allowed = {"pending", "ready", "ready_pending_budget"}
            sorted_tasks = [t for t in sorted_tasks if str(t.status) in allowed]
        elif status_filter == "failed":
            allowed = {"failed", "upstream_failed"}
            sorted_tasks = [t for t in sorted_tasks if str(t.status) in allowed]
        else:
            sorted_tasks = [t for t in sorted_tasks if str(t.status) == status_filter]
    return [
        {
            "task": task.name,
            "status": str(task.status),
            "queue": task.queue or default_queue or "—",
            "dependencies": _format_dependencies(task),
            "started_at": _format_datetime(task.started_at),
            "completed_at": _format_datetime(task.completed_at),
            "error": task.error or "—",
            "retries_remaining": task.retries_remaining,
        }
        for task in sorted_tasks
    ]


def _task_sort_key(task: _TaskStatusLike) -> tuple[int, str]:
    status = str(task.status)
    return _TASK_STATUS_ORDER.get(status, 99), task.name


def _format_dependencies(task: _TaskStatusLike) -> str:
    if not task.depends_on:
        return "—"

    total = len(task.depends_on)
    progress = f"{task.completed_deps}/{total} complete"
    if task.failed_deps:
        progress = f"{progress}, {task.failed_deps} failed"
    return f"{', '.join(task.depends_on)} ({task.dep_policy}; {progress})"


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "—"
    timestamp = (
        value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    )
    return timestamp.strftime("%Y-%m-%d %H:%M:%SZ")


def _task_status_styles() -> list[dict[str, object]]:
    styles: list[dict[str, object]] = []
    for status, color in _TASK_STATUS_COLORS.items():
        styles.append(
            {
                "if": {"filter_query": f'{{status}} = "{status}"'},
                "backgroundColor": _rgba(color, 0.08),
                "borderLeft": f"4px solid {color}",
            }
        )
        styles.append(
            {
                "if": {"column_id": "status", "filter_query": f'{{status}} = "{status}"'},
                "color": color,
                "fontWeight": "600",
            }
        )
    return styles


def _rgba(hex_color: str, alpha: float) -> str:
    red = int(hex_color[1:3], 16)
    green = int(hex_color[3:5], 16)
    blue = int(hex_color[5:7], 16)
    return f"rgba({red}, {green}, {blue}, {alpha})"


def _resolve_workflow_id(
    trigger_id: str | None, selected_workflow_id: str | None, input_workflow_id: str | None
) -> str | None:
    selected = (selected_workflow_id or "").strip() or None
    manual = (input_workflow_id or "").strip() or None

    if trigger_id == "wf-selected-workflow-id":
        return selected or manual
    return manual or selected


def _trigger_id() -> str | None:
    if not callback_context.triggered:
        return None

    first_trigger = callback_context.triggered[0]
    if not isinstance(first_trigger, dict):
        return None

    prop_id = first_trigger.get("prop_id")
    if not isinstance(prop_id, str):
        return None

    return prop_id.split(".")[0]
