# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from dash.exceptions import PreventUpdate

from ai4s.jobq.workflow.entities import TaskState

from ..utils.workflow_store import (
    get_store,
    get_workflow_counts,
    run_many,
    run_many_limited,
    start_counts_refresher,
)

if TYPE_CHECKING:
    from ai4s.jobq.workflow.entities import TaskStatus, WorkflowStatus

LOG = logging.getLogger(__name__)

_STATUS_COLORS = {
    "completed": "#859900",
    "running": "#268bd2",
    "pending": "#b58900",
    "failed": "#dc322f",
    "cancelled": "#93a1a1",
}

_MAX_SCANNED_WORKFLOWS = 50
_PER_SHARD_LIMIT = 50
_STALE_RUNNING_WINDOW = timedelta(minutes=30)
_ALERT_PREVIEW_LIMIT = 5

_StaleTask = tuple[str, str, str, int]
_OrphanTask = tuple[str, str, int, int, str]


def layout() -> html.Div:
    return html.Div(
        [
            dcc.Loading(
                color=_STATUS_COLORS["running"],
                type="default",
                children=html.Div(
                    [
                        dbc.Row(
                            [
                                _stat_card(
                                    "Active workflows",
                                    "wf-health-total",
                                    card_id="wf-health-card-active",
                                    brush_status="active",
                                ),
                                _stat_card(
                                    "Running workflows",
                                    "wf-health-running",
                                    badge_text="running",
                                    accent_color=_STATUS_COLORS["running"],
                                    card_id="wf-health-card-running",
                                    brush_status="running",
                                ),
                                _stat_card(
                                    "Pending workflows",
                                    "wf-health-pending",
                                    badge_text="pending",
                                    accent_color=_STATUS_COLORS["pending"],
                                    card_id="wf-health-card-pending",
                                    brush_status="pending",
                                ),
                            ],
                            className="g-3 row-cols-1 row-cols-md-2 row-cols-xl-3 mb-3",
                        ),
                        html.Div(id="wf-health-alerts", className="d-grid gap-2"),
                    ]
                ),
            )
        ]
    )


def register_callbacks(app) -> None:
    # Kick off the background count refresher so the stat cards have
    # values to read on the first interval tick. Idempotent.
    start_counts_refresher()

    @app.callback(
        Output("wf-status-filter", "value", allow_duplicate=True),
        Input("wf-health-card-active", "n_clicks"),
        Input("wf-health-card-running", "n_clicks"),
        Input("wf-health-card-pending", "n_clicks"),
        prevent_initial_call=True,
    )
    def _brush_overview_filter(
        _n_active: int | None,
        _n_running: int | None,
        _n_pending: int | None,
    ) -> str:
        """Click on a health card -> filter the workflow overview table.

        Maps each clickable stat card to the corresponding value of
        the overview's status dropdown. ``"active"`` is a synthetic
        choice (running + pending) handled by the overview callback.
        """
        from dash import ctx

        triggered = getattr(ctx, "triggered_id", None)
        mapping = {
            "wf-health-card-active": "active",
            "wf-health-card-running": "running",
            "wf-health-card-pending": "pending",
        }
        if not isinstance(triggered, str):
            raise PreventUpdate
        new_value = mapping.get(triggered)
        if new_value is None:
            raise PreventUpdate
        return new_value

    @app.callback(
        Output("wf-health-total", "children"),
        Output("wf-health-running", "children"),
        Output("wf-health-pending", "children"),
        Output("wf-health-alerts", "children"),
        Input("wf-interval", "n_intervals"),
    )
    def update_workflow_health(
        _n_intervals: int,
    ) -> tuple[int, int, int, list[dbc.Alert]]:
        store = get_store()
        if store is None:
            raise PreventUpdate

        # Stat-card counts come from the background-refreshed cache so
        # the displayed numbers are always accurate (full partition
        # scan, not a per-shard ``limit=50`` truncation) and converge
        # page-by-page during refresh cycles. Scope is intentionally
        # limited to *active* workflows — terminal history is unbounded
        # at scale and not useful as a live indicator.
        counts = get_workflow_counts()
        n_running = counts.get("running", 0)
        n_pending = counts.get("pending", 0)
        n_active = n_running + n_pending

        # Stale/orphan alert preview still needs full ``WorkflowStatus``
        # rows (we sort by ``updated_at`` and inspect tasks per
        # workflow), so the bounded list_workflows scan remains for
        # that purpose only.
        try:
            running = run_many(
                store.list_workflows(status="running", limit=_PER_SHARD_LIMIT),
            )[0]
        except Exception as exc:
            LOG.exception("Failed to load running workflows for health preview")
            raise PreventUpdate from exc

        running_workflows = sorted(
            running,
            key=lambda wf: _normalize_timestamp(wf.updated_at),
        )

        # Fan out task listings concurrently with bounded concurrency
        # instead of issuing one-by-one sequential requests.
        scan_targets = running_workflows[:_MAX_SCANNED_WORKFLOWS]

        async def _safe_list_tasks(
            workflow: WorkflowStatus,
        ) -> tuple[WorkflowStatus, list[TaskStatus] | None]:
            try:
                tasks = await store.list_tasks(workflow.workflow_id)
                return workflow, tasks
            except Exception:
                LOG.warning(
                    "Failed to inspect workflow %s for dashboard health",
                    workflow.workflow_id,
                    exc_info=True,
                )
                return workflow, None

        task_results: list[tuple[WorkflowStatus, list[TaskStatus] | None]] = (
            run_many_limited(*(_safe_list_tasks(wf) for wf in scan_targets)) if scan_targets else []
        )

        stale_tasks: list[_StaleTask] = []
        orphan_tasks: list[_OrphanTask] = []
        for workflow, tasks in task_results:
            if tasks is None:
                continue
            stale_tasks.extend(_find_stale_tasks(workflow, tasks))
            orphan_tasks.extend(_find_orphan_tasks(workflow, tasks))

        alerts = _build_alerts(
            stale_tasks=stale_tasks,
            orphan_tasks=orphan_tasks,
            running_workflow_count=n_running,
        )

        return (
            n_active,
            n_running,
            n_pending,
            alerts,
        )


def _stat_card(
    title: str,
    value_id: str,
    *,
    badge_text: str | None = None,
    accent_color: str = "#586e75",
    card_id: str | None = None,
    brush_status: str | None = None,
) -> dbc.Col:
    heading = [html.Span(title, className="small text-muted")]
    if badge_text:
        heading.append(
            dbc.Badge(
                badge_text.title(),
                style={
                    "backgroundColor": accent_color,
                    "color": "#fdf6e3",
                    "fontWeight": "600",
                },
            )
        )

    # Brushable cards get a tooltip + pointer cursor on a wrapping
    # html.Div so Dash callbacks can observe n_clicks without dbc.Card
    # rejecting unknown kwargs.
    tooltip_label = (
        f"Click to show only {brush_status} workflows in the overview table"
        if brush_status
        else None
    )
    if tooltip_label:
        heading.append(
            html.Span(
                "→",
                title=tooltip_label,
                className="ms-2 text-muted small",
                style={"opacity": 0.6},
            )
        )

    card = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    heading,
                    className="d-flex justify-content-between align-items-start mb-2",
                ),
                html.Div(
                    "0",
                    id=value_id,
                    style={
                        "color": accent_color,
                        "fontSize": "2rem",
                        "fontWeight": "700",
                        "lineHeight": "1.1",
                    },
                ),
            ]
        ),
        className="h-100 shadow-sm",
        style={
            "backgroundColor": "#fdf6e3",
            "border": "1px solid #eee8d5",
        },
    )

    if card_id is not None:
        card = html.Div(
            card,
            id=card_id,
            n_clicks=0,
            style={"cursor": "pointer", "height": "100%"},
            title=tooltip_label or "",
        )

    return dbc.Col(card)


def _find_stale_tasks(workflow: WorkflowStatus, tasks: list[TaskStatus]) -> list[_StaleTask]:
    now = datetime.now(timezone.utc)
    stale_tasks: list[_StaleTask] = []
    for task in tasks:
        if (
            task.status != TaskState.RUNNING
            or task.started_at is None
            or task.completed_at is not None
        ):
            continue

        age = now - _normalize_timestamp(task.started_at)
        if age > _STALE_RUNNING_WINDOW:
            stale_tasks.append(
                (
                    workflow.workflow_id,
                    task.name,
                    task.queue or "—",
                    int(age.total_seconds() // 60),
                )
            )
    return stale_tasks


def _find_orphan_tasks(workflow: WorkflowStatus, tasks: list[TaskStatus]) -> list[_OrphanTask]:
    orphan_tasks: list[_OrphanTask] = []
    for task in tasks:
        dependency_count = len(task.depends_on)
        resolved_dependencies = task.completed_deps + task.failed_deps
        if (
            task.status == TaskState.PENDING
            and dependency_count > 0
            and resolved_dependencies >= dependency_count
        ):
            orphan_tasks.append(
                (
                    workflow.workflow_id,
                    task.name,
                    resolved_dependencies,
                    dependency_count,
                    task.queue or "—",
                )
            )
    return orphan_tasks


def _build_alerts(
    *,
    stale_tasks: list[_StaleTask],
    orphan_tasks: list[_OrphanTask],
    running_workflow_count: int,
) -> list[dbc.Alert]:
    scanned_workflows = min(running_workflow_count, _MAX_SCANNED_WORKFLOWS)
    scan_note = ""
    if running_workflow_count > scanned_workflows:
        scan_note = f" Scanned {scanned_workflows} of {running_workflow_count} running workflows."

    alerts: list[dbc.Alert] = []
    if stale_tasks:
        alerts.append(
            dbc.Alert(
                [
                    html.H6("Stale running tasks", className="alert-heading mb-2"),
                    html.P(
                        (
                            f"{len(stale_tasks)} running task(s) have been active for more than "
                            "30 minutes."
                        )
                        + scan_note,
                        className="mb-2",
                    ),
                    _examples_list(
                        [
                            f"{workflow_id}/{task_name} — {age_min}m on queue {queue_name}"
                            for workflow_id, task_name, queue_name, age_min in stale_tasks
                        ]
                    ),
                ],
                className="mb-0",
                style={
                    "backgroundColor": "#fff5eb",
                    "border": "1px solid #cb4b16",
                    "color": "#8b3f16",
                },
            )
        )

    if orphan_tasks:
        alerts.append(
            dbc.Alert(
                [
                    html.H6("Potential orphan tasks", className="alert-heading mb-2"),
                    html.P(
                        (
                            f"{len(orphan_tasks)} pending task(s) have all dependency outcomes "
                            "recorded but remain pending."
                        )
                        + scan_note,
                        className="mb-2",
                    ),
                    _examples_list(
                        [
                            (
                                f"{workflow_id}/{task_name} — deps resolved {resolved}/{total}, "
                                f"queue {queue_name}"
                            )
                            for workflow_id, task_name, resolved, total, queue_name in orphan_tasks
                        ]
                    ),
                ],
                className="mb-0",
                color="warning",
            )
        )

    if alerts:
        return alerts

    message = "No workflow health warnings detected."
    if scan_note:
        message += scan_note
    return [dbc.Alert(message, color="success", className="mb-0")]


def _examples_list(items: list[str]) -> html.Div:
    preview = items[:_ALERT_PREVIEW_LIMIT]
    children: list[html.Div | html.Ul] = [
        html.Ul([html.Li(item) for item in preview], className="mb-0")
    ]
    remaining = len(items) - len(preview)
    if remaining > 0:
        children.append(html.Div(f"…and {remaining} more.", className="small mt-2"))
    return html.Div(children)  # type: ignore[arg-type]


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
