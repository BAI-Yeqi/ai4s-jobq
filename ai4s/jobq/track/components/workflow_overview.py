# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Protocol

import dash_bootstrap_components as dbc
from dash import Input, Output, dash_table, dcc, html
from dash.exceptions import PreventUpdate

from ..utils.workflow_store import get_store, run

LOG = logging.getLogger(__name__)

_STATUS_OPTIONS = [
    {"label": "All", "value": ""},
    {"label": "Active (running + pending)", "value": "active"},
    {"label": "Pending", "value": "pending"},
    {"label": "Running", "value": "running"},
    {"label": "Completed", "value": "completed"},
    {"label": "Failed", "value": "failed"},
    {"label": "Cancelled", "value": "cancelled"},
]

_TIME_RANGE_OPTIONS = [
    {"label": "Last hour", "value": "1"},
    {"label": "Last 6 hours", "value": "6"},
    {"label": "Last 24 hours", "value": "24"},
    {"label": "Last 7 days", "value": "168"},
    {"label": "Last 30 days", "value": "720"},
]

_DEFAULT_TIME_RANGE_HOURS = "24"
_PER_SHARD_LIMIT = 50
# Hard cap on the number of recent-terminal rows fetched per status.
# The R-{reverse_ticks}-{id} index returns the N most recent globally
# with an index-only seek per shard, so this can be much higher than
# the active-side per-shard cap without touching the (unbounded) T-
# range.
_RECENT_TERMINAL_LIMIT = 200

_STATUS_COLORS = {
    "completed": "#859900",
    "running": "#268bd2",
    "pending": "#b58900",
    "failed": "#dc322f",
    "cancelled": "#93a1a1",
    "skipped": "#6c71c4",
}


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
                                        dbc.Label("Status"),
                                        dcc.Dropdown(
                                            id="wf-status-filter",
                                            options=_STATUS_OPTIONS,  # type: ignore[arg-type]
                                            value="",
                                            clearable=False,
                                        ),
                                    ],
                                    md=4,
                                    lg=3,
                                ),
                                dbc.Col(
                                    [
                                        dbc.Label("Time range"),
                                        dcc.Dropdown(
                                            id="wf-time-range",
                                            options=_TIME_RANGE_OPTIONS,  # type: ignore[arg-type]
                                            value=_DEFAULT_TIME_RANGE_HOURS,
                                            clearable=False,
                                        ),
                                    ],
                                    md=4,
                                    lg=3,
                                ),
                            ],
                            className="g-3 mb-3",
                        ),
                        html.Div(id="wf-aggregate-stats", className="mb-3"),
                        dash_table.DataTable(  # type: ignore[attr-defined]
                            id="wf-overview-table",
                            data=[],
                            columns=_table_columns(),
                            row_selectable="single",
                            selected_rows=[],
                            page_size=15,
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
                            },
                            style_data={"cursor": "pointer"},
                            style_data_conditional=_data_conditional([]),
                        ),
                        dcc.Interval(id="wf-interval", interval=60_000, n_intervals=0),
                    ]
                ),
                className="shadow-sm",
            )
        ]
    )


def register_callbacks(app) -> None:
    @app.callback(
        Output("wf-overview-table", "data"),
        Output("wf-overview-table", "columns"),
        Output("wf-overview-table", "style_data_conditional"),
        Output("wf-aggregate-stats", "children"),
        Input("wf-interval", "n_intervals"),
        Input("wf-status-filter", "value"),
        Input("wf-time-range", "value"),
    )
    def update_workflow_table(
        _n_intervals: int, filter_status: str | None, time_range_hours: str | None
    ) -> tuple[
        list[dict[str, object]],
        list[dict[str, str]],
        list[dict[str, object]],
        object,
    ]:
        store = get_store()
        if store is None:
            raise PreventUpdate

        hours = int(time_range_hours or _DEFAULT_TIME_RANGE_HOURS)
        updated_after = datetime.now(timezone.utc) - timedelta(hours=hours)
        terminal_statuses = ("completed", "failed", "cancelled")

        def _fetch_active(status_filter: str | None) -> list[_WorkflowStatusLike]:
            return run(
                store.list_workflows(
                    status=status_filter,
                    limit=_PER_SHARD_LIMIT,
                    updated_after=updated_after,
                )
            )

        def _fetch_recent_terminal(
            status_filter: str | None,
        ) -> list[_WorkflowStatusLike]:
            # Use the dashboard-only R- index for terminal workflows so
            # we don't pay for a full T- range scan when there are
            # millions of archived rows. Returns newest-first; trim
            # client-side by the chosen time window.
            recent = run(
                store.list_recent_terminal(
                    limit=_RECENT_TERMINAL_LIMIT,
                    status=status_filter,
                )
            )
            return [w for w in recent if w.updated_at is None or w.updated_at >= updated_after]

        try:
            if filter_status == "active":
                # "Active" is a synthetic option that unions the two
                # non-terminal statuses. list_workflows takes a single
                # status, so run two queries and concatenate.
                workflows = _fetch_active("running") + _fetch_active("pending")
            elif filter_status in terminal_statuses:
                workflows = _fetch_recent_terminal(filter_status)
            elif not filter_status:
                # "All": fan out across both indexes — active rows via
                # the A- range, recent terminal rows via the R- index.
                workflows = (
                    _fetch_active("running")
                    + _fetch_active("pending")
                    + _fetch_recent_terminal(None)
                )
            else:
                workflows = _fetch_active(filter_status)
        except Exception as exc:
            LOG.exception("Failed to load workflow overview")
            raise PreventUpdate from exc

        return (
            _workflow_rows(workflows),
            _table_columns(),
            _data_conditional(workflows),
            _aggregate_stats(workflows),
        )


def _workflow_rows(workflows: list[_WorkflowStatusLike]) -> list[dict[str, object]]:
    sorted_workflows = sorted(
        workflows,
        key=lambda workflow: _normalize_timestamp(workflow.updated_at),
        reverse=True,
    )
    return [
        {
            "id": workflow.workflow_id,
            "workflow_id": workflow.workflow_id,
            "name": workflow.name,
            "status": str(workflow.status),
            "progress": f"{workflow.completed}/{workflow.total}",
            "velocity": _format_velocity(workflow),
            "running": workflow.running,
            "failed": workflow.failed,
            "created_at": _relative_time(workflow.created_at),
            "updated_at": _relative_time(workflow.updated_at),
        }
        for workflow in sorted_workflows
    ]


def _table_columns() -> list[dict[str, str]]:
    return [
        {"name": "ID", "id": "workflow_id"},
        {"name": "Name", "id": "name"},
        {"name": "Status", "id": "status"},
        {"name": "Progress", "id": "progress"},
        {"name": "Velocity", "id": "velocity"},
        {"name": "Running", "id": "running"},
        {"name": "Failed", "id": "failed"},
        {"name": "Created", "id": "created_at"},
        {"name": "Updated", "id": "updated_at"},
    ]


def _status_styles() -> list[dict[str, object]]:
    return [
        {
            "if": {"column_id": "status", "filter_query": f'{{status}} = "{status}"'},
            "color": color,
            "fontWeight": "600",
        }
        for status, color in _STATUS_COLORS.items()
    ]


_PROGRESS_COMPLETED_RGBA = "rgba(133, 153, 0, 0.35)"
_PROGRESS_FAILED_RGBA = "rgba(220, 50, 47, 0.35)"
_PROGRESS_RUNNING_RGBA = "rgba(38, 139, 210, 0.35)"


def _progress_bar_styles(workflows: list[_WorkflowStatusLike]) -> list[dict[str, object]]:
    """Render the ``progress`` cell as a stacked progress bar.

    Uses a CSS ``linear-gradient`` background per row, with three
    contiguous bands (completed → failed → running) so the bar
    encodes terminal vs. in-flight progress at a glance. The cell
    text (``completed/total``) stays overlaid via the regular
    DataTable rendering — no markdown required.
    """
    styles: list[dict[str, object]] = []
    for wf in workflows:
        total = max(wf.total, 1)
        completed_pct = min(100.0, 100.0 * wf.completed / total)
        failed_end_pct = min(100.0, 100.0 * (wf.completed + wf.failed) / total)
        running_end_pct = min(
            100.0,
            100.0 * (wf.completed + wf.failed + wf.running) / total,
        )
        # Render a horizontal stacked bar via a multi-stop linear gradient.
        # Each stop pair (start%, end%) defines a solid band; transparent
        # after the last stop reveals the cell background.
        gradient = (
            "linear-gradient(90deg, "
            f"{_PROGRESS_COMPLETED_RGBA} 0%, "
            f"{_PROGRESS_COMPLETED_RGBA} {completed_pct:.2f}%, "
            f"{_PROGRESS_FAILED_RGBA} {completed_pct:.2f}%, "
            f"{_PROGRESS_FAILED_RGBA} {failed_end_pct:.2f}%, "
            f"{_PROGRESS_RUNNING_RGBA} {failed_end_pct:.2f}%, "
            f"{_PROGRESS_RUNNING_RGBA} {running_end_pct:.2f}%, "
            f"transparent {running_end_pct:.2f}%)"
        )
        styles.append(
            {
                "if": {
                    "column_id": "progress",
                    "filter_query": f'{{workflow_id}} = "{wf.workflow_id}"',
                },
                "background": gradient,
                "fontVariantNumeric": "tabular-nums",
                "fontWeight": "500",
            }
        )
    return styles


def _data_conditional(workflows: list[_WorkflowStatusLike]) -> list[dict[str, object]]:
    return [
        *_status_styles(),
        *_progress_bar_styles(workflows),
        {
            "if": {"state": "selected"},
            "backgroundColor": "rgba(38, 139, 210, 0.12)",
            "border": "1px solid #268bd2",
        },
    ]


_TERMINAL_WF_STATES = {"completed", "failed", "cancelled"}


def _workflow_duration_seconds(workflow: _WorkflowStatusLike) -> float | None:
    """Wall-clock duration for a terminal workflow; ``None`` otherwise.

    Running and pending workflows return ``None`` so they don't pull the
    average down with an inflated "ongoing" elapsed.
    """
    status = str(workflow.status).lower().split(".")[-1]
    if status not in _TERMINAL_WF_STATES:
        return None
    created = _normalize_timestamp(workflow.created_at)
    updated = _normalize_timestamp(workflow.updated_at)
    seconds = (updated - created).total_seconds()
    if seconds <= 0:
        return None
    return seconds


def _aggregate_stats(workflows: list[_WorkflowStatusLike]) -> object:
    """Render the summary stat row above the workflow table.

    Two aggregates over the *currently visible* workflows:

    - **Avg seconds / workflow**: mean wall-clock duration of terminal
      workflows in view. Running workflows are excluded so the mean
      reflects observed end-to-end latency, not in-flight progress.
    - **Avg tasks / second**: total terminal tasks (completed + failed
      + skipped) across all visible workflows divided by the wall-clock
      window between the earliest ``created_at`` and the latest
      observation point (``updated_at`` for terminal workflows, ``now``
      for running ones). This is a system-wide throughput metric —
      independent of how many workers were involved — so concurrent
      workflows don't inflate the denominator.
    """
    durations = [d for d in (_workflow_duration_seconds(wf) for wf in workflows) if d is not None]
    if durations:
        avg_seconds = sum(durations) / len(durations)
        avg_seconds_text = _format_duration_seconds(avg_seconds)
        avg_seconds_caption = f"over {len(durations)} terminal workflow{_pluralize(len(durations))}"
    else:
        avg_seconds_text = "—"
        avg_seconds_caption = "no terminal workflows"

    now = datetime.now(timezone.utc)
    total_terminal_tasks = 0
    earliest_start: datetime | None = None
    latest_end: datetime | None = None
    for wf in workflows:
        terminal_tasks = wf.completed + wf.failed + wf.skipped
        if terminal_tasks <= 0:
            continue
        created = _normalize_timestamp(wf.created_at)
        status = str(wf.status).lower().split(".")[-1]
        end = _normalize_timestamp(wf.updated_at) if status in _TERMINAL_WF_STATES else now
        total_terminal_tasks += terminal_tasks
        if earliest_start is None or created < earliest_start:
            earliest_start = created
        if latest_end is None or end > latest_end:
            latest_end = end
    wall_clock_seconds = (
        (latest_end - earliest_start).total_seconds()
        if earliest_start is not None and latest_end is not None
        else 0.0
    )
    if wall_clock_seconds >= 1.0 and total_terminal_tasks > 0:
        tps = total_terminal_tasks / wall_clock_seconds
        tps_text = _format_tps(tps)
        tps_caption = f"{total_terminal_tasks} tasks / {_format_duration_seconds(wall_clock_seconds)} wall clock"
    else:
        tps_text = "—"
        tps_caption = "no completed tasks"

    return dbc.Row(
        [
            _stat_card("Avg seconds / workflow", avg_seconds_text, avg_seconds_caption),
            _stat_card("Avg tasks / second", tps_text, tps_caption),
        ],
        className="g-3",
    )


def _stat_card(label: str, value: str, caption: str) -> object:
    return dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.Div(label, className="text-muted small text-uppercase"),
                    html.Div(
                        value,
                        style={
                            "fontSize": "1.5rem",
                            "fontWeight": "600",
                            "fontVariantNumeric": "tabular-nums",
                        },
                    ),
                    html.Div(caption, className="text-muted small"),
                ]
            ),
            className="shadow-sm h-100",
        ),
        md=6,
        lg=4,
    )


def _format_duration_seconds(seconds: float) -> str:
    if seconds < 1.0:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.1f} s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {int(sec):02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes):02d}m"


def _format_tps(tps: float) -> str:
    if tps >= 100:
        return f"{tps:.0f} /s"
    if tps >= 10:
        return f"{tps:.1f} /s"
    if tps >= 1:
        return f"{tps:.2f} /s"
    return f"{tps:.3f} /s"


def _pluralize(count: int) -> str:
    return "" if count == 1 else "s"


def _format_velocity(workflow: _WorkflowStatusLike) -> str:
    """Average terminal-task throughput since the workflow was submitted.

    Returns ``<value> /min`` formatted to a reasonable precision, or
    ``"—"`` when not enough wall-clock has passed (< 1 s) or no tasks
    have finished yet. For running workflows the denominator is
    ``now - created_at``; for terminal workflows it freezes at
    ``updated_at - created_at`` so the column doesn't decay after the
    run is done.
    """
    terminal_tasks = workflow.completed + workflow.failed + workflow.skipped
    if terminal_tasks <= 0:
        return "—"
    created = _normalize_timestamp(workflow.created_at)
    status = str(workflow.status).lower().split(".")[-1]
    if status in _TERMINAL_WF_STATES:
        end = _normalize_timestamp(workflow.updated_at)
    else:
        end = datetime.now(timezone.utc)
    elapsed = (end - created).total_seconds()
    if elapsed < 1.0:
        return "—"
    per_minute = terminal_tasks / elapsed * 60.0
    if per_minute >= 100:
        return f"{per_minute:.0f} /min"
    if per_minute >= 10:
        return f"{per_minute:.1f} /min"
    return f"{per_minute:.2f} /min"


def _relative_time(value: datetime) -> str:
    timestamp = _normalize_timestamp(value)
    delta_seconds = max(int((datetime.now(timezone.utc) - timestamp).total_seconds()), 0)

    if delta_seconds < 5:
        return "just now"
    if delta_seconds < 60:
        return f"{delta_seconds}s ago"

    minutes = delta_seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"

    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"

    days = hours // 24
    if days < 7:
        return f"{days}d ago"

    weeks = days // 7
    if weeks < 5:
        return f"{weeks}w ago"

    months = days // 30
    if months < 12:
        return f"{months}mo ago"

    years = days // 365
    return f"{years}y ago"


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
