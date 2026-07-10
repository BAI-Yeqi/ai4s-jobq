# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Interactive DAG visualization for workflow tasks using Cytoscape."""

from __future__ import annotations

import logging
import math
import random
from collections import deque
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import dash_bootstrap_components as dbc
import dash_cytoscape as cyto  # type: ignore[import-not-found]
from dash import Input, Output, State, dcc, html
from dash.exceptions import PreventUpdate

from ..utils.workflow_store import get_store, run

if TYPE_CHECKING:
    from ai4s.jobq.workflow.entities import TaskStatus

LOG = logging.getLogger(__name__)

_SUBSET_DEFAULT_MAX_TASK_COUNT = 45
_MAX_CANDIDATES_TO_TRY = 800

# Load the dagre layout algorithm for hierarchical graphs
cyto.load_extra_layouts()

_STATUS_COLORS = {
    "completed": "#859900",
    "running": "#268bd2",
    "pending": "#b58900",
    "ready": "#b58900",
    "ready_pending_budget": "#b58900",
    "failed": "#dc322f",
    "upstream_failed": "#dc322f",
    "cancelled": "#93a1a1",
    "skipped": "#6c71c4",
}

_STATUS_SHAPES = {
    "completed": "ellipse",
    "running": "diamond",
    "pending": "ellipse",
    "ready": "ellipse",
    "ready_pending_budget": "ellipse",
    "failed": "triangle",
    "upstream_failed": "vee",
    "cancelled": "rectangle",
    "skipped": "rectangle",
}

_LAYOUT_OPTIONS = [
    {"label": "Hierarchical (top-down)", "value": "dagre-tb"},
    {"label": "Hierarchical (left-right)", "value": "dagre-lr"},
    {"label": "Breadth-first", "value": "breadthfirst"},
    {"label": "Concentric", "value": "concentric"},
]

_COLOR_MODE_OPTIONS = [
    {"label": "Status", "value": "status"},
    {"label": "Queue", "value": "queue"},
    {"label": "Completion time", "value": "duration"},
    {"label": "Relative timeline", "value": "timeline"},
]

_QUEUE_PALETTE = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]

# ---------------------------------------------------------------------------
# Viridis palette (perceptually uniform, colorblind-safe)
# Sampled at 9 evenly-spaced stops from the matplotlib viridis colormap.
# ---------------------------------------------------------------------------
_VIRIDIS_STOPS: list[tuple[float, int, int, int]] = [
    (0.000, 68, 1, 84),
    (0.125, 72, 36, 117),
    (0.250, 64, 67, 135),
    (0.375, 52, 94, 141),
    (0.500, 41, 121, 142),
    (0.625, 33, 148, 140),
    (0.750, 59, 179, 113),
    (0.875, 141, 206, 62),
    (1.000, 253, 231, 37),
]


def _viridis(t: float) -> str:
    """Interpolate the viridis colormap at position *t* in [0, 1]."""
    t = max(0.0, min(1.0, t))
    # Find the two surrounding stops
    for i in range(len(_VIRIDIS_STOPS) - 1):
        t0, r0, g0, b0 = _VIRIDIS_STOPS[i]
        t1, r1, g1, b1 = _VIRIDIS_STOPS[i + 1]
        if t <= t1:
            f = (t - t0) / (t1 - t0) if t1 != t0 else 0.0
            r = int(r0 + f * (r1 - r0))
            g = int(g0 + f * (g1 - g0))
            b = int(b0 + f * (b1 - b0))
            return f"rgb({r}, {g}, {b})"
    # Fallback to last stop
    _, r, g, b = _VIRIDIS_STOPS[-1]
    return f"rgb({r}, {g}, {b})"


def _task_duration_seconds(task: TaskStatus) -> float | None:
    """Return wall-clock duration in seconds, or None if not available."""
    if task.started_at is None or task.completed_at is None:
        return None
    start = (
        task.started_at.astimezone(UTC)
        if task.started_at.tzinfo
        else task.started_at.replace(tzinfo=UTC)
    )
    end = (
        task.completed_at.astimezone(UTC)
        if task.completed_at.tzinfo
        else task.completed_at.replace(tzinfo=UTC)
    )
    return max((end - start).total_seconds(), 0.0)


def _format_duration(seconds: float) -> str:
    """Human-readable duration string."""
    if seconds < 1:
        return "<1s"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    return f"{hours:.1f}h"


def _normalize_ts(ts: datetime | None) -> datetime | None:
    """Ensure a timestamp is UTC-aware, or return None."""
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def layout() -> html.Div:
    """Build the workflow graph component layout."""
    return html.Div(
        [
            dbc.Card(
                dbc.CardBody(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Label("Layout", className="small text-muted"),
                                        dcc.Dropdown(
                                            id="wf-graph-layout",
                                            options=_LAYOUT_OPTIONS,  # type: ignore[arg-type]
                                            value="dagre-tb",
                                            clearable=False,
                                        ),
                                    ],
                                    md=3,
                                ),
                                dbc.Col(
                                    [
                                        dbc.Label("Color by", className="small text-muted"),
                                        dcc.Dropdown(
                                            id="wf-graph-color-mode",
                                            options=_COLOR_MODE_OPTIONS,  # type: ignore[arg-type]
                                            value="status",
                                            clearable=False,
                                        ),
                                    ],
                                    md=3,
                                ),
                                dbc.Col(
                                    dbc.Checklist(
                                        id="wf-graph-options",
                                        options=[
                                            {"label": " Show task names", "value": "labels"},
                                            {"label": " Fit to view", "value": "fit"},
                                        ],
                                        value=["labels", "fit"],
                                        inline=True,
                                        className="mt-4",
                                    ),
                                    md=3,
                                ),
                                dbc.Col(
                                    [
                                        dbc.Label("Subset seed", className="small text-muted"),
                                        dbc.Button(
                                            "Random start",
                                            id="wf-graph-random-start-btn",
                                            color="secondary",
                                            outline=True,
                                            className="w-100",
                                            n_clicks=0,
                                        ),
                                    ],
                                    md=2,
                                ),
                                dbc.Col(
                                    [
                                        dbc.Label("Max nodes", className="small text-muted"),
                                        dcc.Slider(
                                            id="wf-graph-max-nodes",
                                            min=20,
                                            max=200,
                                            step=5,
                                            value=_SUBSET_DEFAULT_MAX_TASK_COUNT,
                                            marks={
                                                20: "20",
                                                35: "35",
                                                50: "50",
                                                75: "75",
                                                100: "100",
                                                150: "150",
                                                200: "200",
                                            },
                                            tooltip={
                                                "always_visible": False,
                                                "placement": "bottom",
                                            },
                                        ),
                                    ],
                                    md=4,
                                ),
                            ],
                            className="g-3 mb-3",
                        ),
                        html.Div(
                            cyto.Cytoscape(
                                id="wf-graph",
                                elements=[],
                                layout={"name": "dagre", "rankDir": "TB"},
                                style={"width": "100%", "height": "600px"},
                                stylesheet=_stylesheet(),
                                responsive=True,
                                minZoom=0.1,
                                maxZoom=3.0,
                                userZoomingEnabled=True,
                                userPanningEnabled=True,
                                boxSelectionEnabled=False,
                            ),
                            style={
                                "border": "1px solid #eee8d5",
                                "borderRadius": "6px",
                                "backgroundColor": "#fdf6e3",
                            },
                        ),
                        html.Div(id="wf-graph-legend", className="mt-2"),
                        html.Div(id="wf-graph-info", className="mt-2"),
                    ]
                ),
                className="shadow-sm",
            ),
        ]
    )


def register_callbacks(app) -> None:
    """Register Dash callbacks for the workflow graph component."""

    @app.callback(
        Output("wf-graph", "elements"),
        Output("wf-graph", "layout"),
        Output("wf-graph-legend", "children"),
        Input("wf-selected-workflow-id", "data"),
        Input("wf-detail-load-btn", "n_clicks"),
        Input("wf-graph-layout", "value"),
        Input("wf-graph-color-mode", "value"),
        Input("wf-graph-options", "value"),
        Input("wf-graph-max-nodes", "value"),
        Input("wf-graph-random-start-btn", "n_clicks"),
        State("wf-detail-id-input", "value"),
    )
    def update_graph(
        selected_workflow_id: str | None,
        _n_clicks: int | None,
        layout_value: str,
        color_mode: str,
        options: list[str],
        max_subset_nodes: int | None,
        random_start_clicks: int | None,
        input_workflow_id: str | None,
    ) -> tuple[list[dict], dict, html.Div | None]:
        store = get_store()
        if store is None:
            raise PreventUpdate

        workflow_id = (selected_workflow_id or "").strip() or (input_workflow_id or "").strip()

        # In local preview mode there is typically a single workflow and no
        # selection yet; auto-pick the first pending workflow so the graph
        # renders immediately on page load.
        if not workflow_id:
            try:
                candidates = run(store.list_workflows(status="pending", limit=1))
                if not candidates:
                    candidates = run(store.list_workflows(limit=1))
                if candidates:
                    workflow_id = candidates[0].workflow_id
            except Exception:
                LOG.debug("Failed to auto-select default workflow", exc_info=True)

        if not workflow_id:
            raise PreventUpdate

        try:
            status = run(store.get_workflow_status(workflow_id, include_tasks=True))
        except Exception as exc:
            LOG.warning("Failed to load workflow graph for %s", workflow_id, exc_info=True)
            raise PreventUpdate from exc

        command_map: dict[str, str] = {}
        get_commands = getattr(store, "get_task_command_map", None)
        if callable(get_commands):
            try:
                command_map = get_commands()
            except Exception:
                LOG.debug("Task command map unavailable from store", exc_info=True)

        task_subset = status.tasks
        truncated_up: set[str] = set()
        truncated_down: set[str] = set()
        graph_message: dbc.Alert | None = None
        subset_limit = max(20, int(max_subset_nodes or _SUBSET_DEFAULT_MAX_TASK_COUNT))

        if len(status.tasks) > subset_limit:
            focused = _select_focus_subset(
                status.tasks,
                max_nodes=subset_limit,
                random_seed=(random_start_clicks or 0) or None,
            )
            if focused is None:
                graph_message = dbc.Alert(
                    (
                        "Workflow graph not rendered: this workflow is too large and no "
                        f"dependency-chain subset under {subset_limit} tasks could be found."
                    ),
                    color="warning",
                    className="mb-2 py-2",
                )
                graph_layout = _build_layout(layout_value, fit="fit" in (options or []))
                return [], graph_layout, html.Div([graph_message])

            sample_task, sample_subset, sample_mode, truncated_up, truncated_down, type_coverage = (
                focused
            )
            task_subset = sample_subset
            mode_suffix = (
                "using up-then-down neighborhood traversal"
                if sample_mode == "updown"
                else "using subset traversal"
            )
            graph_message = dbc.Alert(
                (
                    f"Showing subset view ({len(task_subset)} of {len(status.tasks)} tasks), "
                    f"anchored at sample task '{sample_task}' {mode_suffix}; "
                    f"task-type coverage {type_coverage}."
                ),
                color="warning",
                className="mb-2 py-2",
            )

        show_labels = "labels" in (options or [])
        elements = _build_elements(
            task_subset,
            show_labels=show_labels,
            color_mode=color_mode or "status",
            command_map=command_map,
            truncated_up=truncated_up,
            truncated_down=truncated_down,
        )
        graph_layout = _build_layout(layout_value, fit="fit" in (options or []))
        legend = _build_legend(task_subset, color_mode or "status")

        if graph_message is not None:
            legend = html.Div([graph_message, legend] if legend is not None else [graph_message])

        return elements, graph_layout, legend

    @app.callback(
        Output("wf-graph-info", "children"),
        Input("wf-graph", "mouseoverNodeData"),
        Input("wf-graph", "tapNodeData"),
    )
    def show_node_info(hover_data: dict | None, tap_data: dict | None) -> dbc.Alert | None:
        from dash import ctx

        triggered_id = getattr(ctx, "triggered_id", None)
        data = hover_data or tap_data if triggered_id == "wf-graph" else tap_data or hover_data
        if not data:
            raise PreventUpdate

        status_str = data.get("status", "unknown")
        color = _STATUS_COLORS.get(status_str, "#586e75")

        info_parts = [
            html.Strong(data.get("label", data.get("id", "?"))),
            html.Span(f"  —  {status_str}", style={"color": color, "fontWeight": "600"}),
        ]

        if data.get("duration"):
            info_parts.append(html.Span(f"  •  Duration: {data['duration']}"))
        if data.get("timeline"):
            info_parts.append(html.Span(f"  •  Completed: {data['timeline']} from workflow start"))
        if data.get("queue"):
            info_parts.append(html.Span(f"  •  Queue: {data['queue']}", className="text-muted"))
        if data.get("error"):
            info_parts.append(html.Div(data["error"], className="text-danger mt-1 small"))
        if data.get("deps"):
            info_parts.append(
                html.Div(
                    f"Dependencies: {data['deps']}",
                    className="text-muted mt-1 small",
                )
            )
        if data.get("command"):
            info_parts.append(
                html.Div(
                    [
                        html.Div("Command", className="text-muted mt-1 small fw-semibold"),
                        html.Pre(
                            data["command"],
                            className="small mb-0",
                            style={
                                "whiteSpace": "pre-wrap",
                                "wordBreak": "break-word",
                                "marginTop": "0.25rem",
                            },
                        ),
                    ]
                )
            )

        return dbc.Alert(info_parts, color="light", className="mb-0 py-2")


def _build_elements(
    tasks: dict[str, TaskStatus],
    *,
    show_labels: bool = True,
    color_mode: str = "status",
    command_map: dict[str, str] | None = None,
    truncated_up: set[str] | None = None,
    truncated_down: set[str] | None = None,
) -> list[dict]:
    """Convert workflow tasks into Cytoscape elements (nodes + edges)."""
    nodes: list[dict] = []
    edges: list[dict] = []
    queue_colors = _build_queue_color_map(tasks)

    # Pre-compute per-task durations (for "duration" mode)
    duration_map: dict[str, float] = {}
    # Pre-compute relative completion offsets (for "timeline" mode)
    timeline_map: dict[str, float] = {}

    if color_mode == "duration":
        for name, task in tasks.items():
            dur = _task_duration_seconds(task)
            if dur is not None:
                duration_map[name] = dur

    if color_mode == "timeline":
        # Find the earliest started_at across the whole workflow
        all_starts = [ts for t in tasks.values() if (ts := _normalize_ts(t.started_at)) is not None]
        epoch = min(all_starts) if all_starts else None

        if epoch is not None:
            for name, task in tasks.items():
                completed = _normalize_ts(task.completed_at)
                if completed is not None:
                    timeline_map[name] = (completed - epoch).total_seconds()

    # Compute color-scale ranges
    if color_mode == "duration" and duration_map:
        val_min = min(duration_map.values())
        val_max = max(duration_map.values())
        use_log = val_max > 0 and (val_max / max(val_min, 0.1)) > 10
    elif color_mode == "timeline" and timeline_map:
        val_min = min(timeline_map.values())
        val_max = max(timeline_map.values())
        use_log = val_max > 0 and (val_max / max(val_min, 0.1)) > 10
    else:
        val_min = val_max = 0.0
        use_log = False

    # Select the active value map
    active_map = duration_map if color_mode == "duration" else timeline_map

    for name, task in tasks.items():
        status_str = str(task.status)
        label = name if show_labels else ""

        if len(label) > 20:
            label = label[:18] + "…"

        if color_mode == "queue":
            queue_name = task.queue or "<default>"
            color = queue_colors.get(queue_name, "#586e75")
            border_color = color
        elif color_mode in ("duration", "timeline") and name in active_map:
            color = _duration_color(active_map[name], val_min, val_max, use_log=use_log)
            border_color = color
        else:
            color = _STATUS_COLORS.get(status_str, "#586e75")
            border_color = color

        dur = _task_duration_seconds(task)
        duration_label = _format_duration(dur) if dur is not None else ""
        timeline_label = f"+{_format_duration(timeline_map[name])}" if name in timeline_map else ""

        node_data = {
            "id": name,
            "label": label,
            "status": status_str,
            "color": color,
            "border_color": border_color,
            "shape": _STATUS_SHAPES.get(status_str, "ellipse"),
            "queue": task.queue or "",
            "error": task.error or "",
            "deps": ", ".join(task.depends_on) if task.depends_on else "",
            "duration": duration_label,
            "timeline": timeline_label,
            "command": (command_map or {}).get(name, ""),
        }
        nodes.append({"data": node_data, "classes": status_str})

        edges.extend(
            {
                "data": {
                    "source": dep,
                    "target": name,
                    "source_status": str(tasks[dep].status),
                }
            }
            for dep in task.depends_on
            if dep in tasks
        )

    for name in sorted(truncated_up or set()):
        if name not in tasks:
            continue
        ellipsis_id = f"ellipsis-up::{name}"
        nodes.append(
            {
                "data": {
                    "id": ellipsis_id,
                    "label": "...",
                    "status": "more",
                    "color": "#b0b7bf",
                    "border_color": "#8f99a3",
                    "shape": "round-rectangle",
                    "queue": "",
                    "error": "",
                    "deps": "",
                    "duration": "",
                    "timeline": "",
                    "command": "",
                },
                "classes": "ellipsis-node",
            }
        )
        edges.append(
            {
                "data": {
                    "source": ellipsis_id,
                    "target": name,
                    "source_status": "more",
                }
            }
        )

    for name in sorted(truncated_down or set()):
        if name not in tasks:
            continue
        ellipsis_id = f"ellipsis-down::{name}"
        nodes.append(
            {
                "data": {
                    "id": ellipsis_id,
                    "label": "...",
                    "status": "more",
                    "color": "#b0b7bf",
                    "border_color": "#8f99a3",
                    "shape": "round-rectangle",
                    "queue": "",
                    "error": "",
                    "deps": "",
                    "duration": "",
                    "timeline": "",
                    "command": "",
                },
                "classes": "ellipsis-node",
            }
        )
        edges.append(
            {
                "data": {
                    "source": name,
                    "target": ellipsis_id,
                    "source_status": str(tasks[name].status),
                }
            }
        )

    return nodes + edges


def _build_queue_color_map(tasks: dict[str, TaskStatus]) -> dict[str, str]:
    queues = sorted({(task.queue or "<default>") for task in tasks.values()})
    return {queue: _QUEUE_PALETTE[i % len(_QUEUE_PALETTE)] for i, queue in enumerate(queues)}


def _select_focus_subset(
    tasks: dict[str, TaskStatus], *, max_nodes: int, random_seed: int | None = None
) -> tuple[str, dict[str, TaskStatus], str, set[str], set[str], str] | None:
    """Pick a sample-task focus subset with up-first-then-down traversal."""
    children: dict[str, list[str]] = {name: [] for name in tasks}
    parents: dict[str, list[str]] = {name: [] for name in tasks}
    task_types = {name: _task_type_key(name) for name in tasks}
    all_types = {task_types[name] for name in tasks}

    for name, task in tasks.items():
        for dep in task.depends_on:
            if dep in children:
                children[dep].append(name)
                parents[name].append(dep)

    for node_children in children.values():
        node_children.sort()

    for node_parents in parents.values():
        node_parents.sort()

    childful_candidates = [name for name in tasks if children.get(name)]
    small_fanout = [name for name in childful_candidates if len(children[name]) <= 16]
    large_fanout = [name for name in childful_candidates if len(children[name]) > 16]

    ordered_candidates = [
        *sorted(small_fanout, key=lambda name: (len(children[name]), name)),
        *sorted(large_fanout, key=lambda name: (len(children[name]), name)),
        *sorted(name for name in tasks if not children.get(name)),
    ]

    if random_seed is not None:
        rng = random.Random(random_seed)  # noqa: S311 - UI-only deterministic shuffling
        rng.shuffle(ordered_candidates)

    seen_candidates: set[str] = set()
    best_sample: str | None = None
    best_subset: set[str] | None = None
    best_truncated_up: set[str] = set()
    best_truncated_down: set[str] = set()
    best_type_coverage = -1
    best_size = -1

    for sample in ordered_candidates:
        if sample in seen_candidates:
            continue
        seen_candidates.add(sample)
        if len(seen_candidates) > _MAX_CANDIDATES_TO_TRY:
            break

        candidate_subset, truncated_up, truncated_down = _walk_up_then_down(
            start=sample,
            parents=parents,
            children=children,
            limit=max_nodes,
        )
        if not candidate_subset:
            continue

        type_coverage = len({task_types[name] for name in candidate_subset})
        candidate_size = len(candidate_subset)
        if (
            best_subset is None
            or type_coverage > best_type_coverage
            or (type_coverage == best_type_coverage and candidate_size > best_size)
        ):
            best_sample = sample
            best_subset = candidate_subset
            best_truncated_up = truncated_up
            best_truncated_down = truncated_down
            best_type_coverage = type_coverage
            best_size = candidate_size

        if type_coverage == len(all_types) and candidate_size >= max_nodes:
            break

    if best_subset is None or best_sample is None:
        return None

    ordered_names = sorted(best_subset)
    subset = {name: tasks[name] for name in ordered_names}
    final_up = {name for name in subset if any(p not in subset for p in parents[name])}
    final_down = {name for name in subset if any(c not in subset for c in children[name])}
    # Keep traversal-derived truncation info merged with computed boundary checks.
    final_up |= {name for name in best_truncated_up if name in subset}
    final_down |= {name for name in best_truncated_down if name in subset}
    coverage_label = f"{best_type_coverage}/{len(all_types)}"
    return best_sample, subset, "updown", final_up, final_down, coverage_label


def _task_type_key(task_name: str) -> str:
    """Infer a task-type prefix from a task name for subset coverage scoring."""
    parts = task_name.split("-")
    if not parts:
        return task_name

    type_parts: list[str] = []
    for index, part in enumerate(parts):
        if index > 0 and _looks_like_identity_token(part):
            break
        type_parts.append(part)

    return "-".join(type_parts) or task_name


def _looks_like_identity_token(part: str) -> bool:
    """Heuristic for task-name identity segments such as numeric indexes and hashes."""
    if part.isdigit():
        return True

    is_hex = all(ch in "0123456789abcdef" for ch in part.lower())
    return bool(is_hex and len(part) >= 8)


def _walk_up_then_down(
    *,
    start: str,
    parents: dict[str, list[str]],
    children: dict[str, list[str]],
    limit: int,
) -> tuple[set[str], set[str], set[str]]:
    """Traverse from *start* by visiting parents before dependents at each step."""
    visited: set[str] = set()
    stack: list[str] = [start]

    while stack:
        node = stack.pop()
        if node in visited:
            continue

        visited.add(node)
        if len(visited) >= limit:
            break

        node_parents = parents.get(node, [])
        node_children = children.get(node, [])

        # LIFO stack: push children first, then parents, so parents are processed first.
        stack.extend(child for child in reversed(node_children) if child not in visited)
        stack.extend(parent for parent in reversed(node_parents) if parent not in visited)

    truncated_up = {
        name for name in visited if any(parent not in visited for parent in parents[name])
    }
    truncated_down = {
        name for name in visited if any(child not in visited for child in children[name])
    }
    return visited, truncated_up, truncated_down


def _bounded_reachable(
    starts: set[str],
    *,
    adjacency: dict[str, list[str]],
    limit: int,
) -> set[str] | None:
    """Return reachable nodes from starts, or None if the set exceeds *limit*."""
    out: set[str] = set()
    queue: deque[str] = deque(sorted(starts))

    while queue:
        node = queue.popleft()
        if node in out or node not in adjacency:
            continue

        out.add(node)
        if len(out) > limit:
            return None

        for nxt in adjacency[node]:
            if nxt not in out:
                queue.append(nxt)

    return out


def _duration_color(value: float, vmin: float, vmax: float, *, use_log: bool = False) -> str:
    """Map a duration value to a viridis color."""
    if use_log:
        # Shift to avoid log(0); use log1p-like transform
        log_val = math.log1p(value)
        log_min = math.log1p(vmin)
        log_max = math.log1p(vmax)
        t = (log_val - log_min) / (log_max - log_min) if log_max != log_min else 0.5
    else:
        t = (value - vmin) / (vmax - vmin) if vmax != vmin else 0.5
    return _viridis(t)


def _build_legend(tasks: dict[str, TaskStatus], color_mode: str) -> html.Div | None:
    """Build a color legend appropriate for the current color mode."""
    if color_mode == "status":
        items = [
            html.Span(
                [
                    html.Span(
                        "●",
                        style={
                            "color": color,
                            "fontSize": "14px",
                            "marginRight": "4px",
                        },
                    ),
                    html.Span(label, className="small"),
                ],
                style={"marginRight": "14px"},
            )
            for label, color in [
                ("Running", _STATUS_COLORS["running"]),
                ("Completed", _STATUS_COLORS["completed"]),
                ("Pending", _STATUS_COLORS["pending"]),
                ("Failed", _STATUS_COLORS["failed"]),
                ("Cancelled", _STATUS_COLORS["cancelled"]),
                ("Skipped", _STATUS_COLORS["skipped"]),
            ]
        ]
        return html.Div(
            items,
            className="d-flex flex-wrap align-items-center",
            style={"color": "#586e75"},
        )

    if color_mode == "queue":
        queue_colors = _build_queue_color_map(tasks)
        return html.Div(
            [
                html.Div(
                    "Queue colors", className="small text-muted mb-2", style={"fontWeight": "600"}
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Span(
                                    "●",
                                    style={
                                        "color": color,
                                        "fontSize": "14px",
                                        "marginRight": "6px",
                                    },
                                ),
                                html.Span(queue, className="small text-muted"),
                            ],
                            style={"marginBottom": "2px"},
                        )
                        for queue, color in queue_colors.items()
                    ],
                    style={"columnCount": 2, "columnGap": "16px"},
                ),
            ]
        )

    if color_mode == "timeline":
        return _gradient_legend(tasks, mode="timeline")

    # duration
    return _gradient_legend(tasks, mode="duration")


def _gradient_legend(tasks: dict[str, TaskStatus], *, mode: str) -> html.Div:
    """Build a viridis gradient legend for duration or timeline modes."""
    if mode == "timeline":
        all_starts = [ts for t in tasks.values() if (ts := _normalize_ts(t.started_at)) is not None]
        epoch = min(all_starts) if all_starts else None
        values = []
        if epoch is not None:
            for task in tasks.values():
                completed = _normalize_ts(task.completed_at)
                if completed is not None:
                    values.append((completed - epoch).total_seconds())
        title = "Relative timeline (from first task start)"
        fmt_label = lambda v: f"+{_format_duration(v)}"  # noqa: E731
    else:
        values = [
            dur for task in tasks.values() if (dur := _task_duration_seconds(task)) is not None
        ]
        title = "Completion time"
        fmt_label = _format_duration

    if not values:
        return html.Div(
            "No completed tasks with timing data.",
            className="small text-muted",
        )

    val_min = min(values)
    val_max = max(values)

    n_stops = 20
    gradient_colors = [_viridis(i / (n_stops - 1)) for i in range(n_stops)]
    gradient_css = ", ".join(gradient_colors)

    return html.Div(
        [
            html.Div(
                title,
                className="small text-muted mb-1",
                style={"fontWeight": "600"},
            ),
            html.Div(
                style={
                    "height": "12px",
                    "borderRadius": "6px",
                    "background": f"linear-gradient(to right, {gradient_css})",
                    "border": "1px solid #eee8d5",
                },
            ),
            html.Div(
                [
                    html.Span(fmt_label(val_min), className="small text-muted"),
                    html.Span(fmt_label((val_min + val_max) / 2), className="small text-muted"),
                    html.Span(fmt_label(val_max), className="small text-muted"),
                ],
                className="d-flex justify-content-between",
                style={"marginTop": "2px"},
            ),
        ],
        style={"maxWidth": "400px"},
    )


def _build_layout(layout_value: str, *, fit: bool = True) -> dict:
    """Build the Cytoscape layout configuration."""
    if layout_value == "dagre-tb":
        return {
            "name": "dagre",
            "rankDir": "TB",
            "spacingFactor": 1.2,
            "nodeSep": 30,
            "rankSep": 60,
            "fit": fit,
            "padding": 30,
        }
    if layout_value == "dagre-lr":
        return {
            "name": "dagre",
            "rankDir": "LR",
            "spacingFactor": 1.2,
            "nodeSep": 30,
            "rankSep": 80,
            "fit": fit,
            "padding": 30,
        }
    if layout_value == "breadthfirst":
        return {
            "name": "breadthfirst",
            "directed": True,
            "spacingFactor": 1.1,
            "fit": fit,
            "padding": 30,
        }
    # concentric
    return {
        "name": "concentric",
        "fit": fit,
        "padding": 30,
        "minNodeSpacing": 40,
    }


def _stylesheet() -> list[dict]:
    """Build the Cytoscape stylesheet for nodes and edges."""
    return [
        # Base node style
        {
            "selector": "node",
            "style": {
                "label": "data(label)",
                "background-color": "data(color)",
                "shape": "data(shape)",
                "width": 28,
                "height": 28,
                "font-size": "9px",
                "font-family": "Segoe UI, sans-serif",
                "color": "#586e75",
                "text-valign": "bottom",
                "text-halign": "center",
                "text-margin-y": 6,
                "border-width": 2,
                "border-color": "data(border_color)",
                "border-opacity": 0.6,
                "background-opacity": 0.85,
                "text-max-width": "80px",
                "text-wrap": "ellipsis",
                "min-zoomed-font-size": 8,
            },
        },
        # Running nodes pulse larger
        {
            "selector": "node.running",
            "style": {
                "width": 36,
                "height": 36,
                "border-width": 3,
                "border-style": "double",
                "background-opacity": 1.0,
            },
        },
        # Failed/error nodes are emphasized
        {
            "selector": "node.failed, node.upstream_failed",
            "style": {
                "width": 32,
                "height": 32,
                "border-width": 3,
            },
        },
        # Completed nodes are subtle
        {
            "selector": "node.completed",
            "style": {
                "background-opacity": 0.7,
                "border-opacity": 0.4,
            },
        },
        # Skipped/cancelled are dimmed
        {
            "selector": "node.skipped, node.cancelled",
            "style": {
                "background-opacity": 0.4,
                "border-opacity": 0.3,
                "color": "#93a1a1",
            },
        },
        # Base edge style
        {
            "selector": "edge",
            "style": {
                "curve-style": "bezier",
                "target-arrow-shape": "triangle",
                "target-arrow-color": "#93a1a1",
                "line-color": "#93a1a1",
                "width": 1.5,
                "arrow-scale": 0.8,
                "opacity": 0.6,
            },
        },
        # Edges from completed nodes are green-tinted
        {
            "selector": "edge[source_status = 'completed']",
            "style": {
                "line-color": "#859900",
                "target-arrow-color": "#859900",
                "opacity": 0.4,
            },
        },
        # Edges from failed nodes are red
        {
            "selector": "edge[source_status = 'failed']",
            "style": {
                "line-color": "#dc322f",
                "target-arrow-color": "#dc322f",
                "opacity": 0.7,
            },
        },
        # Edges from running nodes are blue
        {
            "selector": "edge[source_status = 'running']",
            "style": {
                "line-color": "#268bd2",
                "target-arrow-color": "#268bd2",
                "opacity": 0.8,
                "width": 2,
            },
        },
        {
            "selector": "node.ellipsis-node",
            "style": {
                "width": 22,
                "height": 22,
                "font-size": "12px",
                "font-weight": "700",
                "color": "#5f6b77",
                "text-valign": "center",
                "text-halign": "center",
                "text-margin-y": 0,
                "border-style": "dashed",
                "border-width": 2,
                "background-opacity": 0.8,
            },
        },
        {
            "selector": "edge[source_status = 'more']",
            "style": {
                "line-style": "dashed",
                "line-color": "#8f99a3",
                "target-arrow-color": "#8f99a3",
                "opacity": 0.8,
            },
        },
        # Hover effect
        {
            "selector": "node:selected",
            "style": {
                "border-width": 4,
                "border-color": "#268bd2",
                "background-opacity": 1.0,
                "z-index": 999,
            },
        },
    ]
