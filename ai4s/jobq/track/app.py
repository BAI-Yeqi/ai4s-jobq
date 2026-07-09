# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from __future__ import annotations

import logging
import webbrowser
from threading import Timer

import dash_bootstrap_components as dbc
from dash import Dash, Input, Output, dcc, html

from .components import (
    active_environments,
    active_workers,
    connection_info,
    cpu_utilization,
    env_efficiency,
    errors,
    preemption_events,
    preemptions_by_env,
    queue_size,
    ram_utilization,
    stat_cards,
    task_runtimes,
    tasks_completed,
    tasks_completed_trend,
    tasks_starting,
    worker_churn,
    worker_lifetime,
    workflow_detail,
    workflow_graph,
    workflow_health,
    workflow_overview,
    workflow_tasks,
)

LOG = logging.getLogger(__name__)


def _has_workflow_store() -> bool:
    """Return True if workflow store *configuration* is present.

    Only checks the environment variable — the actual connection is
    deferred to the first callback so startup stays instant.
    """
    from .utils.workflow_store import has_workflow_source

    return has_workflow_source()


def _workflow_tab() -> html.Div:
    """Build the Workflow Dashboard tab content."""
    return html.Div(
        [
            workflow_health.layout(),
            html.Hr(className="my-4"),
            html.H4("Workflows", className="mb-3"),
            workflow_overview.layout(),
            html.Hr(className="my-4"),
            html.H4("Workflow Detail", className="mb-3"),
            workflow_detail.layout(),
            html.Hr(className="my-4"),
            html.H4("Workflow Graph", className="mb-3"),
            workflow_graph.layout(),
            html.Hr(className="my-4"),
            html.H4("Task Explorer", className="mb-3"),
            workflow_tasks.layout(),
        ],
        className="mt-3",
    )


def _has_log_analytics() -> bool:
    """Return True if Log Analytics workspace is configured."""
    import os

    return bool(os.environ.get("JOBQ_LA_WORKSPACE_ID", "").strip())


def _register_url_routing(app: Dash, *, dual_tab: bool) -> None:
    """Register callbacks that sync browser URL ↔ dashboard state.

    URL scheme:
        /                       → Queue tab (or workflow overview if single-tab)
        /workflow                → Workflow tab, overview
        /workflow/<workflow_id>  → Workflow tab, with that workflow selected
    """
    from urllib.parse import parse_qs, urlencode

    from dash import State
    from dash.exceptions import PreventUpdate

    if dual_tab:
        # URL → tab selection: /workflow* activates the workflow tab.
        @app.callback(
            Output("main-tabs", "value"),
            Input("wf-url", "pathname"),
        )
        def route_tab_from_url(pathname: str | None) -> str:
            if pathname and pathname.startswith("/workflow"):
                return "workflow-tab"
            return "queue-tab"

    # URL → workflow selection: /workflow/<id> populates the store.
    # This is the primary Output for wf-selected-workflow-id and fires
    # on page load to restore bookmarked state from the URL.
    @app.callback(
        Output("wf-selected-workflow-id", "data"),
        Input("wf-url", "pathname"),
    )
    def load_workflow_from_url(pathname: str | None) -> str:
        if not pathname:
            raise PreventUpdate
        prefix = "/workflow/"
        if not pathname.startswith(prefix) or len(pathname) <= len(prefix):
            raise PreventUpdate
        workflow_id = pathname[len(prefix) :].rstrip("/")
        if not workflow_id:
            raise PreventUpdate
        return workflow_id

    # Workflow selection → URL: push /workflow/<id> to the address bar.
    @app.callback(
        Output("wf-url", "pathname", allow_duplicate=True),
        Input("wf-selected-workflow-id", "data"),
        State("wf-url", "pathname"),
        prevent_initial_call=True,
    )
    def push_workflow_to_url(workflow_id: str | None, current_path: str | None) -> str:
        if not workflow_id or not workflow_id.strip():
            raise PreventUpdate
        new_path = f"/workflow/{workflow_id.strip()}"
        # Avoid pushing if URL already matches (breaks the circular update)
        if current_path == new_path:
            raise PreventUpdate
        return new_path

    # Status filter → URL query param.
    @app.callback(
        Output("wf-url", "search", allow_duplicate=True),
        Input("wf-status-filter", "value"),
        State("wf-url", "search"),
        prevent_initial_call=True,
    )
    def push_status_to_url(status: str | None, current_search: str | None) -> str:
        params: dict[str, str] = {}
        if status:
            params["status"] = status
        new_search = f"?{urlencode(params)}" if params else ""
        if (current_search or "") == new_search:
            raise PreventUpdate
        return new_search

    # URL query param → status filter (fires on page load).
    @app.callback(
        Output("wf-status-filter", "value"),
        Input("wf-url", "search"),
    )
    def load_status_from_url(search: str | None) -> str:
        if not search:
            return ""
        params = parse_qs(search.lstrip("?"))
        return params.get("status", [""])[0]


def run_with_default_queue(queue_name=None, debug=False, port=8050, open_browser=True):
    debug = debug or False
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        external_scripts=[dbc._js_dist[0]["relative_package_path"]],
        suppress_callback_exceptions=True,
    )
    app.title = "JobQ Track"

    workflow_available = _has_workflow_store()
    queue_available = _has_log_analytics()

    if workflow_available and queue_available:
        # Both tabs
        tabs = dcc.Tabs(
            id="main-tabs",
            value="queue-tab",
            children=[
                dcc.Tab(label="Queue Monitor", value="queue-tab"),
                dcc.Tab(label="Workflow Dashboard", value="workflow-tab"),
            ],
            className="mb-3",
        )

        app.layout = html.Div(
            [
                dcc.Location(id="wf-url", refresh=False),
                dcc.Store(id="wf-selected-workflow-id"),
                html.H1("JobQ Track"),
                connection_info.layout(default_queue=queue_name),
                tabs,
                html.Div(id="main-tab-content"),
            ]
        )

        @app.callback(
            Output("main-tab-content", "children"),
            Input("main-tabs", "value"),
        )
        def render_tab(tab: str) -> html.Div:
            if tab == "workflow-tab":
                return _workflow_tab()
            return html.Div(active_workers.layout(default_queue=queue_name))

        _register_url_routing(app, dual_tab=True)

    elif workflow_available:
        # Workflow only (no Log Analytics)
        app.layout = html.Div(
            [
                dcc.Location(id="wf-url", refresh=False),
                dcc.Store(id="wf-selected-workflow-id"),
                html.H1("JobQ Track — Workflows"),
                connection_info.layout(default_queue=queue_name),
                _workflow_tab(),
            ]
        )

        _register_url_routing(app, dual_tab=False)

    else:
        # Queue monitor only (original behaviour)
        app.layout = html.Div(
            [
                html.H1("JobQ Track"),
                connection_info.layout(default_queue=queue_name),
                active_workers.layout(default_queue=queue_name),
            ]
        )

    if workflow_available:
        workflow_overview.register_callbacks(app)
        workflow_detail.register_callbacks(app)
        workflow_graph.register_callbacks(app)
        workflow_tasks.register_callbacks(app)
        workflow_health.register_callbacks(app)

    if queue_available:
        active_workers.register_callbacks(app)
        active_environments.register_callbacks(app)
        queue_size.register_callbacks(app)
        tasks_starting.register_callbacks(app)
        tasks_completed.register_callbacks(app)
        tasks_completed_trend.register_callbacks(app)
        task_runtimes.register_callbacks(app)
        cpu_utilization.register_callbacks(app)
        ram_utilization.register_callbacks(app)
        errors.register_callbacks(app)
        preemption_events.register_callbacks(app)
        stat_cards.register_callbacks(app)
        worker_churn.register_callbacks(app)
        worker_lifetime.register_callbacks(app)
        preemptions_by_env.register_callbacks(app)
        env_efficiency.register_callbacks(app)

    url = f"http://127.0.0.1:{port}/"
    LOG.info("Starting JobQ Track on %s", url)
    print(f"\n  JobQ Track → {url}\n")

    if open_browser:

        def _open() -> None:
            webbrowser.open_new_tab(url)

        Timer(2, _open).start()

    app.run(debug=debug, dev_tools_ui=debug, port=port)


if __name__ == "__main__":
    run_with_default_queue()
