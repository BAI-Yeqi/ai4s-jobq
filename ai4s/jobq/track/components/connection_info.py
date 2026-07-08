# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Display the storage backend the dashboard is currently observing.

A small banner shown beneath the page header so the user can tell at a
glance which storage account / table prefix / completion queue is
being inspected. Crucial when multiple chaos runs reuse the same
process and stale logs / dashboards otherwise look identical.
"""

from __future__ import annotations

import os

import dash_bootstrap_components as dbc
from dash import html


def _resolve_workflow_target() -> tuple[str, str] | None:
    """Return ``(account, prefix)`` for the configured workflow store, or None."""
    wf_env = os.environ.get("JOBQ_WORKFLOW_PREFIX", "").strip()
    if not wf_env:
        return None
    try:
        from ai4s.jobq.workflow.env import parse_workflow_value

        return parse_workflow_value(wf_env)
    except Exception:
        return None


def _resolve_workflow_file() -> str | None:
    path = os.environ.get("JOBQ_WORKFLOW_FILE", "").strip()
    return path or None


def _resolve_queue_target(default_queue: str | None) -> tuple[str | None, str | None]:
    """Return ``(storage_account, queue_name)`` for the configured queue backend."""
    storage = os.environ.get("JOBQ_STORAGE", "").strip() or None
    queue = os.environ.get("JOBQ_QUEUE", "").strip() or (
        default_queue.strip() if default_queue else None
    )
    return storage, queue


def _badge(label: str, value: str, *, color: str = "secondary") -> dbc.Badge:
    return dbc.Badge(
        [
            html.Span(label, className="me-1 fw-normal opacity-75"),
            html.Span(value, className="fw-semibold"),
        ],
        color=color,
        className="me-2 mb-1 py-2 px-2",
    )


def layout(*, default_queue: str | None = None) -> html.Div:
    """Build the connection-info banner. Safe to call when nothing is configured."""
    badges: list[dbc.Badge] = []

    wf = _resolve_workflow_target()
    if wf is not None:
        from ai4s.jobq.workflow.ids import completion_queue_name, index_table_name

        account, prefix = wf
        wf_table = index_table_name(prefix)
        badges.extend(
            [
                _badge("Storage account", account, color="primary"),
                _badge("Workflow prefix", prefix, color="info"),
                _badge("Workflow index table", wf_table, color="dark"),
                _badge(
                    "Completion queue",
                    completion_queue_name(prefix),
                    color="dark",
                ),
            ]
        )

    wf_file = _resolve_workflow_file()
    if wf_file is not None:
        badges.append(_badge("Workflow file", wf_file, color="info"))

    storage, queue = _resolve_queue_target(default_queue)
    if storage:
        badges.append(_badge("Queue storage", storage, color="primary"))
    if queue:
        badges.append(_badge("Queue", queue, color="dark"))

    la_workspace = os.environ.get("JOBQ_LA_WORKSPACE_ID", "").strip()
    if la_workspace:
        # The full GUID is noisy; show the leading 8 chars so the user
        # can still tell two workspaces apart without dominating the
        # banner.
        short = la_workspace[:8] + "…" if len(la_workspace) > 9 else la_workspace
        badges.append(_badge("LA workspace", short, color="secondary"))

    if not badges:
        return html.Div()

    return html.Div(
        badges,
        className="mb-3 small",
        title="Storage backend this dashboard is connected to",
    )
