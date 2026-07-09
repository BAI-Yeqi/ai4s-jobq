# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Deterministic identifier helpers for the workflow layer.

Centralises the naming conventions used across the persistence,
coordinator, worker, and CLI tiers so callers never hand-craft these
strings.  Keep this module tiny and dependency-free.
"""

from __future__ import annotations


def state_blob_name(workflow_id: str) -> str:
    """Blob name that holds the full ``WorkflowRuntime`` JSON for *workflow_id*.

    This is the *legacy* single-blob format.  New workflows use
    :func:`definition_blob_name` + :func:`mutable_state_blob_name`.
    """
    return f"{workflow_id}.json"


def definition_blob_name(workflow_id: str) -> str:
    """Blob name for the immutable workflow definition (topology, kwargs).

    Written once at submit; never overwritten.
    """
    return f"{workflow_id}.def.bin"


def mutable_state_blob_name(workflow_id: str) -> str:
    """Blob name for the mutable workflow state (task states, counters).

    Rewritten by the coordinator on every flush (~6x smaller than the
    legacy full blob).
    """
    return f"{workflow_id}.state.bin"


def output_blob_name(workflow_id: str, task_name: str) -> str:
    """Blob name for a single task's stashed output payload."""
    # Slashes in task names are legal; the blob backend handles nested "paths".
    return f"{workflow_id}/{task_name}.json"


def output_ref_for_blob(workflow_id: str, task_name: str) -> str:
    """Construct the ``blob:<path>`` reference returned to the coordinator."""
    return f"blob:{output_blob_name(workflow_id, task_name)}"


def is_blob_ref(output_ref: str | None) -> bool:
    """Return ``True`` iff *output_ref* points at a blob (not inline JSON)."""
    return bool(output_ref) and output_ref.startswith("blob:")  # type: ignore[union-attr]


def state_container_name(prefix: str) -> str:
    """Blob container that holds workflow state blobs."""
    return f"{prefix.lower()}-workflows"


def output_container_name(prefix: str) -> str:
    """Blob container that holds stashed task output blobs."""
    return f"{prefix.lower()}-outputs"


def index_table_name(prefix: str) -> str:
    """Azure Table that holds the per-workflow index rows."""
    return f"{prefix}WorkflowsIndex"


COMPLETION_QUEUE_SUFFIX = "-workflow-completions"


def completion_queue_name(prefix: str) -> str:
    """Per-prefix completion queue name for worker→coordinator messages.

    Azure Storage Queue names must be lowercase ASCII; lowercase the
    prefix before joining.  Stable across releases — used by every
    workflow component to find a single shared queue.
    """
    return f"{prefix.lower()}{COMPLETION_QUEUE_SUFFIX}"


def task_message_id(workflow_id: str, task_name: str, attempt: int) -> str:
    """Deterministic id stamped on the JobQ task message for dedup.

    The coordinator uses this to make task pushes idempotent across
    coordinator restarts: pushing the same ``(workflow_id, task_name,
    attempt)`` twice is a no-op for backends that honour ``num_retries``-
    style dedup.  For Storage Queue (no dedup window) the worker side
    must remain idempotent.
    """
    return f"{workflow_id}/{task_name}/{attempt}"
