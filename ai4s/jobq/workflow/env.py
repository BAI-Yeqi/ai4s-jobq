# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Environment-variable helpers for workflow CLI and runtime.

Workflows are configured by a tiny set of env vars, all under the
``JOBQ_WORKFLOW_PREFIX*`` family for unambiguous scope:

- ``JOBQ_WORKFLOW_PREFIX=<account>/<prefix>`` — required. The storage
  account
  hosting state (tables) and, by default, queues and large-output blobs.
  ``<prefix>`` namespaces tables (``<prefix>Workflows``,
  ``<prefix>WorkflowTasks``) so multiple projects can share one account.
- ``JOBQ_WORKFLOW_QUEUES`` — optional override for the queue backend
  (set this to ``sb://<namespace>`` to use Service Bus).
- ``JOBQ_WORKFLOW_BLOBS=<account>/<container>`` — optional override for
  large-output blob storage. Defaults to the
  ``JOBQ_WORKFLOW_PREFIX`` account with container ``jobq-workflow-data``.

This module is the single entry point for parsing these vars; CLI,
coordinator, worker, doctor, and client all go through it so error
messages and parsing rules stay consistent.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

WORKFLOW_ENV = "JOBQ_WORKFLOW_PREFIX"
QUEUES_ENV = "JOBQ_WORKFLOW_QUEUES"
BLOBS_ENV = "JOBQ_WORKFLOW_BLOBS"

DEFAULT_BLOB_CONTAINER = "jobq-workflow-data"

# Legacy variables, hard-broken in the redesign. We detect these and
# emit a migration error pointing at the new names.
LEGACY_ENV_VARS = ("JOBQ_WORKFLOW_STATE", "JOBQ_WORKFLOW")  # old name


class WorkflowEnvError(ValueError):
    """Raised when ``JOBQ_WORKFLOW_PREFIX*`` env vars are missing or malformed."""


@dataclass(frozen=True)
class WorkflowEnv:
    """Parsed workflow configuration from env vars and/or CLI flags.

    All four fields are populated even when the user only set
    ``JOBQ_WORKFLOW_PREFIX`` — queues and blobs default to the same account.
    """

    state_account: str
    """Storage account hosting the workflow tables (and, by default,
    queues and blobs). May be ``"devstoreaccount1"``, a bare account
    name (uses DefaultAzureCredential), an ``https://`` URL, or a
    full connection string."""

    prefix: str
    """Table-name prefix; tables are ``<prefix>Workflows`` and
    ``<prefix>WorkflowTasks``."""

    queues: str
    """Queue backend. Defaults to ``state_account``; may be overridden
    to a different account or to ``sb://<namespace>`` for Service Bus."""

    blob_account: str
    """Blob-storage account. Defaults to ``state_account``."""

    blob_container: str = DEFAULT_BLOB_CONTAINER
    """Container holding large task outputs."""

    @classmethod
    def from_environ(
        cls,
        *,
        state_account: str | None = None,
        prefix: str | None = None,
    ) -> WorkflowEnv:
        """Parse ``JOBQ_WORKFLOW_PREFIX``/``JOBQ_WORKFLOW_QUEUES``/``JOBQ_WORKFLOW_BLOBS``.

        ``state_account`` and ``prefix`` override the parsed
        ``JOBQ_WORKFLOW_PREFIX`` values (CLI positional / flags take precedence).
        """
        _check_legacy_env()

        # If both overrides are supplied, JOBQ_WORKFLOW_PREFIX is unnecessary.
        if state_account is None or prefix is None:
            wf = os.environ.get(WORKFLOW_ENV, "").strip()
            if not wf:
                missing = []
                if state_account is None:
                    missing.append("storage account")
                if prefix is None:
                    missing.append("prefix")
                raise WorkflowEnvError(
                    f"Workflow {' and '.join(missing)} not configured. "
                    f"Set {WORKFLOW_ENV}=<account>/<prefix> "
                    "(for example, JOBQ_WORKFLOW_PREFIX=mystorageaccount/MyProject), "
                    "or pass STORAGE/PREFIX positionally to `ai4s-jobq workflow`."
                )
            parsed_account, parsed_prefix = parse_workflow_value(wf)
            state_account = state_account or parsed_account
            prefix = prefix or parsed_prefix

        queues = os.environ.get(QUEUES_ENV, "").strip() or state_account

        blobs_raw = os.environ.get(BLOBS_ENV, "").strip()
        if blobs_raw:
            blob_account, blob_container = parse_blobs_value(blobs_raw)
        else:
            blob_account, blob_container = state_account, DEFAULT_BLOB_CONTAINER

        return cls(
            state_account=state_account,
            prefix=prefix,
            queues=queues,
            blob_account=blob_account,
            blob_container=blob_container,
        )


def parse_workflow_value(value: str) -> tuple[str, str]:
    """Split ``<account>/<prefix>`` into its two parts.

    URLs and connection strings (anything containing ``://``, ``;``, or
    ``=``) are rejected because rsplit on ``/`` would mangle them. Use
    the explicit CLI flags or set ``JOBQ_WORKFLOW_PREFIX`` to a bare
    account name in that case.
    """
    if "://" in value or ";" in value or "=" in value:
        raise WorkflowEnvError(
            f"{WORKFLOW_ENV}={value!r} cannot be a URL or connection string. "
            f"Use a bare account name and prefix (for example, "
            f"{WORKFLOW_ENV}=mystorageaccount/MyProject), or pass "
            "--storage/--prefix flags."
        )
    if "/" not in value:
        raise WorkflowEnvError(
            f"Missing prefix in {value!r}. Expected <account>/<prefix> "
            "(for example, 'mystorageaccount/MyProject' or "
            "'devstoreaccount1/dev')."
        )
    account, prefix = value.rsplit("/", 1)
    if not account or not prefix:
        raise WorkflowEnvError(
            f"Invalid <account>/<prefix>: {value!r}. Both segments must be non-empty."
        )
    return account, prefix


def parse_blobs_value(value: str) -> tuple[str, str]:
    """Split ``<account>/<container>`` for ``JOBQ_WORKFLOW_BLOBS``.

    Same rules as :func:`parse_workflow_value`: URLs and connection
    strings are rejected.
    """
    if "://" in value or ";" in value or "=" in value:
        raise WorkflowEnvError(
            f"{BLOBS_ENV}={value!r} cannot be a URL or connection string. "
            f"Use <account>/<container> (for example, "
            f"{BLOBS_ENV}=mystorageaccount/workflow-data)."
        )
    if "/" not in value:
        raise WorkflowEnvError(
            f"Missing container in {value!r}. Expected <account>/<container> "
            f"(for example, '{BLOBS_ENV}=mystorageaccount/workflow-data')."
        )
    account, container = value.rsplit("/", 1)
    if not account or not container:
        raise WorkflowEnvError(
            f"Invalid <account>/<container>: {value!r}. Both segments must be non-empty."
        )
    return account, container


def _check_legacy_env() -> None:
    """Hard-break: raise if any deprecated workflow env var is set."""
    found = [v for v in LEGACY_ENV_VARS if os.environ.get(v)]
    if not found:
        return
    raise WorkflowEnvError(
        f"{', '.join(found)} are no longer used by the workflow CLI. "
        f"Set {WORKFLOW_ENV}=<account>/<prefix> instead "
        "(for example, JOBQ_WORKFLOW_PREFIX=mystorageaccount/MyProject). "
        f"For Service Bus queues, also set {QUEUES_ENV}=sb://<namespace>; "
        f"for a custom blob container, set {BLOBS_ENV}=<account>/<container>."
    )
