# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Configuration helpers for the workflow CLI and runtime.

Workflows are configured from three layers, highest precedence first:

1. **CLI flags / positional** — ``STORAGE/PREFIX``, ``--queues``,
   ``--blobs`` (and coordinator tuning flags).
2. **Environment variables** — the ``JOBQ_WORKFLOW_PREFIX*`` family:

   - ``JOBQ_WORKFLOW_PREFIX=<account>/<prefix>`` — the storage account
     hosting state (tables) and, by default, queues and large-output
     blobs. ``<prefix>`` namespaces tables (``<prefix>Workflows``,
     ``<prefix>WorkflowTasks``) so multiple projects can share one account.
   - ``JOBQ_WORKFLOW_QUEUES`` — override for the queue backend (set to
     ``sb://<namespace>`` to use Service Bus).
   - ``JOBQ_WORKFLOW_BLOBS=<account>/<container>`` — override for
     large-output blob storage. Defaults to the ``JOBQ_WORKFLOW_PREFIX``
     account with container ``jobq-workflow-data``.

3. **Config file** — a shared ``jobq.yaml`` so the coordinator, workers,
   and clients across multiple terminals share one source of truth
   instead of each exporting the same env vars. Discovered via
   ``--config PATH`` → ``JOBQ_WORKFLOW_CONFIG`` → ``./jobq.yaml`` →
   ``./.jobq.yaml`` → ``~/.config/ai4s-jobq/jobq.yaml``. Schema::

       connection:
         storage: mystorageacct
         prefix:  MyProject
         queues:  sb://my-namespace      # optional
         blobs:   mystorageacct/wf-data  # optional
       coordinator:                      # optional tuning defaults
         batch_size: 64
         running_timeout_s: 3600

This module is the single entry point for resolving configuration; CLI,
coordinator, worker, doctor, and client all go through it so error
messages, precedence, and parsing rules stay consistent.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

WORKFLOW_ENV = "JOBQ_WORKFLOW_PREFIX"
QUEUES_ENV = "JOBQ_WORKFLOW_QUEUES"
BLOBS_ENV = "JOBQ_WORKFLOW_BLOBS"
CONFIG_ENV = "JOBQ_WORKFLOW_CONFIG"

DEFAULT_BLOB_CONTAINER = "jobq-workflow-data"

# Config-file discovery: relative names tried in the current directory,
# then a per-user fallback under XDG-style config.
CONFIG_FILENAMES = ("jobq.yaml", ".jobq.yaml")
USER_CONFIG_PATH = "~/.config/ai4s-jobq/jobq.yaml"

# Recognised keys in the config-file ``connection`` section.
_CONNECTION_KEYS = ("storage", "prefix", "queues", "blobs")

# Recognised keys in the config-file ``coordinator`` section (tuning
# defaults consumed by `workflow coordinator`).
_COORDINATOR_KEYS = (
    "batch_size",
    "visibility_timeout_s",
    "idle_sleep_s",
    "cancel_poll_interval_s",
    "flush_retry_limit",
    "ready_sweep_interval_s",
    "ready_repair_threshold_s",
    "running_timeout_s",
    "running_sweep_interval_s",
)

# Legacy variables, hard-broken in the redesign. We detect these and
# emit a migration error pointing at the new names.
LEGACY_ENV_VARS = ("JOBQ_WORKFLOW_STATE", "JOBQ_WORKFLOW")  # old name


class WorkflowEnvError(ValueError):
    """Raised when workflow configuration is missing or malformed."""


@dataclass(frozen=True)
class WorkflowConfig:
    """Parsed ``jobq.yaml`` config file (or an empty one when absent)."""

    path: str | None = None
    """Absolute/relative path the config was loaded from, or ``None``."""

    connection: dict[str, str] = field(default_factory=dict)
    """The ``connection`` section (storage/prefix/queues/blobs)."""

    coordinator: dict[str, Any] = field(default_factory=dict)
    """The ``coordinator`` section (tuning defaults)."""


def discover_config_path(explicit: str | None = None) -> str | None:
    """Return the config-file path per the discovery order, or ``None``.

    ``--config`` (``explicit``) wins, then ``JOBQ_WORKFLOW_CONFIG``, then
    ``./jobq.yaml`` / ``./.jobq.yaml``, then ``~/.config/ai4s-jobq/jobq.yaml``.
    An ``explicit`` path is returned as-is even if missing so the loader
    can raise a clear error.
    """
    if explicit:
        return explicit
    env_val = os.environ.get(CONFIG_ENV, "").strip()
    if env_val:
        return env_val
    for name in CONFIG_FILENAMES:
        if os.path.isfile(name):
            return name
    user = os.path.expanduser(USER_CONFIG_PATH)
    if os.path.isfile(user):
        return user
    return None


def load_config(explicit: str | None = None) -> WorkflowConfig:
    """Discover and parse ``jobq.yaml``; return an empty config if none.

    A missing file is only an error when the path was given explicitly
    (via ``--config`` or ``JOBQ_WORKFLOW_CONFIG``); implicit discovery of
    a non-existent file is a silent no-op.
    """
    import yaml

    from_env = not explicit and bool(os.environ.get(CONFIG_ENV, "").strip())
    path = discover_config_path(explicit)
    if path is None:
        return WorkflowConfig()
    if not os.path.isfile(path):
        if explicit is not None or from_env:
            source = "--config" if explicit is not None else CONFIG_ENV
            raise WorkflowEnvError(f"Config file not found ({source}): {path}")
        return WorkflowConfig()

    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if raw is None:
        return WorkflowConfig(path=path)
    if not isinstance(raw, dict):
        raise WorkflowEnvError(f"Config file {path} must contain a YAML mapping at the top level.")

    connection = raw.get("connection") or {}
    coordinator = raw.get("coordinator") or {}
    if not isinstance(connection, dict):
        raise WorkflowEnvError(f"'connection' in {path} must be a mapping.")
    if not isinstance(coordinator, dict):
        raise WorkflowEnvError(f"'coordinator' in {path} must be a mapping.")

    unknown = set(connection) - set(_CONNECTION_KEYS)
    if unknown:
        raise WorkflowEnvError(
            f"Unknown key(s) in 'connection' of {path}: {', '.join(sorted(unknown))}. "
            f"Allowed: {', '.join(_CONNECTION_KEYS)}."
        )

    unknown_coord = set(coordinator) - set(_COORDINATOR_KEYS)
    if unknown_coord:
        raise WorkflowEnvError(
            f"Unknown key(s) in 'coordinator' of {path}: {', '.join(sorted(unknown_coord))}. "
            f"Allowed: {', '.join(_COORDINATOR_KEYS)}."
        )

    return WorkflowConfig(
        path=path,
        connection={k: str(v) for k, v in connection.items() if v is not None},
        coordinator=dict(coordinator),
    )


@dataclass(frozen=True)
class WorkflowEnv:
    """Resolved workflow configuration from flags, env vars, and config file.

    All connection fields are populated even when the user only set
    ``JOBQ_WORKFLOW_PREFIX`` — queues and blobs default to the same account.
    Values are resolved with precedence flag → env var → config file →
    built-in default.
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

    config_path: str | None = None
    """Path of the ``jobq.yaml`` that contributed values, if any."""

    coordinator: dict[str, Any] = field(default_factory=dict)
    """Raw ``coordinator`` tuning section from the config file (may be empty)."""

    sources: dict[str, str] = field(default_factory=dict)
    """Per-field provenance: maps ``storage``/``prefix``/``queues``/``blobs``
    to the layer that supplied the value (``flag``/``env``/``file``/``default``)."""

    @classmethod
    def from_environ(
        cls,
        *,
        state_account: str | None = None,
        prefix: str | None = None,
        queues: str | None = None,
        blobs: str | None = None,
        config: str | WorkflowConfig | None = None,
    ) -> WorkflowEnv:
        """Resolve workflow configuration from flags, env vars, and config file.

        Precedence for every value is flag → env var → config file →
        built-in default. Explicit arguments (``state_account``,
        ``prefix``, ``queues``, ``blobs``) correspond to CLI flags /
        positionals and win over everything else. ``config`` is either a
        path (``--config``) or an already-loaded :class:`WorkflowConfig`.
        """
        _check_legacy_env()

        cfg = config if isinstance(config, WorkflowConfig) else load_config(config)
        conn = cfg.connection
        sources: dict[str, str] = {}

        # storage account & prefix: flag → env → file.
        parsed_account, parsed_prefix = (None, None)
        if state_account is None or prefix is None:
            wf = os.environ.get(WORKFLOW_ENV, "").strip()
            if wf:
                parsed_account, parsed_prefix = parse_workflow_value(wf)

        state_account, sources["storage"] = _pick(
            state_account, parsed_account, conn.get("storage")
        )
        prefix, sources["prefix"] = _pick(prefix, parsed_prefix, conn.get("prefix"))

        if not state_account or not prefix:
            missing = []
            if not state_account:
                missing.append("storage account")
            if not prefix:
                missing.append("prefix")
            raise WorkflowEnvError(
                f"Workflow {' and '.join(missing)} not configured. "
                f"Set {WORKFLOW_ENV}=<account>/<prefix> "
                "(for example, JOBQ_WORKFLOW_PREFIX=mystorageaccount/MyProject), "
                "pass STORAGE/PREFIX positionally to `ai4s-jobq workflow`, "
                "or add a 'connection:' section to a jobq.yaml config file."
            )

        # queues: flag → env → file → default (state account).
        queues_val, sources["queues"] = _pick(
            (queues or "").strip() or None,
            os.environ.get(QUEUES_ENV, "").strip() or None,
            conn.get("queues", "").strip() or None,
            default=state_account,
        )
        assert queues_val is not None  # default=state_account is non-empty

        # blobs: flag → env → file → default (state account + default container).
        blobs_raw, sources["blobs"] = _pick(
            (blobs or "").strip() or None,
            os.environ.get(BLOBS_ENV, "").strip() or None,
            conn.get("blobs", "").strip() or None,
        )
        if blobs_raw:
            blob_account, blob_container = parse_blobs_value(blobs_raw)
        else:
            blob_account, blob_container = state_account, DEFAULT_BLOB_CONTAINER

        return cls(
            state_account=state_account,
            prefix=prefix,
            queues=queues_val,
            blob_account=blob_account,
            blob_container=blob_container,
            config_path=cfg.path,
            coordinator=dict(cfg.coordinator),
            sources=sources,
        )


def _pick(
    flag: str | None,
    env_val: str | None,
    file_val: str | None,
    *,
    default: str | None = None,
) -> tuple[str | None, str]:
    """Return ``(value, source)`` honouring flag → env → file → default.

    ``source`` is one of ``flag``/``env``/``file``/``default`` and lets
    callers surface where each resolved value originated.
    """
    if flag:
        return flag, "flag"
    if env_val:
        return env_val, "env"
    if file_val:
        return file_val, "file"
    return default, "default"


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
