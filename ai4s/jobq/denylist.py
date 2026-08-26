# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Centralized image-SHA denylist backed by an Azure Table.

Operators add container-image digests (``sha256:…`` — either the
multi-arch manifest-list digest or an arch-specific child digest) to a
shared table. The orchestration layer refuses to launch, and cancels,
workers whose image is denied; individual workers poll the same table and
shut themselves down when they discover their own image is denied.

The store is **org-wide by default** (an Azure Table in the shared
``jobq0central`` storage account, overridable via
:envvar:`JOBQ_DENYLIST_ACCOUNT`) but every consumer must
treat it as **fail-open**: when the denylist is unreachable, nothing is
denied and healthy work continues.
The Singularity fail-closed hardening (see the plan) is layered on top of
this module by its callers, not baked into it.

Configuration (environment):

``JOBQ_DENYLIST_ACCOUNT``
    Account descriptor for the table. Dispatches like
    :meth:`ai4s.jobq.workflow.persistence.WorkflowPersistence.from_account`:
    ``devstoreaccount1`` (Azurite), a verbatim connection string
    (contains ``AccountKey=`` / ``SharedAccessSignature=``), or a bare
    AAD account name expanded to ``https://<account>.table.core.windows.net``.
    Defaults to the shared org-wide account ``jobq0central``; override to
    point at a different store.

``JOBQ_DENYLIST_TABLE``
    Table name (default ``JobQImageDenylist``).

``JOBQ_DENYLIST_DISABLE``
    Truthy value hard-disables the feature everywhere.

``JOBQ_DENYLIST_POLL_INTERVAL_S``
    Worker self-check cadence in seconds (default ``60``); consumed by the
    worker-side handler, surfaced here for a single source of truth.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.data.tables.aio import TableClient, TableServiceClient

LOG = logging.getLogger("ai4s.jobq")

_PARTITION_KEY = "image"
_DEFAULT_TABLE_NAME = "JobQImageDenylist"
_DEFAULT_ACCOUNT = "jobq0central"
_DEFAULT_POLL_INTERVAL_S = 60

VALID_SHUTDOWN_MODES = ("graceful", "hard")
_DEFAULT_SHUTDOWN_MODE = "graceful"


class DenylistEntryExistsError(Exception):
    """Raised when adding a digest that is already denied without ``force``."""

    def __init__(self, digest: str) -> None:
        super().__init__(f"{digest} is already in the denylist; pass force=True to overwrite it")
        self.digest = digest


# Env-var and AML-job-tag keys carrying a worker's resolved image digests.
# Injected by the workforce at hire time; read by the worker self-check and
# by the workforce's denylist enforcement (via job tags).
IMAGE_DIGEST_ENV = "JOBQ_IMAGE_DIGEST"
IMAGE_DIGEST_ARCH_ENV = "JOBQ_IMAGE_DIGEST_ARCH"

_TRUISH = frozenset({"1", "true", "yes", "t", "y", "on"})
_FALSISH = frozenset({"0", "false", "no", "f", "n", "off"})
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


# ── digest normalization ──────────────────────────────────────────────────────


def normalize_digest(value: str) -> str:
    """Normalize an image SHA to canonical ``sha256:<64-hex>`` form.

    Accepts a bare 64-char hex string, a ``sha256:<hex>`` digest, or a full
    image reference containing ``@sha256:<hex>``. Case- and
    whitespace-insensitive. Raises :class:`ValueError` on anything else.
    """
    if value is None:
        raise ValueError("digest must not be None")
    text = value.strip().lower()
    if not text:
        raise ValueError("digest must not be empty")
    if "@sha256:" in text:
        text = text.split("@sha256:", 1)[1]
    elif text.startswith("sha256:"):
        text = text[len("sha256:") :]
    if not _SHA256_HEX_RE.match(text):
        raise ValueError(
            f"not a valid sha256 image digest: {value!r} "
            "(expected 'sha256:<64 hex>', a bare 64-hex string, or 'ref@sha256:<hex>')"
        )
    return f"sha256:{text}"


def _row_key(digest: str) -> str:
    """Table RowKey for a canonical digest (the bare hex, always key-safe)."""
    return normalize_digest(digest).split(":", 1)[1]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        LOG.warning("denylist_bad_timestamp value=%r (ignoring)", value)
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_table_not_found(exc: Exception) -> bool:
    """Whether an Azure Table not-found error refers to the table itself."""
    return getattr(exc, "error_code", None) == "TableNotFound" or "TableNotFound" in str(exc)


# ── entry model ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DenylistEntry:
    """One denied image digest and its policy metadata."""

    digest: str
    reason: str = ""
    added_by: str = ""
    added_at: datetime | None = None
    shutdown_mode: str = _DEFAULT_SHUTDOWN_MODE
    effective_at: datetime | None = None

    def is_effective(self, now: datetime | None = None) -> bool:
        """True when this entry is active (its effective date has arrived).

        A ``None`` effective date means "effective immediately". Entries whose
        effective date is still in the future are stored and listed but must
        not be enforced (workers keep running, hires are not refused).
        """
        if self.effective_at is None:
            return True
        return self.effective_at <= (now or _now_utc())

    @classmethod
    def _from_entity(cls, entity: dict) -> DenylistEntry:
        digest = str(entity.get("digest") or f"sha256:{entity['RowKey']}")
        mode = str(entity.get("shutdown_mode") or _DEFAULT_SHUTDOWN_MODE).lower()
        if mode not in VALID_SHUTDOWN_MODES:
            mode = _DEFAULT_SHUTDOWN_MODE
        return cls(
            digest=digest,
            reason=str(entity.get("reason") or ""),
            added_by=str(entity.get("added_by") or ""),
            added_at=_parse_iso(entity.get("added_at")),
            shutdown_mode=mode,
            effective_at=_parse_iso(entity.get("effective_at")),
        )

    def _to_entity(self) -> dict:
        entity: dict[str, str] = {
            "PartitionKey": _PARTITION_KEY,
            "RowKey": _row_key(self.digest),
            "digest": normalize_digest(self.digest),
            "reason": self.reason,
            "added_by": self.added_by,
            "shutdown_mode": self.shutdown_mode,
        }
        entity["added_at"] = (self.added_at or _now_utc()).isoformat()
        entity["effective_at"] = (self.effective_at or self.added_at or _now_utc()).isoformat()
        return entity


# ── configuration helpers ─────────────────────────────────────────────────────


def denylist_disabled() -> bool:
    """True when :envvar:`JOBQ_DENYLIST_DISABLE` is set to a truthy value."""
    return os.environ.get("JOBQ_DENYLIST_DISABLE", "0").strip().lower() in _TRUISH


def denylist_account() -> str | None:
    """The configured account descriptor, or ``None`` when unconfigured.

    Defaults to the shared org-wide storage account (:data:`_DEFAULT_ACCOUNT`)
    so the denylist is active out of the box; override with
    :envvar:`JOBQ_DENYLIST_ACCOUNT`. An explicit empty value is treated as
    unset and falls back to the default.
    """
    account = os.environ.get("JOBQ_DENYLIST_ACCOUNT", "").strip()
    return account or _DEFAULT_ACCOUNT


def denylist_table_name() -> str:
    return os.environ.get("JOBQ_DENYLIST_TABLE", "").strip() or _DEFAULT_TABLE_NAME


def account_uses_aad(account: str) -> bool:
    """True when the account descriptor authenticates via an AAD token.

    Azurite (``devstoreaccount1``) and verbatim connection strings
    (``AccountKey=`` / ``SharedAccessSignature=``) authenticate without a
    user token, so no caller identity can be derived from them.
    """
    return (
        account != "devstoreaccount1"
        and "AccountKey=" not in account
        and "SharedAccessSignature=" not in account
    )


def denylist_configured() -> bool:
    """True when a denylist account is configured and not hard-disabled.

    Used by fail-closed callers (for example the Singularity startup guard)
    to decide whether a store *should* be reachable.
    """
    return not denylist_disabled() and denylist_account() is not None


def denylist_poll_interval_s() -> float:
    raw = os.environ.get("JOBQ_DENYLIST_POLL_INTERVAL_S", "").strip()
    if not raw:
        return float(_DEFAULT_POLL_INTERVAL_S)
    try:
        value = float(raw)
    except ValueError:
        LOG.warning("denylist_bad_poll_interval value=%r; using default", raw)
        return float(_DEFAULT_POLL_INTERVAL_S)
    return max(1.0, value)


def running_on_singularity() -> bool:
    """Best-effort detection of a Singularity/AML managed compute context.

    Singularity jobs run under AzureML, which exports a number of
    ``AZUREML_*`` markers. We treat the presence of the Singularity-specific
    marker (or, failing that, the generic AzureML run markers) as "managed
    compute where an unremovable worker is unacceptable".
    """
    if os.environ.get("JOBQ_FORCE_SINGULARITY", "").strip().lower() in _TRUISH:
        return True
    if any(k.startswith("AZUREML_SINGULARITY") for k in os.environ):
        return True
    singularity_markers = ("SINGULARITY_JOB_ID", "AZ_LS_CERT_THUMBPRINT")
    return any(marker in os.environ for marker in singularity_markers)


def denylist_require_configured() -> bool:
    """True when the worker must refuse to start without a working denylist.

    Fail-closed policy for managed compute (Singularity): enabled when
    :envvar:`JOBQ_DENYLIST_REQUIRE` is truthy, or automatically when running
    on Singularity and not explicitly disabled. Never required when the
    feature is hard-disabled via :envvar:`JOBQ_DENYLIST_DISABLE`.
    """
    if denylist_disabled():
        return False
    forced = os.environ.get("JOBQ_DENYLIST_REQUIRE", "").strip().lower()
    if forced in _TRUISH:
        return True
    if forced in _FALSISH:
        return False
    return running_on_singularity()


def denylist_shutdown_on_unreachable() -> bool:
    """True when a running worker should shut down if the denylist is
    persistently unreachable (fail-closed). Follows the same policy as
    :func:`denylist_require_configured`."""
    return denylist_require_configured()


# ── the store ─────────────────────────────────────────────────────────────────


class ImageDenylist:
    """Async CRUD/query wrapper over the denylist Azure Table.

    Prefer the :meth:`open` / :meth:`from_account` factories. Use as an
    async context manager so the underlying table-service client is closed.
    """

    def __init__(
        self,
        table_service: TableServiceClient,
        *,
        table_name: str = _DEFAULT_TABLE_NAME,
        own_table_service: bool = False,
    ) -> None:
        self._table_service = table_service
        self._table_name = table_name
        self._own_table_service = own_table_service
        self._table: TableClient = table_service.get_table_client(table_name)

    # -- construction ----------------------------------------------------------

    @classmethod
    async def open(cls) -> ImageDenylist | None:
        """Open the denylist from environment config.

        Returns ``None`` (inert) when the feature is disabled or no account
        is configured — callers must treat ``None`` as "nothing denied".
        """
        if denylist_disabled():
            LOG.debug("denylist disabled via JOBQ_DENYLIST_DISABLE")
            return None
        account = denylist_account()
        if account is None:
            LOG.debug("denylist inert: JOBQ_DENYLIST_ACCOUNT is not set")
            return None
        return await cls.from_account(account, table_name=denylist_table_name())

    @classmethod
    async def from_account(
        cls, account: str, *, table_name: str = _DEFAULT_TABLE_NAME
    ) -> ImageDenylist:
        """Build a store from an account descriptor (see module docstring)."""
        from azure.data.tables.aio import TableServiceClient

        if account == "devstoreaccount1":
            from ai4s.jobq.backend.storage_queue import azurite_conn_str

            conn_str = azurite_conn_str(service="table")
            table_service = TableServiceClient.from_connection_string(conn_str)
            own = True
        elif not account_uses_aad(account):
            table_service = TableServiceClient.from_connection_string(account)
            own = True
        else:
            from ai4s.jobq.auth import get_token_credential

            table_service = TableServiceClient(
                endpoint=f"https://{account}.table.core.windows.net",
                credential=get_token_credential(),
            )
            own = True

        store = cls(table_service, table_name=table_name, own_table_service=own)
        await store._ensure_table()
        return store

    async def _ensure_table(self) -> None:
        from azure.core.exceptions import HttpResponseError, ResourceExistsError

        try:
            await self._table_service.create_table_if_not_exists(self._table_name)
        except ResourceExistsError:
            pass
        except HttpResponseError as exc:  # pragma: no cover - transient
            LOG.warning("denylist_ensure_table_failed table=%s error=%s", self._table_name, exc)

    async def __aenter__(self) -> ImageDenylist:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._table.close()
        if self._own_table_service:
            await self._table_service.close()

    # -- mutations -------------------------------------------------------------

    async def add(
        self,
        digest: str,
        *,
        reason: str = "",
        added_by: str = "",
        shutdown_mode: str = _DEFAULT_SHUTDOWN_MODE,
        effective_at: datetime | None = None,
        force: bool = False,
    ) -> DenylistEntry:
        """Add a denied digest. Returns the stored entry.

        ``effective_at`` schedules when the deny takes effect; when ``None``
        (the default) the entry is effective immediately. Entries whose
        effective date is still in the future are stored and listed but not
        enforced until that time arrives.

        By default this refuses to clobber an existing entry: if the digest is
        already denied, :class:`DenylistEntryExistsError` is raised. Pass
        ``force=True`` to overwrite the existing entry.
        """
        from azure.core.exceptions import ResourceExistsError

        mode = (shutdown_mode or _DEFAULT_SHUTDOWN_MODE).lower()
        if mode not in VALID_SHUTDOWN_MODES:
            raise ValueError(
                f"invalid shutdown_mode {shutdown_mode!r}; expected one of {VALID_SHUTDOWN_MODES}"
            )
        entry = DenylistEntry(
            digest=normalize_digest(digest),
            reason=reason,
            added_by=added_by,
            added_at=_now_utc(),
            shutdown_mode=mode,
            effective_at=effective_at,
        )
        if force:
            await self._table.upsert_entity(entry._to_entity())
        else:
            try:
                await self._table.create_entity(entry._to_entity())
            except ResourceExistsError:
                raise DenylistEntryExistsError(entry.digest) from None
        LOG.info(
            "denylist_add digest=%s mode=%s effective=%s force=%s reason=%s",
            entry.digest,
            mode,
            (effective_at.isoformat() if effective_at else "now"),
            force,
            reason,
        )
        return entry

    async def remove(self, digest: str) -> bool:
        """Remove a denied digest. Returns True if it existed."""
        from azure.core.exceptions import ResourceNotFoundError

        canonical = normalize_digest(digest)
        row_key = _row_key(canonical)
        # Some table backends (notably Azurite) treat delete as idempotent
        # and do not 404 on a missing row, so check existence explicitly for
        # a reliable return value.
        try:
            await self._table.get_entity(_PARTITION_KEY, row_key)
        except ResourceNotFoundError:
            return False
        try:
            await self._table.delete_entity(_PARTITION_KEY, row_key)
        except ResourceNotFoundError:
            return False
        LOG.info("denylist_remove digest=%s", canonical)
        return True

    # -- queries ---------------------------------------------------------------

    async def get(self, digest: str) -> DenylistEntry | None:
        """Return the entry for ``digest``, or ``None``."""
        from azure.core.exceptions import ResourceNotFoundError

        try:
            entity = await self._table.get_entity(_PARTITION_KEY, _row_key(digest))
        except ResourceNotFoundError as exc:
            if _is_table_not_found(exc):
                raise
            return None
        return DenylistEntry._from_entity(dict(entity))

    async def list_entries(self) -> list[DenylistEntry]:
        """List all entries, newest-first."""
        query = f"PartitionKey eq '{_PARTITION_KEY}'"
        entries: list[DenylistEntry] = [
            DenylistEntry._from_entity(dict(entity))
            async for entity in self._table.query_entities(query)
        ]
        entries.sort(
            key=lambda e: e.added_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True
        )
        return entries

    async def is_denied(self, *digests: str | None) -> DenylistEntry | None:
        """Return the matching *effective* entry for any of ``digests``, or ``None``.

        Digests are normalized; invalid/empty ones are skipped. Looks up
        each candidate's row directly (cheap point reads), so this is safe
        to call on a hot self-check path. Entries whose effective date has
        not yet arrived are treated as not-yet-denied and skipped.
        """
        seen: set[str] = set()
        for raw in digests:
            if not raw:
                continue
            try:
                canonical = normalize_digest(raw)
            except ValueError:
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            entry = await self.get(canonical)
            if entry is not None and entry.is_effective():
                return entry
        return None

    async def denied_digests(self) -> set[str]:
        """All currently-*effective* denied canonical digests.

        Excludes entries scheduled to take effect in the future.
        """
        return {e.digest for e in await self.list_entries() if e.is_effective()}
