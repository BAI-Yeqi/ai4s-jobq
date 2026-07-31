# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Azure-backed durable storage for workflow state.

Layout
------

* Blob container ``<prefix>-workflows`` — two blobs per workflow:

  - ``{workflow_id}.def.bin`` — immutable definition (topology, kwargs,
    queues).  Written once at submission, never overwritten.
  - ``{workflow_id}.state.bin`` — mutable state (task states, counters,
    timestamps).  Rewritten by the coordinator on every flush with
    ``If-Match`` ETag CAS.

  Legacy single-blob format (``{workflow_id}.json``, gzip-compressed
  full state) is still accepted transparently: on load, if the split
  blobs are not found, the legacy blob is read.  New submissions always
  use the split format.

* Blob container ``<prefix>-outputs`` — one
  ``{workflow_id}/{task_name}.json`` blob per task whose output exceeded
  the inline threshold.  Workers write here directly; the coordinator
  refers to these via ``blob:`` output refs.

* Azure Table ``<prefix>WorkflowsIndex`` — one row per workflow.  Cheap
  index used by ``list_workflows`` / ``summary`` / ``purge`` /
  ``request_cancel``.  The index is best-effort consistent with the
  state blob; the blob is authoritative.

Why split blobs
---------------

The mutable state blob is ~6x smaller than a full blob because it
omits the static definition (parents, children, kwargs, queues, dep
policies).  Since the coordinator flushes state after every batch
(tens of times per second), this reduces write bandwidth and speeds up
ETag-conflict retries.

The definition blob is read only once per coordinator startup or cache
miss.  It can be cached indefinitely because it never changes.

This module is intentionally compact.  It exposes only the persistence
operations the coordinator, client, and worker actually need.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from azure.core import MatchConditions
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)

from ai4s.jobq.workflow.entities import TaskState, WorkflowState, WorkflowStatus
from ai4s.jobq.workflow.ids import (
    definition_blob_name,
    index_table_name,
    is_blob_ref,
    mutable_state_blob_name,
    output_blob_name,
    output_container_name,
    output_ref_for_blob,
    state_blob_name,
    state_container_name,
)
from ai4s.jobq.workflow.state import WorkflowRuntime

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from azure.data.tables.aio import TableClient, TableServiceClient
    from azure.storage.blob.aio import BlobClient, BlobServiceClient, ContainerClient

    from ai4s.jobq.workflow.entities import TaskStatus, WorkflowDefinition

LOG = logging.getLogger(__name__)

_GZIP_MAGIC = b"\x1f\x8b"


def _encode_state(runtime: WorkflowRuntime) -> bytes:
    """Serialize *runtime* to gzip-compressed JSON bytes (legacy full blob).

    Using ``compresslevel=1`` (fastest) gives ~90% size reduction for large
    workflows (16 k tasks: 15.5 MB → 1.4 MB) with negligible CPU cost.
    Old blobs without the gzip magic header are still accepted by
    :func:`_decode_state` so the format change is backward-compatible.
    """
    raw = json.dumps(runtime.to_json()).encode("utf-8")
    return gzip.compress(raw, compresslevel=1)


def _decode_state(data: bytes) -> WorkflowRuntime:
    """Deserialize a state blob, transparently handling both compressed and plain formats."""
    if data[:2] == _GZIP_MAGIC:
        data = gzip.decompress(data)
    return WorkflowRuntime.from_json(json.loads(data))


def _encode_definition(runtime: WorkflowRuntime) -> bytes:
    """Serialize the immutable definition portion (topology, kwargs) to gzip JSON."""
    raw = json.dumps(runtime.to_definition_json()).encode("utf-8")
    return gzip.compress(raw, compresslevel=1)


def _encode_mutable_state(runtime: WorkflowRuntime) -> bytes:
    """Serialize only the mutable state (task states, counters) to gzip JSON."""
    raw = json.dumps(runtime.to_state_json()).encode("utf-8")
    return gzip.compress(raw, compresslevel=1)


def _decode_split(def_data: bytes, state_data: bytes) -> WorkflowRuntime:
    """Reconstruct a runtime from separate definition and state blobs."""
    if def_data[:2] == _GZIP_MAGIC:
        def_data = gzip.decompress(def_data)
    if state_data[:2] == _GZIP_MAGIC:
        state_data = gzip.decompress(state_data)
    return WorkflowRuntime.from_split_json(json.loads(def_data), json.loads(state_data))


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class WorkflowConflictError(RuntimeError):
    """Raised when a flush fails because someone else wrote the state blob first."""


class WorkflowNotFoundError(KeyError):
    """Raised when a workflow id is not present in either the blob or the index."""


# ---------------------------------------------------------------------------
# Index row dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class WorkflowIndexRow:
    """Cheap projection of a workflow's status for list/summary/purge."""

    workflow_id: str
    name: str
    workflow_state: WorkflowState
    cancel_requested: bool
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    running_tasks: int
    pending_tasks: int
    skipped_tasks: int
    default_queue: str
    queues_used: list[str]
    created_at: datetime
    updated_at: datetime
    error: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _index_row_from_runtime(runtime: WorkflowRuntime) -> WorkflowIndexRow:
    status = runtime.to_status()
    return WorkflowIndexRow(
        workflow_id=runtime.workflow_id,
        name=runtime.name,
        workflow_state=runtime.workflow_state,
        cancel_requested=runtime.cancel_requested,
        total_tasks=status.total,
        completed_tasks=status.completed,
        failed_tasks=status.failed,
        running_tasks=status.running,
        pending_tasks=status.pending,
        skipped_tasks=status.skipped,
        default_queue=runtime.default_queue,
        queues_used=runtime.queues_used(),
        created_at=runtime.created_at,
        updated_at=runtime.updated_at,
        error=runtime.error,
    )


def _row_to_entity(row: WorkflowIndexRow) -> dict[str, Any]:
    return {
        "PartitionKey": _partition_key(row.workflow_id),
        "RowKey": row.workflow_id,
        "name": row.name,
        "workflow_state": str(row.workflow_state),
        "cancel_requested": row.cancel_requested,
        "total_tasks": row.total_tasks,
        "completed_tasks": row.completed_tasks,
        "failed_tasks": row.failed_tasks,
        "running_tasks": row.running_tasks,
        "pending_tasks": row.pending_tasks,
        "skipped_tasks": row.skipped_tasks,
        "default_queue": row.default_queue,
        "queues_used": ",".join(row.queues_used),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "error": row.error or "",
    }


def _entity_to_row(entity: dict[str, Any]) -> WorkflowIndexRow:
    queues_raw = entity.get("queues_used", "") or ""
    queues = [q for q in queues_raw.split(",") if q]
    return WorkflowIndexRow(
        workflow_id=entity["RowKey"],
        name=entity.get("name", ""),
        workflow_state=WorkflowState(entity.get("workflow_state", "pending")),
        cancel_requested=bool(entity.get("cancel_requested", False)),
        total_tasks=int(entity.get("total_tasks", 0)),
        completed_tasks=int(entity.get("completed_tasks", 0)),
        failed_tasks=int(entity.get("failed_tasks", 0)),
        running_tasks=int(entity.get("running_tasks", 0)),
        pending_tasks=int(entity.get("pending_tasks", 0)),
        skipped_tasks=int(entity.get("skipped_tasks", 0)),
        default_queue=entity.get("default_queue", ""),
        queues_used=queues,
        created_at=_as_datetime(entity.get("created_at")),
        updated_at=_as_datetime(entity.get("updated_at")),
        error=(entity.get("error") or None),
    )


def _status_from_index_row(row: WorkflowIndexRow) -> WorkflowStatus:
    """Build a :class:`WorkflowStatus` from an index row (no tasks dict)."""
    return WorkflowStatus(
        workflow_id=row.workflow_id,
        name=row.name,
        status=row.workflow_state,
        total=row.total_tasks,
        completed=row.completed_tasks,
        running=row.running_tasks,
        failed=row.failed_tasks,
        pending=row.pending_tasks,
        skipped=row.skipped_tasks,
        default_queue=row.default_queue,
        queues_used=list(row.queues_used),
        created_at=row.created_at,
        updated_at=row.updated_at,
        error=row.error,
    )


def _partition_key(workflow_id: str) -> str:
    """Shard workflows across 16 partitions for index parallelism.

    The shard prefix mirrors the legacy ``wf-XX`` convention so operators
    looking at the index Table see a familiar layout.
    """
    digest = sum(ord(c) for c in workflow_id) & 0x0F
    return f"wf-{digest:02x}"


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    return _utcnow()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class WorkflowPersistence:
    """Azure-backed durable storage for workflow state.

    Construct via :meth:`from_account` or :meth:`from_connection_string`.
    Always use as an async context manager so the underlying SDK clients
    close cleanly.
    """

    def __init__(
        self,
        *,
        prefix: str,
        blob_service: BlobServiceClient,
        table_service: TableServiceClient,
        own_blob_service: bool = False,
        own_table_service: bool = False,
    ) -> None:
        self._prefix = prefix
        self._blob_service = blob_service
        self._table_service = table_service
        self._own_blob_service = own_blob_service
        self._own_table_service = own_table_service
        self._state_container_name = state_container_name(prefix)
        self._output_container_name = output_container_name(prefix)
        self._index_table_name = index_table_name(prefix)
        self._state_container: ContainerClient = blob_service.get_container_client(
            self._state_container_name
        )
        self._output_container: ContainerClient = blob_service.get_container_client(
            self._output_container_name
        )
        self._index_table: TableClient = table_service.get_table_client(self._index_table_name)
        # BlobClient objects are cheap wrappers that share the parent session
        # (via AsyncTransportWrapper), but re-creating them on every load/flush
        # allocates a new Pipeline + TransportWrapper each time.  Cache them
        # keyed by blob name so each workflow reuses the same wrapper object.
        self._blob_client_cache: dict[str, BlobClient] = {}
        self._closed = False

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    async def from_connection_string(
        cls, conn_str: str, *, prefix: str = "JobQ"
    ) -> WorkflowPersistence:
        from azure.data.tables.aio import TableServiceClient
        from azure.storage.blob.aio import BlobServiceClient

        blob_service = BlobServiceClient.from_connection_string(conn_str)
        table_service = TableServiceClient.from_connection_string(conn_str)
        store = cls(
            prefix=prefix,
            blob_service=blob_service,
            table_service=table_service,
            own_blob_service=True,
            own_table_service=True,
        )
        await store._ensure_resources()
        return store

    @classmethod
    async def from_account(cls, account: str, *, prefix: str = "JobQ") -> WorkflowPersistence:
        """Create a persistence handle from an *account* descriptor.

        Dispatches on the shape of *account*:

        - ``"devstoreaccount1"`` → Azurite (uses dev connection strings
          for blob and table endpoints).
        - Anything containing ``"AccountKey="`` or
          ``"SharedAccessSignature="`` → treated as a verbatim
          connection string.
        - Otherwise → bare AAD account name; expands to
          ``https://<account>.{blob,table}.core.windows.net`` URLs and
          uses :func:`~ai4s.jobq.auth.get_token_credential`.
        """
        if account == "devstoreaccount1":
            from ai4s.jobq.backend.storage_queue import _AZURITE_ACCOUNT_KEY

            blob_port = int(os.environ.get("BLOB_PORT", "10000"))
            table_port = int(os.environ.get("TABLE_PORT", "10002"))
            conn_str = (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                f"AccountKey={_AZURITE_ACCOUNT_KEY};"
                f"BlobEndpoint=http://127.0.0.1:{blob_port}/devstoreaccount1;"
                f"TableEndpoint=http://127.0.0.1:{table_port}/devstoreaccount1;"
            )
            return await cls.from_connection_string(conn_str, prefix=prefix)

        if "AccountKey=" in account or "SharedAccessSignature=" in account:
            return await cls.from_connection_string(account, prefix=prefix)

        from azure.data.tables.aio import TableServiceClient
        from azure.storage.blob.aio import BlobServiceClient

        from ai4s.jobq.auth import get_token_credential

        cred = get_token_credential()
        blob_service = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=cred,
        )
        table_service = TableServiceClient(
            endpoint=f"https://{account}.table.core.windows.net",
            credential=cred,
        )
        store = cls(
            prefix=prefix,
            blob_service=blob_service,
            table_service=table_service,
            own_blob_service=True,
            own_table_service=True,
        )
        await store._ensure_resources()
        return store

    async def _ensure_resources(self) -> None:
        # Best effort — already-exists is fine.
        for create in (
            self._state_container.create_container(),
            self._output_container.create_container(),
            self._index_table.create_table(),
        ):
            with contextlib.suppress(ResourceExistsError, HttpResponseError):
                await create

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._own_blob_service:
            with contextlib.suppress(Exception):
                await self._blob_service.close()
        if self._own_table_service:
            with contextlib.suppress(Exception):
                await self._table_service.close()

    async def __aenter__(self) -> WorkflowPersistence:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def _state_blob_client(self, workflow_id: str) -> BlobClient:
        """Return a cached BlobClient for *workflow_id*'s legacy state blob."""
        name = state_blob_name(workflow_id)
        if name not in self._blob_client_cache:
            self._blob_client_cache[name] = self._state_container.get_blob_client(name)
        return self._blob_client_cache[name]

    def _definition_blob_client(self, workflow_id: str) -> BlobClient:
        """Return a cached BlobClient for *workflow_id*'s definition blob."""
        name = definition_blob_name(workflow_id)
        if name not in self._blob_client_cache:
            self._blob_client_cache[name] = self._state_container.get_blob_client(name)
        return self._blob_client_cache[name]

    def _mutable_state_blob_client(self, workflow_id: str) -> BlobClient:
        """Return a cached BlobClient for *workflow_id*'s mutable state blob."""
        name = mutable_state_blob_name(workflow_id)
        if name not in self._blob_client_cache:
            self._blob_client_cache[name] = self._state_container.get_blob_client(name)
        return self._blob_client_cache[name]

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    async def submit(
        self,
        workflow_id: str,
        defn: WorkflowDefinition,
    ) -> WorkflowRuntime:
        """Persist a fresh workflow.  Raises if *workflow_id* already exists."""
        runtime = WorkflowRuntime.from_definition(workflow_id, defn)

        # Write the definition blob (immutable, never overwritten).
        def_client = self._definition_blob_client(workflow_id)
        def_payload = _encode_definition(runtime)
        try:
            await def_client.upload_blob(def_payload, overwrite=False)
        except ResourceExistsError as exc:
            raise WorkflowConflictError(f"workflow {workflow_id} already exists") from exc

        # Write the initial mutable state blob.
        state_client = self._mutable_state_blob_client(workflow_id)
        state_payload = _encode_mutable_state(runtime)
        await state_client.upload_blob(state_payload, overwrite=False)

        await self._upsert_index(_index_row_from_runtime(runtime))
        return runtime

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def load(self, workflow_id: str) -> tuple[WorkflowRuntime, str] | None:
        """Load *workflow_id* from blob storage.

        Returns ``(runtime, etag)`` on success, ``None`` if not found.
        The returned ``etag`` is the mutable state blob's ETag (or the
        legacy blob's ETag for old-format workflows), to pass back to
        :meth:`flush` for CAS.

        Tries the split format (definition + state blobs) first; falls
        back to the legacy single-blob format for older workflows.
        """
        # Try split format: read definition + mutable state in parallel.
        def_client = self._definition_blob_client(workflow_id)
        state_client = self._mutable_state_blob_client(workflow_id)

        async def _read_def() -> tuple[bytes, bool]:
            try:
                dl = await def_client.download_blob()
                return await dl.readall(), True
            except ResourceNotFoundError:
                return b"", False

        async def _read_state() -> tuple[bytes, str, bool]:
            try:
                dl = await state_client.download_blob()
                data = await dl.readall()
                return data, dl.properties.etag, True
            except ResourceNotFoundError:
                return b"", "", False

        (def_data, def_found), (state_data, state_etag, state_found) = await asyncio.gather(
            _read_def(), _read_state()
        )

        if def_found and state_found:
            runtime = _decode_split(def_data, state_data)
            cancel = await self.get_cancel_requested(workflow_id)
            if cancel and not runtime.cancel_requested:
                runtime.cancel_requested = True
            return runtime, state_etag

        # Fallback: legacy single-blob format.
        blob_client = self._state_blob_client(workflow_id)
        try:
            downloader = await blob_client.download_blob()
            data = await downloader.readall()
            etag = downloader.properties.etag
        except ResourceNotFoundError:
            return None
        runtime = _decode_state(data)
        cancel = await self.get_cancel_requested(workflow_id)
        if cancel and not runtime.cancel_requested:
            runtime.cancel_requested = True
        return runtime, etag

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def flush(self, runtime: WorkflowRuntime, etag: str, *, update_index: bool = True) -> str:
        """Write *runtime*'s mutable state back to its state blob with ``If-Match`` *etag*.

        Returns the new etag.  Raises :class:`WorkflowConflictError` if
        the blob was modified since.

        Only the mutable state blob is written (~6x smaller than a full
        blob for large workflows).  The immutable definition blob is
        never touched after submission.

        Pass ``update_index=False`` for intermediate coordinator flushes where
        the workflow is still running — this skips the extra Table Storage write
        and the O(N) ``to_status()`` scan, which dominate flush latency for large
        workflows.  The index is always updated on terminal state transitions so
        that ``watch``/``list``/``status`` reflect the final outcome.
        """
        state_client = self._mutable_state_blob_client(runtime.workflow_id)
        payload = _encode_mutable_state(runtime)
        try:
            result = await state_client.upload_blob(
                payload,
                overwrite=True,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except ResourceModifiedError as exc:
            raise WorkflowConflictError(
                f"workflow {runtime.workflow_id} state blob etag mismatch"
            ) from exc
        new_etag = result.get("etag") or ""
        if update_index:
            # Best-effort: don't fail the flush on index write errors.
            try:
                await self._upsert_index(_index_row_from_runtime(runtime))
            except Exception as exc:
                LOG.warning("Index update failed for %s: %s", runtime.workflow_id, exc)
        return str(new_etag)

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def request_cancel(self, workflow_id: str) -> bool:
        """Set ``cancel_requested=True`` on the index row.

        Returns ``True`` if the row's flag flipped from False to True,
        ``False`` if it was already True or the workflow row is missing.
        The coordinator is responsible for actually applying the cancel
        to its in-memory runtime and flushing to the state blob.
        """
        try:
            entity = await self._index_table.get_entity(
                partition_key=_partition_key(workflow_id),
                row_key=workflow_id,
            )
        except ResourceNotFoundError:
            return False
        if bool(entity.get("cancel_requested", False)):
            return False
        entity["cancel_requested"] = True
        await self._index_table.update_entity(entity, mode="merge")
        return True

    async def get_cancel_requested(self, workflow_id: str) -> bool:
        try:
            entity = await self._index_table.get_entity(
                partition_key=_partition_key(workflow_id),
                row_key=workflow_id,
            )
        except ResourceNotFoundError:
            return False
        return bool(entity.get("cancel_requested", False))

    async def list_cancel_requested_active(self) -> list[str]:
        """Workflow ids whose ``cancel_requested`` is set and that have not yet
        reached a terminal state.  Used by the coordinator's cancel poller.
        """
        filter_expr = "cancel_requested eq true"
        out: list[str] = []
        async for entity in self._index_table.query_entities(query_filter=filter_expr):
            try:
                wf_state = WorkflowState(entity.get("workflow_state", "pending"))
            except ValueError:
                continue
            if WorkflowState.is_terminal(wf_state):
                continue
            out.append(entity["RowKey"])
        return out

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    async def list_workflows(
        self,
        *,
        status: WorkflowState | str | None = None,
        status_filter: Iterable[WorkflowState | str] | None = None,
        limit: int | None = None,
        updated_after: datetime | None = None,
    ) -> list[WorkflowIndexRow]:
        """Return workflows, optionally filtered by state, time, and limited.

        Pure index scan — does not load state blobs.

        Args:
            status: Filter by a single status (convenience alias for status_filter).
            status_filter: Filter by multiple statuses. Ignored if status is given.
            limit: Max number of workflows to return. None = no limit.
            updated_after: Only return workflows updated after this datetime.
        """
        wanted: set[str] | None = None
        if status is not None:
            wanted = {str(status)}
        elif status_filter is not None:
            wanted = {str(s) for s in status_filter}

        rows: list[WorkflowIndexRow] = []
        async for entity in self._index_table.list_entities():
            row = _entity_to_row(entity)
            if wanted is not None and str(row.workflow_state) not in wanted:
                continue
            if updated_after is not None and row.updated_at < updated_after:
                continue
            rows.append(row)
            if limit is not None and len(rows) >= limit:
                break
        return rows

    async def get_index_row(self, workflow_id: str) -> WorkflowIndexRow | None:
        try:
            entity = await self._index_table.get_entity(
                partition_key=_partition_key(workflow_id),
                row_key=workflow_id,
            )
        except ResourceNotFoundError:
            return None
        return _entity_to_row(entity)

    async def total_workflows(self) -> int:
        n = 0
        async for _ in self._index_table.list_entities(
            select=["PartitionKey", "RowKey"],
        ):
            n += 1
        return n

    # ------------------------------------------------------------------
    # Output stash / fetch
    # ------------------------------------------------------------------

    async def stash_output(self, workflow_id: str, task_name: str, payload: bytes) -> str:
        """Upload *payload* bytes to the outputs container and return a blob ref."""
        blob = self._output_container.get_blob_client(output_blob_name(workflow_id, task_name))
        await blob.upload_blob(payload, overwrite=True)
        return output_ref_for_blob(workflow_id, task_name)

    async def fetch_output(self, output_ref: str) -> Any:
        """Resolve *output_ref* to its JSON-decoded value.

        Handles both inline refs (raw JSON) and blob refs (``blob:<path>``).
        """
        if not is_blob_ref(output_ref):
            return json.loads(output_ref)
        path = output_ref.removeprefix("blob:")
        blob = self._output_container.get_blob_client(path)
        downloader = await blob.download_blob()
        data = await downloader.readall()
        return json.loads(data)

    # ------------------------------------------------------------------
    # Purge / delete
    # ------------------------------------------------------------------

    async def delete(self, workflow_id: str) -> bool:
        """Delete a single workflow's blobs, index row, and any output blobs.

        Returns ``True`` if any state blob (the authoritative resource)
        existed prior to the call.  Returns ``False`` on a no-op delete
        — useful for idempotency checks.  Any output blobs and the
        index row are cleaned up as a best effort regardless.
        """
        existed = False

        # Split-format blobs (definition + mutable state).
        for client in (
            self._definition_blob_client(workflow_id),
            self._mutable_state_blob_client(workflow_id),
        ):
            try:
                await client.delete_blob()
                existed = True
            except ResourceNotFoundError:
                pass

        # Legacy single-blob format.
        legacy_client = self._state_blob_client(workflow_id)
        try:
            await legacy_client.delete_blob()
            existed = True
        except ResourceNotFoundError:
            pass

        # Output blobs (best effort — list and delete what's there).
        async for blob in self._output_container.list_blobs(name_starts_with=f"{workflow_id}/"):
            with contextlib.suppress(ResourceNotFoundError):
                await self._output_container.delete_blob(blob.name)

        # Index row (best effort).
        with contextlib.suppress(ResourceNotFoundError):
            await self._index_table.delete_entity(
                partition_key=_partition_key(workflow_id),
                row_key=workflow_id,
            )

        return existed

    async def purge(
        self,
        *,
        terminal_only: bool = True,
        concurrency: int = 16,
    ) -> int:
        """Delete workflows in bulk.

        Args:
            terminal_only: Only delete workflows whose ``workflow_state``
                is terminal.  Default ``True``.
            concurrency: Max simultaneous delete operations.
        """
        rows = await self.list_workflows()
        if terminal_only:
            rows = [r for r in rows if WorkflowState.is_terminal(r.workflow_state)]

        sem = asyncio.Semaphore(max(1, concurrency))
        deleted = 0

        async def _del(wf_id: str) -> None:
            nonlocal deleted
            async with sem:
                if await self.delete(wf_id):
                    deleted += 1

        await asyncio.gather(*(_del(r.workflow_id) for r in rows))
        return deleted

    async def drop_resources(self) -> None:
        """Delete state container, outputs container, and index table entirely.

        Resources are recreated on next ``submit()``.  Use during tests.
        """
        for action in (
            self._state_container.delete_container(),
            self._output_container.delete_container(),
            self._index_table.delete_table(),
        ):
            with contextlib.suppress(ResourceNotFoundError, HttpResponseError):
                await action

    # ------------------------------------------------------------------
    # User-facing client conveniences
    # ------------------------------------------------------------------

    async def get_workflow_status(
        self,
        workflow_id: str,
        *,
        include_tasks: bool = True,
    ) -> WorkflowStatus | None:
        """Return a :class:`WorkflowStatus` for *workflow_id*, or ``None``.

        With ``include_tasks=False`` only the index row is read (one
        Table GET).  With ``include_tasks=True`` the runtime blob is
        also loaded so per-task :class:`TaskStatus` entries are
        populated.
        """
        if include_tasks:
            loaded = await self.load(workflow_id)
            if loaded is None:
                return None
            runtime, _ = loaded
            return runtime.to_status()

        row = await self.get_index_row(workflow_id)
        if row is None:
            return None
        return _status_from_index_row(row)

    async def list_workflow_statuses(
        self,
        *,
        status: WorkflowState | str | None = None,
    ) -> list[WorkflowStatus]:
        """Return lightweight statuses (no tasks) for all workflows.

        Pure index scan, mirrors :meth:`list_workflows` but yields the
        user-facing :class:`WorkflowStatus` view.  Per-task data is
        omitted; call :meth:`get_workflow_status` for a full status.
        """
        status_filter = [status] if status is not None else None
        rows = await self.list_workflows(status_filter=status_filter)
        return [_status_from_index_row(r) for r in rows]

    async def list_recent_terminal(
        self,
        *,
        limit: int = 50,
        status: WorkflowState | str | None = None,
    ) -> list[WorkflowStatus]:
        """Return up to *limit* terminal workflows sorted newest-first.

        Scan-based; intended for "recently finished" dashboards.  When
        *status* is given (must be a terminal state) only matching rows
        are returned.
        """
        if status is not None:
            status_filter: list[WorkflowState | str] | None = [status]
        else:
            status_filter = [
                WorkflowState.COMPLETED,
                WorkflowState.FAILED,
                WorkflowState.CANCELLED,
            ]
        rows = await self.list_workflows(status_filter=status_filter)
        rows = [r for r in rows if WorkflowState.is_terminal(r.workflow_state)]
        rows.sort(key=lambda r: r.updated_at, reverse=True)
        return [_status_from_index_row(r) for r in rows[:limit]]

    async def list_tasks(
        self,
        workflow_id: str,
        *,
        status: TaskState | str | None = None,
        queue: str | None = None,
        name_prefix: str | None = None,
    ) -> list[TaskStatus]:
        """Return tasks of *workflow_id* matching the given filters.

        Loads the runtime blob (one Blob GET) and filters in memory.
        Returns an empty list if the workflow doesn't exist.
        """
        loaded = await self.load(workflow_id)
        if loaded is None:
            return []
        runtime, _ = loaded

        if status is not None:
            wanted_state = TaskState(status) if isinstance(status, str) else status
        else:
            wanted_state = None

        out: list[TaskStatus] = []
        for n, t in runtime.tasks.items():
            if wanted_state is not None and t.state != wanted_state:
                continue
            if queue is not None and t.queue != queue:
                continue
            if name_prefix is not None and not n.startswith(name_prefix):
                continue
            out.append(t.to_status())
        return out

    async def reset_failed_tasks(self, workflow_id: str) -> dict[str, int]:
        """Reset failed / upstream-failed / cancelled tasks for retry.

        Loads the runtime, applies :meth:`WorkflowRuntime.reset_failed_tasks`,
        and flushes.  Returns the counters from the runtime call:
        ``{"reset": N, "now_ready": M, "still_pending": K}``.

        Raises :class:`WorkflowNotFoundError` if no such workflow.
        Raises :class:`WorkflowConflictError` on ETag conflict (caller
        retries).
        """
        loaded = await self.load(workflow_id)
        if loaded is None:
            raise WorkflowNotFoundError(workflow_id)
        runtime, etag = loaded
        counters = runtime.reset_failed_tasks()
        await self.flush(runtime, etag)
        return counters

    async def count_active_workflows_by_status(
        self,
        *,
        on_progress: Callable[[dict[str, int]], None] | None = None,
    ) -> dict[str, int]:
        """Count active (non-terminal) workflows grouped by state.

        Performs a single pass over the index table, calling *on_progress*
        with a running snapshot after each entity so callers can update the
        dashboard incrementally while the scan is in flight.

        Returns:
            dict mapping state name -> count for active (non-terminal) states.
        """
        counts: dict[str, int] = {}
        async for entity in self._index_table.list_entities(
            select=["workflow_state"],
        ):
            state = entity.get("workflow_state", "pending")
            if not WorkflowState.is_terminal(state):
                counts[state] = counts.get(state, 0) + 1
                if on_progress is not None:
                    on_progress(dict(counts))
        return counts

    async def summary(self) -> dict[str, Any]:
        """Aggregate counts across all index rows."""
        rows = await self.list_workflows()
        by_state: dict[str, int] = {}
        total_tasks = 0
        completed = 0
        running = 0
        failed = 0
        pending = 0
        skipped = 0
        for r in rows:
            by_state[str(r.workflow_state)] = by_state.get(str(r.workflow_state), 0) + 1
            total_tasks += r.total_tasks
            completed += r.completed_tasks
            running += r.running_tasks
            failed += r.failed_tasks
            pending += r.pending_tasks
            skipped += r.skipped_tasks
        return {
            "workflows": by_state,
            "total_workflows": len(rows),
            "total_tasks": total_tasks,
            "completed_tasks": completed,
            "running_tasks": running,
            "failed_tasks": failed,
            "pending_tasks": pending,
            "skipped_tasks": skipped,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _upsert_index(self, row: WorkflowIndexRow) -> None:
        await self._index_table.upsert_entity(_row_to_entity(row), mode="replace")
