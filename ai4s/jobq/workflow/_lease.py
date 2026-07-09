# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Sentinel blob lease for coordinator single-writer enforcement.

Acquires a 60-second lease on a small lock blob in the workflow state
container.  The coordinator renews the lease periodically; a second
coordinator attempting to start against the same prefix will fail fast
with a clear error rather than silently racing.

Usage::

    async with CoordinatorLease(container_client, prefix="JobQ") as lease:
        # lease.renew_task is running in the background
        await coordinator.run()
    # lease released on exit
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING

from azure.core.exceptions import HttpResponseError, ResourceExistsError

if TYPE_CHECKING:
    from types import TracebackType

    from azure.storage.blob.aio import BlobClient, ContainerClient

LOG = logging.getLogger(__name__)

_LOCK_BLOB_NAME = "_coordinator.lock"
_LEASE_DURATION_S = 60
# Renew well before expiry to tolerate transient delays.
_RENEW_INTERVAL_S = 15


class CoordinatorLeaseError(RuntimeError):
    """Raised when the lease cannot be acquired (another coordinator is running)."""


async def break_lease(container: ContainerClient, *, prefix: str = "JobQ") -> None:
    """Forcibly break the coordinator lease.

    Use this when a coordinator crashed without releasing its lease and
    you need to start a new one before the 60-second expiry.  Safe to
    call even when no lease is held (no-op in that case).

    Parameters
    ----------
    container : ContainerClient
        The ``{prefix}-workflows`` container.
    prefix : str
        Used only for log messages.
    """
    blob: BlobClient = container.get_blob_client(_LOCK_BLOB_NAME)
    from azure.storage.blob.aio import BlobLeaseClient

    lease_client = BlobLeaseClient(blob)
    try:
        await lease_client.break_lease(lease_break_period=0)
        LOG.info("Broke coordinator lease for prefix %r", prefix)
    except HttpResponseError as exc:
        if exc.status_code == 409:
            # No lease held — nothing to break.
            LOG.debug("No active lease to break for prefix %r", prefix)
        elif exc.status_code == 404:
            LOG.debug("Lock blob does not exist for prefix %r — nothing to break", prefix)
        else:
            raise


class CoordinatorLease:
    """Exclusive sentinel lease for the coordinator process.

    Parameters
    ----------
    container : ContainerClient
        The ``{prefix}-workflows`` container that holds state blobs.
    prefix : str
        Used only for error messages.
    """

    def __init__(self, container: ContainerClient, *, prefix: str = "JobQ") -> None:
        self._container = container
        self._prefix = prefix
        self._blob: BlobClient = container.get_blob_client(_LOCK_BLOB_NAME)
        self._lease_id: str | None = None
        self._renew_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> CoordinatorLease:
        await self._ensure_lock_blob()
        await self._acquire()
        self._renew_task = asyncio.create_task(self._renew_loop(), name="coordinator-lease-renew")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._renew_task is not None:
            self._renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._renew_task
            self._renew_task = None
        await self._release()

    async def _ensure_lock_blob(self) -> None:
        """Create the lock blob if it doesn't exist (empty, zero-byte blob)."""
        from azure.storage.blob import ContentSettings

        with contextlib.suppress(ResourceExistsError, HttpResponseError):
            await self._blob.upload_blob(
                b"",
                overwrite=False,
                content_settings=ContentSettings(content_type="application/octet-stream"),
            )

    async def _acquire(self) -> None:
        """Acquire a fixed-duration lease. Raises CoordinatorLeaseError on conflict."""
        from azure.storage.blob.aio import BlobLeaseClient

        lease_client = BlobLeaseClient(self._blob)
        try:
            await lease_client.acquire(lease_duration=_LEASE_DURATION_S)
        except HttpResponseError as exc:
            if exc.status_code == 409:
                raise CoordinatorLeaseError(
                    f"Another coordinator is already running for prefix {self._prefix!r}. "
                    f"Only one coordinator process per prefix is allowed. "
                    f"If the previous coordinator crashed, the lease will expire within "
                    f"{_LEASE_DURATION_S}s and a new coordinator can start."
                ) from exc
            raise
        self._lease_id = lease_client.id
        LOG.info(
            "Acquired coordinator lease for prefix %r (lease_id=%s)", self._prefix, self._lease_id
        )

    async def _release(self) -> None:
        """Release the lease on clean shutdown."""
        if self._lease_id is None:
            return
        from azure.storage.blob.aio import BlobLeaseClient

        lease_client = BlobLeaseClient(self._blob, lease_id=self._lease_id)
        try:
            await lease_client.release()
            LOG.info("Released coordinator lease for prefix %r", self._prefix)
        except Exception:
            LOG.debug("Failed to release lease (will expire naturally)", exc_info=True)
        finally:
            self._lease_id = None

    async def _renew_loop(self) -> None:
        """Periodically renew the lease to keep it alive."""
        from azure.storage.blob.aio import BlobLeaseClient

        while True:
            await asyncio.sleep(_RENEW_INTERVAL_S)
            if self._lease_id is None:
                return
            lease_client = BlobLeaseClient(self._blob, lease_id=self._lease_id)
            try:
                await lease_client.renew()
                LOG.debug("Renewed coordinator lease for prefix %r", self._prefix)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.warning(
                    "Failed to renew coordinator lease for prefix %r; "
                    "coordinator may lose exclusivity if renewal keeps failing",
                    self._prefix,
                    exc_info=True,
                )
