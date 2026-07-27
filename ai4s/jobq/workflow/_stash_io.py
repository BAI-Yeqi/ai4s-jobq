# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Sync I/O helpers for :class:`ai4s.jobq.workflow.stash.BlobStash`.

Kept in a separate module so importing :mod:`ai4s.jobq.workflow.stash`
(used inside user task scripts via ``BlobStasher.from_file``) does not
pull in :mod:`azure.storage.blob` until a download is actually
requested.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from ai4s.jobq.workflow.stash import BlobStash


# 4 MB chunk: large enough to amortise per-chunk overhead, small enough
# that the in-memory buffer is bounded for ordinary checkpoints.
_DOWNLOAD_CHUNK = 4 * 1024 * 1024


def download_blob_to_path(stash: BlobStash, target: Path) -> Path:
    """Sync wrapper that streams *stash* to *target* and verifies md5."""
    target.parent.mkdir(parents=True, exist_ok=True)
    asyncio.run(_download_async(stash, target))
    return target


def read_blob_bytes(stash: BlobStash) -> bytes:
    """Sync wrapper that downloads *stash* into memory and verifies md5."""
    return asyncio.run(_read_bytes_async(stash))


async def _download_async(stash: BlobStash, target: Path) -> None:
    account, container = stash._resolve_account_container()
    if not account or not container:
        raise RuntimeError(
            "Cannot download BlobStash: workflow blob account/container "
            "not configured. Set JOBQ_WORKFLOW_BLOBS or JOBQ_WORKFLOW_PREFIX."
        )
    from ai4s.jobq.workflow.context import _blob_service_client

    md5 = hashlib.md5(usedforsecurity=False)
    total = 0
    async with _blob_service_client(account) as svc:
        client = svc.get_container_client(container).get_blob_client(stash.blob_name)
        downloader = await client.download_blob(max_concurrency=4)
        with target.open("wb") as fh:
            async for chunk in downloader.chunks():
                fh.write(chunk)
                md5.update(chunk)
                total += len(chunk)

    if stash.md5 and md5.hexdigest() != stash.md5:
        await asyncio.to_thread(target.unlink, missing_ok=True)
        raise RuntimeError(
            f"MD5 mismatch downloading {stash.blob_name}: "
            f"expected {stash.md5}, got {md5.hexdigest()}"
        )
    if stash.size and total != stash.size:
        await asyncio.to_thread(target.unlink, missing_ok=True)
        raise RuntimeError(
            f"Size mismatch downloading {stash.blob_name}: expected {stash.size} bytes, got {total}"
        )


async def _read_bytes_async(stash: BlobStash) -> bytes:
    account, container = stash._resolve_account_container()
    if not account or not container:
        raise RuntimeError(
            "Cannot read BlobStash: workflow blob account/container "
            "not configured. Set JOBQ_WORKFLOW_BLOBS or JOBQ_WORKFLOW_PREFIX."
        )
    from ai4s.jobq.workflow.context import _blob_service_client

    async with _blob_service_client(account) as svc:
        client = svc.get_container_client(container).get_blob_client(stash.blob_name)
        downloader = await client.download_blob(max_concurrency=4)
        data = await downloader.readall()

    if stash.md5:
        actual = hashlib.md5(data, usedforsecurity=False).hexdigest()
        if actual != stash.md5:
            raise RuntimeError(
                f"MD5 mismatch downloading {stash.blob_name}: expected {stash.md5}, got {actual}"
            )
    return bytes(data)
