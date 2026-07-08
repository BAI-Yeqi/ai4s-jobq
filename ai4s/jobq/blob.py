# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import hashlib
import io
import logging
import os
import pickle
import typing as ty
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile, TemporaryDirectory

import aiohttp
from azure.core.credentials_async import AsyncTokenCredential
from azure.storage.blob import BlobProperties
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from rich.progress import TextColumn

from ai4s.jobq.auth import get_token_credential
from ai4s.jobq.work import _AbstractAsyncContextManager

LOG = logging.getLogger(__name__)
T = ty.TypeVar("T")


@dataclass(frozen=True)
class BlobStash:
    """Lazy reference to a file already uploaded to Blob Storage.

    Returned both by :class:`BlobContainer` stash helpers
    (:meth:`BlobContainer.stash_as_pickle`, :meth:`BlobContainer.stash_as_json`)
    and by the workflow :func:`ai4s.jobq.workflow.context.get_upstream_output`
    machinery. The legacy attribute names ``filename`` and ``md5sum`` are
    preserved as read-only aliases so callers of the old two-field
    dataclass keep working.

    Attributes:
        blob_name: Path of the blob within the container, e.g.
            ``"workflow-files/wf123/featurize/foo.pt"`` or
            ``"abc123.pck"``.
        md5: Hex-encoded MD5 of the uploaded file. Verified on download
            for the workflow materialisation API.
        size: Size of the uploaded file in bytes. ``0`` when unknown.

    For the workflow flow, ``_account``/``_container`` may be set so
    that :meth:`download_to`, :meth:`read_bytes`, and :meth:`open` can
    materialise the blob without an explicit client.
    """

    blob_name: str
    md5: str = ""
    size: int = 0

    # Internal: populated lazily by the workflow JSON sentinel hook
    # (``ai4s.jobq.workflow.stash.stash_object_hook``) so downstream
    # tasks can call ``download_to`` without configuring a client.
    # Not part of the wire format.
    _account: str | None = field(default=None, repr=False, compare=False)
    _container: str | None = field(default=None, repr=False, compare=False)

    @property
    def filename(self) -> str:
        """Trailing path component of :attr:`blob_name`.

        Backward-compatible attribute that the old two-field
        ``BlobStash`` returned by :class:`BlobContainer` exposed.
        """
        return self.blob_name.rsplit("/", 1)[-1]

    @property
    def md5sum(self) -> str:
        """Alias for :attr:`md5` kept for backwards compatibility."""
        return self.md5

    @property
    def url(self) -> str | None:
        """Absolute https URL of the blob, if the account is known.

        Returns ``None`` when called outside a workflow worker context
        (i.e. when ``JOBQ_WORKFLOW_BLOBS``/``JOBQ_WORKFLOW_PREFIX`` cannot be
        resolved and no account/container was attached at construction).
        """
        account, container = self._resolve_account_container()
        if not account or not container:
            return None
        if account == "devstoreaccount1":
            # Azurite default endpoint
            return f"http://127.0.0.1:10000/devstoreaccount1/{container}/{self.blob_name}"
        return f"https://{account}.blob.core.windows.net/{container}/{self.blob_name}"

    def to_marker(self) -> dict[str, ty.Any]:
        """Render this stash as the workflow JSON sentinel marker."""
        # Imported lazily to avoid a top-level dependency on workflow code.
        from ai4s.jobq.workflow.stash import STASH_MARKER, STASH_VERSION

        return {
            STASH_MARKER: {
                "v": STASH_VERSION,
                "state": "ready",
                "blob_name": self.blob_name,
                "md5": self.md5,
                "size": self.size,
            }
        }

    # ----- materialisation API (sync; downstream user-script friendly) -----

    def download_to(self, local_path: str | os.PathLike[str]) -> Path:
        """Download the blob to *local_path* and return it as a :class:`Path`.

        Verifies the MD5 checksum after download. Parent directories are
        created if missing. Existing files at *local_path* are
        overwritten.
        """
        from ai4s.jobq.workflow._stash_io import download_blob_to_path

        return download_blob_to_path(self, Path(os.fspath(local_path)))

    def read_bytes(self) -> bytes:
        """Download the blob and return its full contents in memory."""
        from ai4s.jobq.workflow._stash_io import read_blob_bytes

        return read_blob_bytes(self)

    def open(self, mode: str = "rb") -> ty.IO[ty.Any]:
        """Download the blob to a temp file and return an open file object.

        Caller is responsible for closing the file. The temp file is
        deleted automatically when the file object is closed (best
        effort).
        """
        if "w" in mode or "a" in mode or "+" in mode:
            raise ValueError(
                f"BlobStash.open does not support write mode {mode!r}; blob is read-only"
            )
        import contextlib
        import tempfile

        suffix = "_" + self.filename if self.filename else ""
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        try:
            self.download_to(tmp.name)
            tmp.close()
            return open(tmp.name, mode)
        except BaseException:
            tmp.close()
            with contextlib.suppress(OSError):
                os.unlink(tmp.name)
            raise

    def _resolve_account_container(self) -> tuple[str | None, str | None]:
        """Best-effort lookup of the blob account/container for this stash.

        Uses values cached on the instance (set by the workflow JSON
        sentinel hook or by ``BlobContainer`` stash helpers) if present;
        otherwise falls back to the workflow environment.
        """
        if self._account and self._container:
            return self._account, self._container
        try:
            from ai4s.jobq.workflow.env import WorkflowEnv

            env = WorkflowEnv.from_environ()
        except Exception:
            return None, None
        return env.blob_account, env.blob_container


class BlobStats(TextColumn):
    def __init__(self, text_format: str) -> None:
        super().__init__(text_format)
        self.n_downloaded: int = 0
        self.n_uploaded: int = 0
        self.download_bytes: int = 0
        self.upload_bytes: int = 0
        self.start_time: datetime = datetime.now()
        self._fixed_text = ""

    def __str__(self) -> str:
        td = datetime.now() - self.start_time
        down_mb = self.download_bytes / 1024 / 1024
        up_mb = self.upload_bytes / 1024 / 1024
        down_speed_mb = down_mb / td.total_seconds()
        up_speed_mb = up_mb / td.total_seconds()
        return (
            self._fixed_text + ": "
            f"Downloaded {self.n_downloaded} files ({down_mb:5.1f} MB) "
            f"at {down_speed_mb:4.2f} MB/s, "
            f"uploaded {self.n_uploaded} files ({up_mb:5.1f} MB) "
            f"at {up_speed_mb:4.2f} MB/s."
        )

    @property
    def text_format(self) -> str:
        return str(self)

    @text_format.setter
    def text_format(self, value: str) -> None:
        self._fixed_text = value


class BlobContainer(_AbstractAsyncContextManager["BlobContainer"]):
    def __init__(
        self,
        storage_account: str,
        container: str,
        max_concurrency: int = 10,
        max_blob_connections: int = 1000,
        prefix: str = "",
        credential: str | AsyncTokenCredential | None = None,
        output_filename: str = "output.txt",
    ) -> None:
        if credential is None:
            credential = get_token_credential()
        self._credential = credential
        self._max_blob_connections = max_blob_connections
        self._max_concurrency = max_concurrency
        self._storage_account = storage_account
        self._container = container
        self._prefix = prefix
        self._output_filename = output_filename
        self.stats = BlobStats(self.__class__.__qualname__)
        self._stack = AsyncExitStack()
        super().__init__()

    @cached_property
    def client(self) -> ContainerClient:
        return self._blob_client.get_container_client(self._container)

    async def __aenter__(self) -> "BlobContainer":
        await super().__aenter__()
        self.__conn = aiohttp.TCPConnector(limit_per_host=self._max_blob_connections)
        await self._stack.enter_async_context(self.__conn)
        self.__session = aiohttp.ClientSession(connector=self.__conn)
        await self._stack.enter_async_context(self.__session)
        self._blob_client = BlobServiceClient(
            account_url=f"https://{self._storage_account}.blob.core.windows.net",
            credential=self._credential,
            session=self.__session,
        )
        await self._stack.enter_async_context(self._blob_client)
        return self

    async def __aexit__(self, *args: ty.Any) -> None:
        await super().__aexit__(*args)
        await self._stack.__aexit__(*args)
        return

    async def download_file(
        self,
        blob_name: str,
        tmp_dir: str,
        max_concurrency: int | None = None,
        use_basename: bool = False,
        **kwargs: ty.Any,
    ) -> str:
        if use_basename:
            filename = os.path.join(tmp_dir, os.path.basename(blob_name))
        else:
            filename = NamedTemporaryFile(dir=tmp_dir, delete=False, **kwargs).name
        try:
            LOG.debug("Downloading %s to %s", blob_name, filename)
            max_concurrency = max_concurrency or self._max_concurrency
            stream = await self.client.download_blob(blob_name, max_concurrency=max_concurrency)
            with open(filename, "wb") as f:
                await stream.readinto(f)
            self.stats.n_downloaded += 1
            self.stats.download_bytes += stream.size if stream.size else 0
            LOG.debug("Done downloading %s", blob_name)
            return filename
        except Exception:
            LOG.debug("Error downloading %s", blob_name)
            raise

    async def download_file_with_properties(
        self,
        blob_name: str,
        tmp_dir: str,
        max_concurrency: int | None = None,
        use_basename: bool = False,
        **kwargs: ty.Any,
    ) -> tuple[str, BlobProperties]:
        filename = await self.download_file(
            blob_name=blob_name,
            tmp_dir=tmp_dir,
            max_concurrency=max_concurrency,
            use_basename=use_basename,
            **kwargs,
        )
        blob_client = self.client.get_blob_client(blob_name)
        properties = await blob_client.get_blob_properties()

        return filename, properties

    async def download_folder(
        self,
        blob_dir: str,
        tmp_dir: str,
        max_concurrent_files: int = 10,
        max_concurrency: int | None = None,
    ) -> None:
        """Downloads a directory from blob storage to a local directory."""
        futures = []
        blob_walk = self.client.walk_blobs(name_starts_with=blob_dir.rstrip("/") + "/")
        semaphore = asyncio.Semaphore(max_concurrent_files)
        async for blob in blob_walk:
            futures.append(  # noqa: PERF401 — async for can't use list comprehension
                self._bounded_download_file(
                    semaphore=semaphore,
                    blob_name=blob.name,  # pyright: ignore
                    tmp_dir=tmp_dir,
                    max_concurrency=max_concurrency,
                )
            )

        await asyncio.gather(*futures)

    async def _bounded_download_file(
        self,
        semaphore: asyncio.Semaphore,
        blob_name: str,
        tmp_dir: str,
        max_concurrency: int | None = None,
    ) -> None:
        async with semaphore:
            await self.download_file(
                blob_name=blob_name,
                tmp_dir=tmp_dir,
                max_concurrency=max_concurrency,
                use_basename=True,
            )

    async def upload_file(
        self,
        local_file: str,
        blob_name: str,
        overwrite: bool = True,
        max_concurrency: int | None = None,
    ) -> None:
        """Uploads a local file to blob storage."""
        LOG.debug("Uploading %s to %s", local_file, blob_name)
        max_concurrency = max_concurrency or self._max_concurrency
        with open(local_file, "rb") as f:
            await self.client.upload_blob(
                blob_name, f, overwrite=overwrite, max_concurrency=max_concurrency
            )
        self.stats.n_uploaded += 1
        self.stats.upload_bytes += os.path.getsize(local_file)
        LOG.debug("Done uploading %s", blob_name)

    async def stash_as_pickle(self, data: ty.Any, filename: str | None = None) -> "BlobStash":
        """Stores some data in blob storage by pickling it.

        Args:
            data: any picklable object.
            filename: (optional) if provided, stores the cache under the specified filename. Otherwise, a random
                filename is generated.
        """
        if filename is None:
            filename = f"{uuid.uuid4()}.pck"
        content = pickle.dumps(data)
        md5sum = hashlib.md5(content).hexdigest()
        data = io.BytesIO(content)
        data.seek(0)
        await self.client.upload_blob(filename, data, overwrite=True)
        return BlobStash(
            blob_name=filename,
            md5=md5sum,
            size=len(content),
            _account=self._storage_account,
            _container=self._container,
        )

    async def unstash_from_pickle(self, filename, md5sum) -> ty.Any:
        """Retrieves a blob stash from blob storage, ideally created by stash_as_pickle()."""
        expected_md5sum = md5sum
        stream = await self.client.download_blob(filename)
        content = await stream.readall()
        md5sum = hashlib.md5(content).hexdigest()
        if expected_md5sum and md5sum != expected_md5sum:
            raise RuntimeError(
                f"The md5 hash ({md5sum}) of {filename} does not match the expected md5 hash ({expected_md5sum})!"
            )
        return pickle.loads(content)

    async def stash_as_json(self, data: ty.Any, filename: str | None = None) -> "BlobStash":
        """Store JSON-serializable data in blob storage.

        Unlike :meth:`stash_as_pickle`, this uses JSON encoding which is
        safer for cross-process data exchange (no arbitrary code execution
        on deserialization).

        Args:
            data: any JSON-serializable object.
            filename: blob name (default: random UUID with ``.json`` suffix).
        """
        import json

        if filename is None:
            filename = f"{uuid.uuid4()}.json"
        content = json.dumps(data).encode()
        md5sum = hashlib.md5(content).hexdigest()
        await self.client.upload_blob(filename, io.BytesIO(content), overwrite=True)
        return BlobStash(
            blob_name=filename,
            md5=md5sum,
            size=len(content),
            _account=self._storage_account,
            _container=self._container,
        )

    async def unstash_from_json(self, filename: str, md5sum: str | None = None) -> ty.Any:
        """Retrieve JSON data previously stored with :meth:`stash_as_json`."""
        import json

        stream = await self.client.download_blob(filename)
        content = await stream.readall()
        if md5sum:
            actual = hashlib.md5(content).hexdigest()
            if actual != md5sum:
                raise RuntimeError(
                    f"The md5 hash ({actual}) of {filename} does not match "
                    f"the expected md5 hash ({md5sum})!"
                )
        return json.loads(content)

    async def upload_from_folder(
        self,
        local_dir: str,
        blob_dir: str,
        overwrite: bool = True,
        max_concurrency: int | None = None,
    ) -> None:
        """
        Uploads all files in a local directory to a blob directory.
        Note, that the overwrite/max_concurrency parameters are only passed through to individual file uploads,
        and do *not* apply to the files as a group.
        """
        futures = []
        blob_path = PurePosixPath(blob_dir)
        for root, _, files in os.walk(local_dir):
            for file in files:
                local_file = os.path.join(root, file)
                blob_name = blob_path / os.path.relpath(local_file, local_dir)
                futures.append(
                    self.upload_file(
                        local_file,
                        str(blob_name),
                        overwrite=overwrite,
                        max_concurrency=max_concurrency,
                    )
                )
        await asyncio.gather(*futures)

    async def blob_exists(self, blob_name: str) -> bool:
        return await self.client.get_blob_client(blob_name).exists()


class CrossWorkerCache(ty.Generic[T]):
    LOCK = asyncio.Lock()

    def __init__(self) -> None:
        self._cache: dict[str, T] = {}

    async def retrieve(self, key: str, **kwargs: ty.Any) -> T:
        raise NotImplementedError

    async def get(self, key: str, **kwargs: ty.Any) -> T:
        async with self.LOCK:
            if key not in self._cache:
                self._cache[key] = await self.retrieve(key, **kwargs)
            return self._cache[key]


class BlobDownloadCache(CrossWorkerCache[str], _AbstractAsyncContextManager["BlobDownloadCache"]):
    def __init__(self, blob_enumerator: BlobContainer, tmp_dir: str | None = None):
        super().__init__()
        self.blob_enumerator = blob_enumerator
        self.tmp_dir_name = tmp_dir
        self.tmp_dir: TemporaryDirectory[str] | None = None

    async def __aenter__(self) -> "BlobDownloadCache":
        if self.tmp_dir_name is None:
            self.tmp_dir = TemporaryDirectory()
            self.tmp_dir.__enter__()
            self.tmp_dir_name = self.tmp_dir.name
        else:
            self.tmp_dir_name = os.path.abspath(self.tmp_dir_name)
        return self

    async def __aexit__(self, *args: ty.Any) -> None:
        if self.tmp_dir is not None:
            self.tmp_dir.__exit__(*args)
        return

    async def retrieve(self, key: str, **kwargs: ty.Any) -> str:
        """Downloads a blob to a temporary directory and returns its local name."""
        assert self.tmp_dir_name is not None, (
            "temporary directory not set, use the cache as an async context manager."
        )
        return await self.blob_enumerator.download_file(key, tmp_dir=self.tmp_dir_name, **kwargs)
