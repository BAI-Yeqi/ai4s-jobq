# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for workflow file-stash outputs."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import replace

import pytest

from ai4s.jobq.workflow.env import LEGACY_ENV_VARS
from ai4s.jobq.workflow.stash import (
    STASH_MARKER,
    BlobStash,
    BlobStasher,
    _PendingStash,
    stash_json_default,
    stash_object_hook,
)

AZURITE_TABLE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


def _needs_port(port: int) -> bool:
    try:
        import socket

        s = socket.create_connection(("127.0.0.1", port), timeout=1)
        s.close()
    except Exception:
        return True
    return False


skip_without_azurite_blob = pytest.mark.skipif(
    _needs_port(10000),
    reason="Azurite Blob Storage not available on port 10000",
)
skip_without_azurite_queue = pytest.mark.skipif(
    _needs_port(10001),
    reason="Azurite Queue not available on port 10001",
)
skip_without_azurite_table = pytest.mark.skipif(
    _needs_port(10002),
    reason="Azurite Table Storage not available on port 10002",
)


@pytest.fixture(autouse=True)
def clean_workflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "JOBQ_WORKFLOW_PREFIX",
        "JOBQ_WORKFLOW_QUEUES",
        "JOBQ_WORKFLOW_BLOBS",
        "JOBQ_STORAGE",
        *LEGACY_ENV_VARS,
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
async def blob_container(clean_workflow_env: None):
    from ai4s.jobq.workflow.context import _blob_service_client

    name = f"stash-test-{uuid.uuid4().hex[:12]}"
    async with _blob_service_client("devstoreaccount1") as svc:
        container = svc.get_container_client(name)
        await container.create_container()
        try:
            yield name
        finally:
            with contextlib.suppress(Exception):
                await container.delete_container()


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()  # noqa: S324 — integrity check in tests


def _stash_marker(obj: dict) -> dict:
    marker = obj[STASH_MARKER]
    assert marker["v"] == 1
    return marker


class TestWireFormat:
    def test_pending_stash_from_file_does_not_touch_filesystem(self) -> None:
        path = os.path.join(os.sep, "tmp", "x.pt")
        pending = BlobStasher.from_file(path)

        assert isinstance(pending, _PendingStash)
        assert pending.local_path == path
        assert pending.filename == "x.pt"

    def test_json_encodes_pending_marker(self) -> None:
        path = os.path.join(os.sep, "tmp", "x.pt")
        pending = BlobStasher.from_file(path)

        encoded = json.dumps({"a": pending}, default=stash_json_default)

        assert json.loads(encoded) == {
            "a": {
                STASH_MARKER: {
                    "v": 1,
                    "state": "pending",
                    "local_path": path,
                    "filename": "x.pt",
                }
            }
        }

    def test_json_decodes_ready_marker(self) -> None:
        decoded = json.loads(
            json.dumps(
                {
                    "a": {
                        STASH_MARKER: {
                            "v": 1,
                            "state": "ready",
                            "blob_name": "workflow-files/wf/task/x.pt",
                            "md5": "abc123",
                            "size": 12345,
                        }
                    }
                }
            ),
            object_hook=stash_object_hook,
        )

        assert isinstance(decoded["a"], BlobStash)
        assert decoded["a"].blob_name == "workflow-files/wf/task/x.pt"
        assert decoded["a"].md5 == "abc123"
        assert decoded["a"].size == 12345

    def test_pending_marker_cannot_decode_downstream(self) -> None:
        marker = {
            STASH_MARKER: {
                "v": 1,
                "state": "pending",
                "local_path": os.path.join(os.sep, "tmp", "x.pt"),
                "filename": "x.pt",
            }
        }

        with pytest.raises(ValueError, match="leaked downstream"):
            json.loads(json.dumps(marker), object_hook=stash_object_hook)

    def test_unsupported_marker_version_raises(self) -> None:
        marker = {STASH_MARKER: {"v": 999, "state": "ready"}}

        with pytest.raises(ValueError, match="Unsupported"):
            json.loads(json.dumps(marker), object_hook=stash_object_hook)

    def test_ready_marker_missing_required_field_raises(self) -> None:
        marker = {
            STASH_MARKER: {
                "v": 1,
                "state": "ready",
                "blob_name": "workflow-files/wf/task/x.pt",
                "size": 1,
            }
        }

        with pytest.raises(ValueError, match="missing field"):
            json.loads(json.dumps(marker), object_hook=stash_object_hook)

    def test_sentinel_must_be_lone_key(self) -> None:
        payload = {STASH_MARKER: {"v": 1, "state": "ready"}, "other": True}

        decoded = json.loads(json.dumps(payload), object_hook=stash_object_hook)

        assert decoded == payload


@skip_without_azurite_blob
class TestMaterialisePendingStashes:
    async def test_uploads_nested_file_stashes(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
        blob_container: str,
    ) -> None:
        from ai4s.jobq.workflow.context import _blob_service_client
        from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

        monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/StashTests")
        monkeypatch.setenv("JOBQ_WORKFLOW_BLOBS", f"devstoreaccount1/{blob_container}")
        proc = WorkflowShellCommandProcessor(num_workers=1, completion_queue="unused")

        small = tmp_path / "small.bin"
        small_data = b"hello world"
        small.write_bytes(small_data)
        large = tmp_path / "large.bin"
        large_data = os.urandom(5 * 1024 * 1024 + 17)
        large.write_bytes(large_data)
        other = tmp_path / "other.bin"
        other_data = b"metrics"
        other.write_bytes(other_data)

        payload = {
            "results": {
                "weights": BlobStasher.from_file(small),
                "metrics": [
                    {"file": BlobStasher.from_file(large)},
                    BlobStasher.from_file(other),
                ],
            }
        }

        rewritten = await proc._materialise_pending_stashes(
            "wf123",
            "trainer",
            json.dumps(payload, default=stash_json_default),
        )
        decoded = json.loads(rewritten)

        weights = _stash_marker(decoded["results"]["weights"])
        large_marker = _stash_marker(decoded["results"]["metrics"][0]["file"])
        other_marker = _stash_marker(decoded["results"]["metrics"][1])
        expected = [
            (weights, "small.bin", small_data),
            (large_marker, "large.bin", large_data),
            (other_marker, "other.bin", other_data),
        ]
        for marker, filename, data in expected:
            assert marker["state"] == "ready"
            assert marker["blob_name"] == f"workflow-files/wf123/trainer/{filename}"
            assert marker["md5"] == _md5(data)
            assert marker["size"] == len(data)

        async with _blob_service_client("devstoreaccount1") as svc:
            container = svc.get_container_client(blob_container)
            for marker, _filename, data in expected:
                downloader = await container.get_blob_client(marker["blob_name"]).download_blob()
                assert await downloader.readall() == data

    async def test_missing_local_file_raises(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
        blob_container: str,
    ) -> None:
        from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

        monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/StashTests")
        monkeypatch.setenv("JOBQ_WORKFLOW_BLOBS", f"devstoreaccount1/{blob_container}")
        proc = WorkflowShellCommandProcessor(num_workers=1, completion_queue="unused")
        payload = {"missing": BlobStasher.from_file(tmp_path / "missing.bin")}

        with pytest.raises(FileNotFoundError, match="missing file"):
            await proc._materialise_pending_stashes(
                "wf123",
                "trainer",
                json.dumps(payload, default=stash_json_default),
            )

    async def test_no_blob_account_configured_raises(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from ai4s.jobq.workflow.worker import WorkflowShellCommandProcessor

        monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/StashTests")
        monkeypatch.delenv("JOBQ_WORKFLOW_BLOBS", raising=False)
        proc = WorkflowShellCommandProcessor(num_workers=1, completion_queue="unused")
        proc._env = replace(proc._env, blob_account="", blob_container="")
        file_path = tmp_path / "file.bin"
        file_path.write_bytes(b"data")
        payload = {"file": BlobStasher.from_file(file_path)}

        with pytest.raises(RuntimeError, match="no Blob Storage account is configured"):
            await proc._materialise_pending_stashes(
                "wf123",
                "trainer",
                json.dumps(payload, default=stash_json_default),
            )


@skip_without_azurite_blob
class TestBlobStashDownloads:
    async def test_download_read_bytes_open_and_url(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
        blob_container: str,
    ) -> None:
        from ai4s.jobq.workflow.context import _blob_service_client

        data = b"known blob content"
        blob_name = "workflow-files/wf/producer/data.bin"
        async with _blob_service_client("devstoreaccount1") as svc:
            await (
                svc.get_container_client(blob_container)
                .get_blob_client(blob_name)
                .upload_blob(
                    data,
                    overwrite=True,
                )
            )

        monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/StashTests")
        monkeypatch.setenv("JOBQ_WORKFLOW_BLOBS", f"devstoreaccount1/{blob_container}")
        scratch = tmp_path / "tempfile"
        scratch.mkdir()
        monkeypatch.setenv("TMPDIR", str(scratch))
        tempfile.tempdir = str(scratch)
        stash = BlobStash(blob_name=blob_name, md5=_md5(data), size=len(data))

        target = tmp_path / "downloads" / "data.bin"
        assert await asyncio.to_thread(stash.download_to, target) == target
        assert target.read_bytes() == data
        assert await asyncio.to_thread(stash.read_bytes) == data
        fh = await asyncio.to_thread(stash.open, "rb")
        with fh:
            assert fh.read() == data
        assert stash.url == f"http://127.0.0.1:10000/devstoreaccount1/{blob_container}/{blob_name}"

    async def test_md5_mismatch_removes_partial_file(
        self,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
        blob_container: str,
    ) -> None:
        from ai4s.jobq.workflow.context import _blob_service_client

        blob_name = "workflow-files/wf/producer/bad.bin"
        async with _blob_service_client("devstoreaccount1") as svc:
            await (
                svc.get_container_client(blob_container)
                .get_blob_client(blob_name)
                .upload_blob(
                    b"actual",
                    overwrite=True,
                )
            )

        monkeypatch.setenv("JOBQ_WORKFLOW_PREFIX", "devstoreaccount1/StashTests")
        monkeypatch.setenv("JOBQ_WORKFLOW_BLOBS", f"devstoreaccount1/{blob_container}")
        target = tmp_path / "bad.bin"
        stash = BlobStash(blob_name=blob_name, md5=_md5(b"expected"), size=len(b"actual"))

        with pytest.raises(RuntimeError, match="MD5 mismatch"):
            await asyncio.to_thread(stash.download_to, target)
        assert not target.exists()

    def test_open_rejects_write_mode(self) -> None:
        stash = BlobStash(blob_name="workflow-files/wf/task/file.bin", md5="", size=0)

        with pytest.raises(ValueError, match="read-only"):
            stash.open("wb")
