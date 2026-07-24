# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for the worker-side denylist self-check (``denylist_monitor``)."""

from __future__ import annotations

import asyncio

import pytest

from ai4s.jobq import denylist as dl
from ai4s.jobq import denylist_monitor as dm
from ai4s.jobq.denylist import DenylistEntry
from ai4s.jobq.denylist_monitor import (
    DenylistEventHandler,
    DenylistUnavailableError,
    assert_denylist_available,
    worker_image_digests,
)

_DIGEST = "sha256:" + "1" * 64


class _FakeStore:
    """Minimal stand-in for ``ImageDenylist`` used by the handler."""

    def __init__(self, entry: DenylistEntry | None = None, *, raises: bool = False) -> None:
        self._entry = entry
        self._raises = raises
        self.closed = False

    async def is_denied(self, *digests: str) -> DenylistEntry | None:
        if self._raises:
            raise RuntimeError("unreachable")
        if self._entry is not None and self._entry.digest in digests:
            return self._entry
        return None

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> _FakeStore:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()


def _entry(mode: str = "graceful") -> DenylistEntry:
    return DenylistEntry(digest=_DIGEST, reason="bad", shutdown_mode=mode)


async def _run_until(pred, timeout: float = 1.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met within timeout")


class TestWorkerImageDigests:
    def test_reads_env(self, monkeypatch):
        monkeypatch.setenv(dl.IMAGE_DIGEST_ENV, _DIGEST)
        monkeypatch.setenv(dl.IMAGE_DIGEST_ARCH_ENV, "  ")
        assert worker_image_digests() == {_DIGEST}

    def test_empty_when_unset(self, monkeypatch):
        monkeypatch.delenv(dl.IMAGE_DIGEST_ENV, raising=False)
        monkeypatch.delenv(dl.IMAGE_DIGEST_ARCH_ENV, raising=False)
        assert worker_image_digests() == set()


class TestDenylistEventHandler:
    async def test_graceful_match_fires_callback(self, monkeypatch):
        store = _FakeStore(_entry("graceful"))
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(store)))
        fired: list[DenylistEntry] = []
        async with DenylistEventHandler(fired.append, poll_interval_s=0.01, digests={_DIGEST}):
            await _run_until(lambda: bool(fired))
        assert fired
        assert fired[0].shutdown_mode == "graceful"
        assert store.closed

    async def test_hard_mode_passed_through(self, monkeypatch):
        store = _FakeStore(_entry("hard"))
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(store)))
        fired: list[DenylistEntry] = []
        async with DenylistEventHandler(fired.append, poll_interval_s=0.01, digests={_DIGEST}):
            await _run_until(lambda: bool(fired))
        assert fired[0].shutdown_mode == "hard"

    async def test_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("JOBQ_DENYLIST_DISABLE", "1")
        called = []
        async with DenylistEventHandler(
            called.append, poll_interval_s=0.01, digests={_DIGEST}
        ) as h:
            assert h._task is None
        assert called == []

    async def test_no_digests_inactive(self, monkeypatch):
        monkeypatch.delenv("JOBQ_DENYLIST_DISABLE", raising=False)
        called = []
        async with DenylistEventHandler(called.append, poll_interval_s=0.01, digests=set()) as h:
            assert h._task is None
        assert called == []

    async def test_no_store_fail_open(self, monkeypatch):
        monkeypatch.delenv("JOBQ_DENYLIST_DISABLE", raising=False)
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(None)))
        called = []
        async with DenylistEventHandler(
            called.append, poll_interval_s=0.01, digests={_DIGEST}
        ) as h:
            assert h._task is None
        assert called == []

    async def test_no_match_no_fire(self, monkeypatch):
        store = _FakeStore(_entry("graceful"))
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(store)))
        fired = []
        other = "sha256:" + "7" * 64
        async with DenylistEventHandler(fired.append, poll_interval_s=0.01, digests={other}):
            await asyncio.sleep(0.05)
        assert fired == []

    async def test_unreachable_shutdown_when_enabled(self, monkeypatch):
        store = _FakeStore(raises=True)
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(store)))
        unreachable = []
        async with DenylistEventHandler(
            lambda e: None,
            poll_interval_s=0.01,
            digests={_DIGEST},
            shutdown_on_unreachable=True,
            unreachable_grace_s=0.0,
            on_unreachable=lambda: unreachable.append(True),
        ):
            await _run_until(lambda: bool(unreachable))
        assert unreachable == [True]

    async def test_unreachable_fail_open_by_default(self, monkeypatch):
        store = _FakeStore(raises=True)
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(store)))
        fired = []
        async with DenylistEventHandler(fired.append, poll_interval_s=0.01, digests={_DIGEST}):
            await asyncio.sleep(0.05)
        assert fired == []


class TestAssertDenylistAvailable:
    async def test_noop_when_not_required(self, monkeypatch):
        monkeypatch.setattr(dl, "denylist_require_configured", lambda: False)
        await assert_denylist_available()  # no raise

    async def test_raises_when_required_but_unset(self, monkeypatch):
        monkeypatch.setattr(dl, "denylist_require_configured", lambda: True)
        monkeypatch.setattr(dl, "denylist_account", lambda: None)
        with pytest.raises(DenylistUnavailableError):
            await assert_denylist_available()

    async def test_ok_when_required_and_reachable(self, monkeypatch):
        monkeypatch.setattr(dl, "denylist_require_configured", lambda: True)
        monkeypatch.setattr(dl, "denylist_account", lambda: "devstoreaccount1")
        store = _FakeStore()
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(store)))
        await assert_denylist_available()

    async def test_raises_when_required_and_unreachable(self, monkeypatch):
        monkeypatch.setattr(dl, "denylist_require_configured", lambda: True)
        monkeypatch.setattr(dl, "denylist_account", lambda: "devstoreaccount1")
        store = _FakeStore(raises=True)
        monkeypatch.setattr(dm.ImageDenylist, "open", staticmethod(lambda: _async(store)))
        with pytest.raises(DenylistUnavailableError):
            await assert_denylist_available()


async def _async(value):
    return value
