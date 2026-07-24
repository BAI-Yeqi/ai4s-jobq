# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for :mod:`ai4s.jobq.denylist` (store + config helpers).

The Azure-Table-backed tests use Azurite on the standard table port (10002)
and are skipped when it is not running.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from ai4s.jobq.denylist import (
    DenylistEntry,
    ImageDenylist,
    denylist_account,
    denylist_configured,
    denylist_disabled,
    denylist_poll_interval_s,
    denylist_require_configured,
    denylist_shutdown_on_unreachable,
    denylist_table_name,
    normalize_digest,
    running_on_singularity,
)


def _table_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 10002), timeout=1).close()
        return True
    except OSError:
        return False


skip_without_table = pytest.mark.skipif(
    not _table_up(), reason="Azurite table storage must be running on port 10002"
)

_HEX = "a" * 64
_DIGEST = f"sha256:{_HEX}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── normalize_digest ──────────────────────────────────────────────────────────


class TestNormalizeDigest:
    def test_bare_hex(self):
        assert normalize_digest(_HEX) == _DIGEST

    def test_prefixed(self):
        assert normalize_digest(_DIGEST) == _DIGEST

    def test_full_reference(self):
        assert normalize_digest(f"reg.azurecr.io/repo:tag@{_DIGEST}") == _DIGEST

    def test_uppercase_and_whitespace(self):
        assert normalize_digest(f"  SHA256:{'A' * 64}  ") == f"sha256:{_HEX}"

    @pytest.mark.parametrize("bad", ["", "sha256:xyz", "nothex" * 8, "sha256:" + "a" * 63])
    def test_invalid_raises(self, bad):
        with pytest.raises(ValueError, match="digest"):
            normalize_digest(bad)


# ── config helpers ────────────────────────────────────────────────────────────


class TestConfig:
    def test_defaults(self, monkeypatch):
        for var in (
            "JOBQ_DENYLIST_ACCOUNT",
            "JOBQ_DENYLIST_TABLE",
            "JOBQ_DENYLIST_DISABLE",
            "JOBQ_DENYLIST_POLL_INTERVAL_S",
            "JOBQ_DENYLIST_REQUIRE",
            "JOBQ_FORCE_SINGULARITY",
        ):
            monkeypatch.delenv(var, raising=False)
        assert denylist_account() == "jobq0central"
        assert denylist_table_name() == "JobQImageDenylist"
        assert denylist_disabled() is False
        assert denylist_configured() is True
        assert denylist_poll_interval_s() == 60.0

    def test_account_and_table(self, monkeypatch):
        monkeypatch.setenv("JOBQ_DENYLIST_ACCOUNT", "myacct")
        monkeypatch.setenv("JOBQ_DENYLIST_TABLE", "MyTable")
        assert denylist_account() == "myacct"
        assert denylist_table_name() == "MyTable"
        assert denylist_configured() is True

    def test_disable_overrides_configured(self, monkeypatch):
        monkeypatch.setenv("JOBQ_DENYLIST_ACCOUNT", "myacct")
        monkeypatch.setenv("JOBQ_DENYLIST_DISABLE", "1")
        assert denylist_disabled() is True
        assert denylist_configured() is False

    def test_poll_interval_floor_and_bad(self, monkeypatch):
        monkeypatch.setenv("JOBQ_DENYLIST_POLL_INTERVAL_S", "0.1")
        assert denylist_poll_interval_s() == 1.0
        monkeypatch.setenv("JOBQ_DENYLIST_POLL_INTERVAL_S", "notanumber")
        assert denylist_poll_interval_s() == 60.0

    def test_singularity_force(self, monkeypatch):
        monkeypatch.delenv("JOBQ_DENYLIST_REQUIRE", raising=False)
        monkeypatch.setenv("JOBQ_FORCE_SINGULARITY", "1")
        assert running_on_singularity() is True
        monkeypatch.setenv("JOBQ_DENYLIST_ACCOUNT", "acct")
        assert denylist_require_configured() is True
        assert denylist_shutdown_on_unreachable() is True

    def test_require_explicit_opt_out(self, monkeypatch):
        monkeypatch.setenv("JOBQ_FORCE_SINGULARITY", "1")
        monkeypatch.setenv("JOBQ_DENYLIST_REQUIRE", "0")
        assert denylist_require_configured() is False

    def test_require_false_when_disabled(self, monkeypatch):
        monkeypatch.setenv("JOBQ_FORCE_SINGULARITY", "1")
        monkeypatch.setenv("JOBQ_DENYLIST_DISABLE", "1")
        assert denylist_require_configured() is False


# ── DenylistEntry ─────────────────────────────────────────────────────────────


class TestDenylistEntry:
    def test_bad_mode_coerced_on_read(self):
        entity = {"RowKey": _HEX, "digest": _DIGEST, "shutdown_mode": "bogus"}
        assert DenylistEntry._from_entity(entity).shutdown_mode == "graceful"

    def test_effective_at_roundtrip(self):
        eff = datetime(2030, 1, 1, tzinfo=timezone.utc)
        entity = DenylistEntry(digest=_DIGEST, effective_at=eff)._to_entity()
        assert entity["effective_at"] == eff.isoformat()
        assert DenylistEntry._from_entity(entity).effective_at == eff

    def test_effective_at_defaults_to_added_at(self):
        # When no explicit effective date is given, the serialized entity is
        # effective immediately (falls back to added_at/now).
        added = datetime(2025, 6, 1, tzinfo=timezone.utc)
        entity = DenylistEntry(digest=_DIGEST, added_at=added)._to_entity()
        assert entity["effective_at"] == added.isoformat()

    def test_is_effective(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert DenylistEntry(digest=_DIGEST).is_effective(now) is True
        past = DenylistEntry(digest=_DIGEST, effective_at=now - timedelta(days=1))
        assert past.is_effective(now) is True
        future = DenylistEntry(digest=_DIGEST, effective_at=now + timedelta(days=1))
        assert future.is_effective(now) is False


# ── ImageDenylist against Azurite ─────────────────────────────────────────────


@pytest.fixture
async def store(monkeypatch):
    table_name = "DenylistTest" + uuid.uuid4().hex[:12]
    monkeypatch.setenv("JOBQ_DENYLIST_ACCOUNT", "devstoreaccount1")
    monkeypatch.setenv("JOBQ_DENYLIST_TABLE", table_name)
    monkeypatch.delenv("JOBQ_DENYLIST_DISABLE", raising=False)
    dl = await ImageDenylist.open()
    assert dl is not None
    async with dl:
        yield dl


@skip_without_table
class TestImageDenylistStore:
    async def test_add_get_remove(self, store):
        entry = await store.add(_DIGEST, reason="bad", added_by="me", shutdown_mode="hard")
        assert entry.digest == _DIGEST
        got = await store.get(_DIGEST)
        assert got is not None
        assert got.reason == "bad"
        assert got.added_by == "me"
        assert got.shutdown_mode == "hard"
        assert await store.remove(_DIGEST) is True
        assert await store.get(_DIGEST) is None
        assert await store.remove(_DIGEST) is False

    async def test_is_denied_forms(self, store):
        await store.add(_DIGEST)
        # bare hex, uppercase, and full ref all match
        assert (await store.is_denied(_HEX)) is not None
        assert (await store.is_denied("A" * 64)) is not None
        assert (await store.is_denied(f"reg/repo@{_DIGEST}")) is not None
        # unrelated digest does not
        assert (await store.is_denied(f"sha256:{'b' * 64}")) is None
        # invalid entries are skipped, not raised
        assert (await store.is_denied("garbage", None)) is None

    async def test_is_denied_multiple(self, store):
        arch = f"sha256:{'c' * 64}"
        await store.add(arch)
        # list-digest not denied, arch is → match returned
        entry = await store.is_denied(f"sha256:{'d' * 64}", arch)
        assert entry is not None
        assert entry.digest == arch

    async def test_list_and_denied_digests(self, store):
        d1 = f"sha256:{'1' * 64}"
        d2 = f"sha256:{'2' * 64}"
        await store.add(d1)
        await store.add(d2)
        digests = await store.denied_digests()
        assert {d1, d2} <= digests

    async def test_future_effective_not_enforced(self, store):
        future = _now() + timedelta(days=30)
        await store.add(_DIGEST, effective_at=future)
        # Stored and listed (visible to operators)...
        assert (await store.get(_DIGEST)) is not None
        assert any(e.digest == _DIGEST for e in await store.list_entries())
        # ...but not yet enforced.
        assert (await store.is_denied(_DIGEST)) is None
        assert _DIGEST not in await store.denied_digests()

    async def test_past_effective_enforced(self, store):
        past = _now() - timedelta(days=1)
        await store.add(_DIGEST, effective_at=past)
        assert (await store.is_denied(_DIGEST)) is not None
        assert _DIGEST in await store.denied_digests()

    async def test_invalid_shutdown_mode_rejected(self, store):
        with pytest.raises(ValueError, match="shutdown_mode"):
            await store.add(_DIGEST, shutdown_mode="nope")

    async def test_add_duplicate_without_force_raises(self, store):
        from ai4s.jobq.denylist import DenylistEntryExistsError

        await store.add(_DIGEST, reason="first")
        with pytest.raises(DenylistEntryExistsError):
            await store.add(_DIGEST, reason="second")
        # The original entry is untouched.
        got = await store.get(_DIGEST)
        assert got is not None
        assert got.reason == "first"

    async def test_add_duplicate_with_force_overwrites(self, store):
        await store.add(_DIGEST, reason="first")
        await store.add(_DIGEST, reason="second", force=True)
        got = await store.get(_DIGEST)
        assert got is not None
        assert got.reason == "second"


@skip_without_table
class TestOpenInert:
    async def test_disabled_returns_none(self, monkeypatch):
        monkeypatch.setenv("JOBQ_DENYLIST_ACCOUNT", "devstoreaccount1")
        monkeypatch.setenv("JOBQ_DENYLIST_DISABLE", "1")
        assert await ImageDenylist.open() is None

    async def test_unset_account_uses_default(self, monkeypatch):
        # With no explicit account the denylist is no longer inert: it falls
        # back to the shared org-wide default account.
        monkeypatch.delenv("JOBQ_DENYLIST_ACCOUNT", raising=False)
        monkeypatch.delenv("JOBQ_DENYLIST_DISABLE", raising=False)
        assert denylist_account() == "jobq0central"
        assert denylist_configured() is True
