# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for :mod:`ai4s.jobq.orchestration.image_resolver`."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from ai4s.jobq.orchestration.image_resolver import (
    ImageDigestResolver,
    _format_digest_uri,
    _parse_image_uri,
)

_REGISTRY = "msrmoldyn.azurecr.io"
_REPO = "materials/dft-calculation"
_TAG = "2026-04-21"
_TAG_URI = f"{_REGISTRY}/{_REPO}:{_TAG}"
_DIGEST = "sha256:" + "a" * 64
_DIGEST_URI = f"{_REGISTRY}/{_REPO}@{_DIGEST}"


# ── pure helpers ──────────────────────────────────────────────────────────────


class TestParseImageUri:
    def test_basic(self):
        assert _parse_image_uri(_TAG_URI) == (_REGISTRY, _REPO, _TAG)

    def test_no_tag_defaults_to_latest(self):
        assert _parse_image_uri(f"{_REGISTRY}/{_REPO}") == (_REGISTRY, _REPO, "latest")

    def test_single_segment_repo(self):
        assert _parse_image_uri("reg.io/foo:v1") == ("reg.io", "foo", "v1")

    def test_missing_registry_raises(self):
        with pytest.raises(ValueError, match="registry hostname"):
            _parse_image_uri("foo:bar")


class TestFormatDigestUri:
    def test_strips_tag_and_appends_digest(self):
        assert _format_digest_uri(_TAG_URI, _DIGEST) == _DIGEST_URI

    def test_no_tag_just_appends(self):
        assert _format_digest_uri(f"{_REGISTRY}/{_REPO}", _DIGEST) == _DIGEST_URI


# ── ImageDigestResolver behaviour ─────────────────────────────────────────────


def _make_resolver(**kwargs) -> ImageDigestResolver:
    """Resolver with a stub credential (resolver._fetch_digest is patched per test)."""
    cred = MagicMock()
    cred.get_token.return_value = MagicMock(token="fake-aad-token")
    return ImageDigestResolver(credential=cred, **kwargs)


class TestResolve:
    def test_already_pinned_returned_unchanged(self):
        r = _make_resolver()
        assert r.resolve(_DIGEST_URI) == _DIGEST_URI

    def test_first_call_fetches_and_caches(self, caplog):
        r = _make_resolver()
        with (
            patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher,
            caplog.at_level(logging.INFO, logger="ai4s.jobq.orchestration.image_resolver"),
        ):
            out = r.resolve(_TAG_URI)
        assert out == _DIGEST_URI
        fetcher.assert_called_once_with(_TAG_URI)
        # Resolution log line was emitted
        assert any("image_digest_resolved" in rec.getMessage() for rec in caplog.records)

    def test_subsequent_call_within_ttl_uses_cache(self):
        r = _make_resolver(ttl_seconds=3600)
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            r.resolve(_TAG_URI)
            r.resolve(_TAG_URI)
            r.resolve(_TAG_URI)
        fetcher.assert_called_once()  # cache hit on calls 2 & 3

    def test_ttl_expiry_triggers_refetch(self):
        r = _make_resolver(ttl_seconds=0)  # always-stale
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            r.resolve(_TAG_URI)
            r.resolve(_TAG_URI)
        assert fetcher.call_count == 2

    def test_digest_change_logs_warning(self, caplog):
        r = _make_resolver(ttl_seconds=0)
        new_digest = "sha256:" + "b" * 64
        with (
            patch.object(r, "_fetch_digest", side_effect=[_DIGEST, new_digest]),
            caplog.at_level(logging.WARNING, logger="ai4s.jobq.orchestration.image_resolver"),
        ):
            r.resolve(_TAG_URI)
            r.resolve(_TAG_URI)
        msgs = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.WARNING]
        assert any("image_digest_changed" in m for m in msgs)
        assert any(_DIGEST in m and new_digest in m for m in msgs)

    def test_fetch_failure_fail_open_returns_tag(self, caplog):
        r = _make_resolver(fail_open=True)
        with (
            patch.object(r, "_fetch_digest", side_effect=RuntimeError("ACR is down")),
            caplog.at_level(logging.WARNING, logger="ai4s.jobq.orchestration.image_resolver"),
        ):
            out = r.resolve(_TAG_URI)
        assert out == _TAG_URI
        assert any("image_digest_resolution_failed" in rec.getMessage() for rec in caplog.records)

    def test_fetch_failure_fail_closed_raises(self):
        r = _make_resolver(fail_open=False)
        with (
            patch.object(r, "_fetch_digest", side_effect=RuntimeError("ACR is down")),
            pytest.raises(RuntimeError, match="ACR is down"),
        ):
            r.resolve(_TAG_URI)


class TestThreadSafety:
    def test_concurrent_resolves_use_lock(self):
        """16 threads all resolving the same image should issue at most a few HTTP calls.

        The lock is held across the fetch, so one thread fetches and the rest
        cache-hit.  This exercises the lock without trying to assert exactly-1
        (which would be fragile if the GIL released between cache-check and
        fetch on a slow machine).
        """
        import threading

        r = _make_resolver(ttl_seconds=3600)
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            barrier = threading.Barrier(16)
            results = []

            def worker():
                barrier.wait()
                results.append(r.resolve(_TAG_URI))

            ts = [threading.Thread(target=worker) for _ in range(16)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
        assert all(x == _DIGEST_URI for x in results)
        # All threads must wait for the lock, so only the first finds an empty
        # cache and fetches; subsequent threads cache-hit.
        assert fetcher.call_count == 1
