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
        assert _parse_image_uri("reg.azurecr.io/foo:v1") == ("reg.azurecr.io", "foo", "v1")

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
    """Resolver with a stub credential (``_fetch_digest`` is patched per test)."""
    return ImageDigestResolver(credential=MagicMock(), **kwargs)


class TestResolve:
    def test_already_pinned_returned_unchanged(self):
        r = _make_resolver()
        assert r.resolve(_DIGEST_URI) == _DIGEST_URI

    def test_non_acr_returns_unchanged_with_warning(self, caplog):
        r = _make_resolver()
        non_acr = "ghcr.io/org/repo:latest"
        with caplog.at_level(logging.WARNING, logger="ai4s.jobq.orchestration.image_resolver"):
            out = r.resolve(non_acr)
        assert out == non_acr
        assert any("non-ACR" in rec.getMessage() for rec in caplog.records)

    def test_first_call_fetches_and_caches(self, caplog):
        r = _make_resolver()
        with (
            patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher,
            caplog.at_level(logging.INFO, logger="ai4s.jobq.orchestration.image_resolver"),
        ):
            out = r.resolve(_TAG_URI)
        assert out == _DIGEST_URI
        fetcher.assert_called_once_with(_REGISTRY, _REPO, _TAG)
        assert any("image_digest_resolved" in rec.getMessage() for rec in caplog.records)

    def test_subsequent_call_within_ttl_uses_cache(self):
        r = _make_resolver(ttl_seconds=3600)
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            r.resolve(_TAG_URI)
            r.resolve(_TAG_URI)
            r.resolve(_TAG_URI)
        fetcher.assert_called_once()

    def test_ttl_expiry_triggers_refetch(self):
        r = _make_resolver(ttl_seconds=0)
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


class TestFetchDigestUsesSDK:
    """``_fetch_digest`` delegates to ``ContainerRegistryClient``."""

    def test_calls_get_manifest_properties(self):
        r = _make_resolver()
        mock_props = MagicMock()
        mock_props.digest = _DIGEST
        mock_client = MagicMock()
        mock_client.get_manifest_properties.return_value = mock_props
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch(
            "azure.containerregistry.ContainerRegistryClient",
            return_value=mock_client,
        ) as mock_cls:
            digest = r._fetch_digest(_REGISTRY, _REPO, _TAG)

        assert digest == _DIGEST
        mock_cls.assert_called_once_with(f"https://{_REGISTRY}", r._credential)
        mock_client.get_manifest_properties.assert_called_once_with(_REPO, _TAG)


class TestThreadSafety:
    def test_concurrent_resolves_use_lock(self):
        """16 threads all resolving the same image — lock serializes fetches."""
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
        assert fetcher.call_count == 1


# ── Integration with Workforce._build_worker ──────────────────────────────────


class _StubEnvironment:
    """Minimal stand-in for ``azure.ai.ml.entities.Environment``."""

    def __init__(self, image: str | None = None):
        self.image = image


class _StubJob:
    """Minimal stand-in for the worker prototype ``Command``."""

    def __init__(self, environment: object | None = None):
        self.environment = environment
        self.environment_variables: dict[str, str] = {}
        self.experiment_name = ""
        self.name = ""


def _apply_resolver_to_job(
    resolver: ImageDigestResolver | None,
    job: _StubJob,
    register_fn=None,
) -> _StubJob:
    """Mirrors the image-rewriting logic from ``Workforce._build_worker``."""
    import copy as _copy

    env = job.environment
    original_image = getattr(env, "image", None)
    if resolver is not None and original_image:
        resolved = resolver.resolve(original_image)
        if resolved != original_image:
            new_env = _copy.copy(env)
            new_env.image = resolved
            assert register_fn is not None, "register_fn required for resolver rewrite"
            job.environment = register_fn(new_env)
    return job


class TestWorkforceIntegration:
    def test_image_rewritten_on_resolved_environment(self):
        r = _make_resolver()
        registered: list = []

        def register(env):
            registered.append(env)
            return "/fake/arm/id/CliV2AnonymousEnvironment/versions/abc"

        with patch.object(r, "_fetch_digest", return_value=_DIGEST):
            job = _StubJob(environment=_StubEnvironment(image=_TAG_URI))
            _apply_resolver_to_job(r, job, register)
        assert len(registered) == 1
        assert registered[0].image == _DIGEST_URI
        assert job.environment == "/fake/arm/id/CliV2AnonymousEnvironment/versions/abc"

    def test_no_resolver_leaves_image_alone(self):
        job = _StubJob(environment=_StubEnvironment(image=_TAG_URI))
        _apply_resolver_to_job(None, job)  # type: ignore[arg-type]
        assert job.environment.image == _TAG_URI

    def test_string_environment_skipped(self):
        """Registered AML environments are passed as strings — no .image attr."""
        r = _make_resolver()
        job = _StubJob(environment="AzureML-PyTorch-1.10:1")
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            _apply_resolver_to_job(r, job)
        fetcher.assert_not_called()
        assert job.environment == "AzureML-PyTorch-1.10:1"

    def test_already_pinned_image_is_a_noop(self):
        r = _make_resolver()
        job = _StubJob(environment=_StubEnvironment(image=_DIGEST_URI))
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            _apply_resolver_to_job(r, job)
        fetcher.assert_not_called()
        assert job.environment.image == _DIGEST_URI

    def test_environment_is_not_mutated_in_place(self):
        """Prototype's Environment must not be touched — concurrent calls share it."""
        r = _make_resolver()
        original_env = _StubEnvironment(image=_TAG_URI)
        job = _StubJob(environment=original_env)
        registered: list = []

        def register(env):
            registered.append(env)
            return "/fake/arm/id/CliV2AnonymousEnvironment/versions/abc"

        with patch.object(r, "_fetch_digest", return_value=_DIGEST):
            _apply_resolver_to_job(r, job, register)
        assert original_env.image == _TAG_URI
        assert registered[0] is not original_env
        assert registered[0].image == _DIGEST_URI
        assert job.environment == "/fake/arm/id/CliV2AnonymousEnvironment/versions/abc"
