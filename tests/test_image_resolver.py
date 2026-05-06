# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for :mod:`ai4s.jobq.orchestration.image_resolver`."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from ai4s.jobq.orchestration.image_resolver import (
    AnonymousAuth,
    BearerTokenAuth,
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
    """Resolver with a stub bearer auth (resolver._fetch_digest is patched per test)."""
    return ImageDigestResolver(auth=BearerTokenAuth("fake-token"), **kwargs)


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


# ── Auth backends ─────────────────────────────────────────────────────────────


class TestAuthBackends:
    def test_anonymous_returns_none(self):
        assert AnonymousAuth().get_pull_token(_REGISTRY, _REPO) is None

    def test_bearer_returns_supplied_token(self):
        assert BearerTokenAuth("hunter2").get_pull_token(_REGISTRY, _REPO) == "hunter2"


class TestFetchDigestUsesAuth:
    """``_fetch_digest`` calls auth.get_pull_token and threads the result through."""

    def _make_response(self, digest: str) -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.headers = {"Docker-Content-Digest": digest}
        resp.raise_for_status = MagicMock()
        return resp

    def test_authenticated_call_sets_authorization_header(self):
        auth = MagicMock(spec=["get_pull_token"])
        auth.get_pull_token.return_value = "tok-abc"
        r = ImageDigestResolver(auth=auth)
        with patch.object(requests, "get", return_value=self._make_response(_DIGEST)) as mget:
            digest = r._fetch_digest(_TAG_URI)
        assert digest == _DIGEST
        auth.get_pull_token.assert_called_once_with(_REGISTRY, _REPO)
        _, kwargs = mget.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer tok-abc"

    def test_anonymous_call_omits_authorization_header(self):
        r = ImageDigestResolver(auth=AnonymousAuth())
        with patch.object(requests, "get", return_value=self._make_response(_DIGEST)) as mget:
            r._fetch_digest(_TAG_URI)
        _, kwargs = mget.call_args
        assert "Authorization" not in kwargs["headers"]


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


# ── Integration with Workforce._build_worker ──────────────────────────────────


class _StubEnvironment:
    """Minimal stand-in for ``azure.ai.ml.entities.Environment``.

    ``copy.copy`` works on this without hitting the real Environment's
    ``InputsAttrDict``-related copy issues.
    """

    def __init__(self, image: str | None = None):
        self.image = image


class _StubJob:
    """Minimal stand-in for the ``azure.ai.ml.entities.Command`` prototype.

    Only carries the attributes ``Workforce._build_worker`` mutates.
    """

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
    """Run the same image-rewriting + env-registration logic as
    ``Workforce._build_worker``.

    Mirrors the production path without spinning up a full ``Workforce``
    (which requires an ``MLClient`` and a credential at construction).
    If the production logic changes, this helper must change too —
    keeping the test honest.

    The Workforce, after rewriting the image, pre-registers the new
    Environment via ``MLClient.environments.create_or_update`` and
    substitutes the returned ARM id (a string) for ``job.environment``
    — this avoids the ``ResourceExistsError`` race where every
    concurrent job submission re-registers the same content-hash
    anonymous environment. Here that registration is represented by
    ``register_fn(env) -> str``; tests that exercise the rewrite path
    must pass one.
    """
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
        # The Environment passed to the SDK's pre-registration step carries
        # the digest-pinned image; the job ends up referencing the ARM id
        # by string so the SDK skips its own (race-prone) re-registration.
        assert len(registered) == 1
        assert registered[0].image == _DIGEST_URI
        assert job.environment == "/fake/arm/id/CliV2AnonymousEnvironment/versions/abc"

    def test_no_resolver_leaves_image_alone(self):
        job = _StubJob(environment=_StubEnvironment(image=_TAG_URI))
        _apply_resolver_to_job(None, job)  # type: ignore[arg-type]
        assert job.environment.image == _TAG_URI

    def test_string_environment_skipped(self):
        """Registered AML environments are passed as strings (no .image attr)."""
        r = _make_resolver()
        job = _StubJob(environment="AzureML-PyTorch-1.10:1")
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            _apply_resolver_to_job(r, job)
        # Resolver was never called because there's no .image attribute
        fetcher.assert_not_called()
        assert job.environment == "AzureML-PyTorch-1.10:1"

    def test_already_pinned_image_is_a_noop(self):
        r = _make_resolver()
        job = _StubJob(environment=_StubEnvironment(image=_DIGEST_URI))
        with patch.object(r, "_fetch_digest", return_value=_DIGEST) as fetcher:
            _apply_resolver_to_job(r, job)
        # Resolver returns input unchanged; no HTTP fetch
        fetcher.assert_not_called()
        assert job.environment.image == _DIGEST_URI

    def test_environment_is_not_mutated_in_place(self):
        """Prototype's Environment must not be touched — concurrent _build_worker
        calls share the prototype and would race on a shared mutation."""
        r = _make_resolver()
        original_env = _StubEnvironment(image=_TAG_URI)
        job = _StubJob(environment=original_env)
        registered: list = []

        def register(env):
            registered.append(env)
            return "/fake/arm/id/CliV2AnonymousEnvironment/versions/abc"

        with patch.object(r, "_fetch_digest", return_value=_DIGEST):
            _apply_resolver_to_job(r, job, register)
        # Prototype's env keeps its tag; the rewritten image lives on a
        # fresh Environment that is then registered, with the job ending
        # up holding only its ARM id string.
        assert original_env.image == _TAG_URI
        assert registered[0] is not original_env
        assert registered[0].image == _DIGEST_URI
        assert job.environment == "/fake/arm/id/CliV2AnonymousEnvironment/versions/abc"
