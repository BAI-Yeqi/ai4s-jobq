# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Resolve OCI / Docker registry image tags to immutable content digests.

Why this exists
---------------
Mutable tags (``:latest`` and even most dated tags unless the registry has
content-immutability enabled) can be re-pushed in the registry after a
workforce has hired some workers but before others spin up.  AML / Singularity
/ Kubernetes / etc. pull the image on each node-allocation, so two workers in
the same workforce session can end up running different image content.  That
is poison for reproducibility: a long-running scheduler will silently mix
builds.

What this module does
---------------------
Looks up the immutable content digest (``sha256:...``) of a registry tag once
per (image, TTL window), caches it, and rewrites the image reference from
``registry/repo:tag`` to ``registry/repo@sha256:...``.  All workers hired
within the same TTL window pull the same content even if the tag is
re-pushed mid-session.

Pluggable registry auth
-----------------------
The resolver is registry-agnostic by construction: the manifest endpoint
(``GET /v2/{repo}/manifests/{tag}``) is OCI-standard.  Only token
acquisition differs across registries, so authentication is delegated to a
:class:`RegistryAuth` strategy:

* :class:`AcrAadAuth` — Azure Container Registry via Azure AD identity
  (the default; matches the typical AML / Singularity setup).
* :class:`AnonymousAuth` — no authentication; works for public registries
  / public repositories.
* :class:`BearerTokenAuth` — pre-acquired bearer token; useful for tests
  or for callers that obtain tokens out-of-band.

Adding support for Docker Hub / GHCR / ECR / GCR is a matter of adding a
small ``RegistryAuth`` implementation; nothing else in the resolver
changes.

Usage
-----
::

    resolver = ImageDigestResolver(ttl_seconds=3600)              # ACR-AAD default
    resolver = ImageDigestResolver(auth=AnonymousAuth())          # public registry
    wf = Workforce(experiment_name=..., worker_prototype=proto,
                   image_resolver=resolver)

Cache semantics
---------------
* TTL-based: digests are re-fetched after ``ttl_seconds`` (default 1 h).
* Per-process: starting a fresh scheduler always re-resolves from scratch
  (no on-disk cache).  This means each scheduler restart pins to "whatever
  is in the registry right now" and stays on that for the session.
* Mid-session rebuild detection: if a re-fetch returns a digest that
  differs from the previously-cached value for the same tag, the resolver
  emits a ``WARNING`` log naming both digests so operators can spot
  rebuilds during long-running workforces.

Failure mode
------------
By default the resolver is fail-open: if the registry is unreachable,
returns a 5xx, or auth fails, ``resolve()`` falls back to the original
tag-based URI with a ``WARNING`` log.  Image pinning is a quality
improvement, not a correctness invariant — don't break hiring because the
registry had a transient hiccup.  Pass ``fail_open=False`` to require
successful resolution.

Logging is terminal-only via the standard
``ai4s.jobq.orchestration.image_resolver`` logger (no metrics emission).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import requests

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

LOG = logging.getLogger(__name__)

# Default TTL — match the cadence of typical image rebuilds (weekly to monthly).
# 1 hour means a long-running scheduler's workforce will see at most one
# digest-change boundary per hour, well below the noise floor of normal
# AML hiring activity.
_DEFAULT_TTL_SECONDS = 3600

# How long we wait on individual registry HTTP calls before falling back.
# The manifest endpoint is normally <100 ms; 10 s is generous headroom.
_HTTP_TIMEOUT_S = 10

# Manifest media types accepted on the GET — covers Docker v2, OCI, and
# multi-arch index variants.  Without this Accept header registries return
# the v1 manifest by default which has a different (deprecated) digest.
_MANIFEST_ACCEPT = (
    "application/vnd.docker.distribution.manifest.v2+json,"
    "application/vnd.oci.image.manifest.v1+json,"
    "application/vnd.docker.distribution.manifest.list.v2+json,"
    "application/vnd.oci.image.index.v1+json"
)

# AAD scope used by AcrAadAuth.  Per ACR docs, the AAD-to-ACR exchange
# accepts any AAD token whose audience the registry trusts; the ARM
# management scope is the conventional choice.
_AAD_SCOPE_FOR_ACR = "https://management.azure.com/.default"


# ── Pluggable auth ───────────────────────────────────────────────────────────


class RegistryAuth(Protocol):
    """Strategy for obtaining a Bearer token to pull manifests from a registry.

    Implementations should be cheap on the hot path (cache internally if
    needed) and thread-safe (the resolver may call from multiple threads).
    Return ``None`` to skip the ``Authorization`` header entirely (anonymous
    access for public registries / public repos).
    """

    def get_pull_token(self, registry: str, repo: str) -> str | None:
        """Return a Bearer token for ``repo`` on ``registry``, or ``None``."""
        ...


class AnonymousAuth:
    """No authentication.  Works for public registries / public repos."""

    def get_pull_token(self, registry: str, repo: str) -> str | None:
        return None


class BearerTokenAuth:
    """Use a caller-supplied bearer token verbatim.

    Useful for tests, for callers that obtain tokens out-of-band, or for
    registries where token-acquisition logic doesn't fit a simple Protocol
    method (the caller can refresh tokens on its own schedule and feed
    them in).
    """

    def __init__(self, token: str):
        self._token = token

    def get_pull_token(self, registry: str, repo: str) -> str:
        return self._token


class AcrAadAuth:
    """Azure Container Registry auth via Azure AD identity.

    Uses ACR's standard two-step OAuth dance:

      1. Acquire an AAD access token (any scope the registry trusts; we
         use the ARM management scope by convention).
      2. Exchange it at ``https://{registry}/oauth2/exchange`` for an ACR
         refresh token.
      3. Exchange the refresh token at ``https://{registry}/oauth2/token``
         for a repo-scoped pull access token.

    The principal must have AcrPull (or stronger) on the registry.

    Token responses are not cached here — the outer
    :class:`ImageDigestResolver` cache typically prevents re-acquisition.
    For very high call rates or registries with low quota, wrap or
    subclass to add caching.
    """

    def __init__(self, credential: TokenCredential | None = None):
        # Defer the import so non-ACR users don't pay the azure-identity
        # import cost (or even need azure-identity installed).
        if credential is None:
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential()
        self._credential: TokenCredential = credential

    def get_pull_token(self, registry: str, repo: str) -> str:
        aad_token = self._credential.get_token(_AAD_SCOPE_FOR_ACR).token

        exchange_resp = requests.post(
            f"https://{registry}/oauth2/exchange",
            data={
                "grant_type": "access_token",
                "service": registry,
                "access_token": aad_token,
            },
            timeout=_HTTP_TIMEOUT_S,
        )
        exchange_resp.raise_for_status()
        refresh_token: str = exchange_resp.json()["refresh_token"]

        token_resp = requests.post(
            f"https://{registry}/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "service": registry,
                "scope": f"repository:{repo}:pull",
                "refresh_token": refresh_token,
            },
            timeout=_HTTP_TIMEOUT_S,
        )
        token_resp.raise_for_status()
        access_token: str = token_resp.json()["access_token"]
        return access_token


# ── Resolver ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _CacheEntry:
    """Cached digest resolution for a single image tag."""

    digest: str
    resolved_at_monotonic: float


class ImageDigestResolver:
    """Resolve ``registry/repo:tag`` references to ``registry/repo@sha256:...``.

    Thread-safe: a single instance can be shared across all Workforces in a
    MultiRegionWorkforce.  Use one resolver per scheduler process.

    Args:
        ttl_seconds: How long a cached digest is reused before re-fetching.
            Defaults to 1 h.  Set to 0 to disable caching (re-fetch every
            call; useful for tests, costly in production).
        fail_open: When True (default), unresolvable images fall back to
            the original tag-based URI with a WARNING.  Set to False to
            require successful resolution (raises the underlying error).
        auth: :class:`RegistryAuth` strategy.  Defaults to
            :class:`AcrAadAuth` since the typical user is on AML + ACR.
            Pass :class:`AnonymousAuth` for public registries, or a
            custom implementation for Docker Hub / GHCR / etc.
    """

    def __init__(
        self,
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
        fail_open: bool = True,
        auth: RegistryAuth | None = None,
    ):
        self._ttl_seconds = ttl_seconds
        self._fail_open = fail_open
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()
        self._auth: RegistryAuth = auth or AcrAadAuth()

    def resolve(self, image_uri: str) -> str:
        """Return ``image_uri`` rewritten with its content digest.

        If ``image_uri`` already contains ``@sha256:``, it's returned
        unchanged.  Otherwise the digest is fetched (or read from cache),
        and a digest-pinned URI is returned.  On any error and with
        ``fail_open=True``, the original ``image_uri`` is returned and a
        WARNING is logged.

        Examples:
            ``"reg.azurecr.io/foo/bar:2026-04-21"`` ->
            ``"reg.azurecr.io/foo/bar@sha256:abc123..."``.
        """
        if "@sha256:" in image_uri:
            return image_uri  # already digest-pinned

        with self._lock:
            entry = self._cache.get(image_uri)
            now = time.monotonic()
            if entry is not None and (now - entry.resolved_at_monotonic) < self._ttl_seconds:
                return _format_digest_uri(image_uri, entry.digest)

            try:
                digest = self._fetch_digest(image_uri)
            except Exception as exc:
                LOG.warning(
                    "image_digest_resolution_failed image=%s error=%s; "
                    "falling back to tag-based URI%s",
                    image_uri,
                    exc,
                    "" if self._fail_open else " (re-raising)",
                )
                if self._fail_open:
                    return image_uri
                raise

            previous_digest = entry.digest if entry is not None else None
            if previous_digest is not None and previous_digest != digest:
                # Registry rebuild during a live scheduler session — ops-relevant.
                LOG.warning(
                    "image_digest_changed image=%s old_digest=%s new_digest=%s "
                    "(image was re-pushed during scheduler session)",
                    image_uri,
                    previous_digest,
                    digest,
                )
            else:
                LOG.info(
                    "image_digest_resolved image=%s digest=%s ttl_s=%d",
                    image_uri,
                    digest,
                    self._ttl_seconds,
                )

            self._cache[image_uri] = _CacheEntry(
                digest=digest,
                resolved_at_monotonic=now,
            )
            return _format_digest_uri(image_uri, digest)

    def _fetch_digest(self, image_uri: str) -> str:
        """HTTP-fetch the manifest digest for a ``registry/repo:tag``."""
        registry, repo, tag = _parse_image_uri(image_uri)
        token = self._auth.get_pull_token(registry, repo)

        headers = {"Accept": _MANIFEST_ACCEPT}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"

        url = f"https://{registry}/v2/{repo}/manifests/{tag}"
        resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT_S)
        resp.raise_for_status()
        digest: str | None = resp.headers.get("Docker-Content-Digest")
        if not digest or not digest.startswith("sha256:"):
            raise RuntimeError(
                f"manifest GET returned no/invalid Docker-Content-Digest header "
                f"(got: {digest!r}) for {image_uri}"
            )
        return digest


def _parse_image_uri(image_uri: str) -> tuple[str, str, str]:
    """Split ``registry/repo:tag`` into ``(registry, repo, tag)``.

    The registry hostname is everything up to the first ``/``.  The tag
    is everything after the final ``:`` *in the last path segment*; if
    no ``:`` is present in the last segment, defaults to ``latest``.
    """
    if "/" not in image_uri:
        raise ValueError(f"image URI must include a registry hostname: {image_uri!r}")
    registry, rest = image_uri.split("/", 1)
    if "/" in rest:
        repo_prefix, last_segment = rest.rsplit("/", 1)
        if ":" in last_segment:
            last_repo, tag = last_segment.rsplit(":", 1)
            repo = f"{repo_prefix}/{last_repo}"
        else:
            repo = rest
            tag = "latest"
    else:
        if ":" in rest:
            repo, tag = rest.rsplit(":", 1)
        else:
            repo, tag = rest, "latest"
    return registry, repo, tag


def _format_digest_uri(image_uri: str, digest: str) -> str:
    """Rewrite ``registry/repo:tag`` to ``registry/repo@sha256:...``."""
    # Strip the tag (everything after the last ':') from the last path segment.
    if "/" in image_uri:
        prefix, last_segment = image_uri.rsplit("/", 1)
        if ":" in last_segment:
            last_segment = last_segment.rsplit(":", 1)[0]
        base = f"{prefix}/{last_segment}"
    else:
        base = image_uri.rsplit(":", 1)[0] if ":" in image_uri else image_uri
    return f"{base}@{digest}"
