# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Resolve Azure Container Registry image tags to immutable content digests.

Why this exists
---------------
Mutable tags (``:latest``, even most dated tags unless the registry has
content-immutability enabled) can be re-pushed in ACR after a workforce
has hired some workers but before others spin up.  AML / Singularity pull
the image on each node-allocation, so two workers in the same workforce
session can end up running different image content.  That's poison for
reproducibility: a long-running workforce will silently mix builds.

What this module does
---------------------
Looks up the immutable content digest (``sha256:...``) of an ACR tag once
per (image, TTL window), caches it, and rewrites the image reference from
``registry/repo:tag`` to ``registry/repo@sha256:...``.  All workers hired
within the same TTL window pull the same content even if the tag is
re-pushed mid-session.

Usage
-----
Construct one ``ImageDigestResolver`` per scheduler process and share it
across all workforces (it is thread-safe).  ``Workforce`` accepts an
optional resolver and uses it in ``_build_worker`` to rewrite the
prototype's environment image before submission.

::

    resolver = ImageDigestResolver(ttl_seconds=3600)
    wf = Workforce(experiment_name=..., worker_prototype=proto,
                   image_resolver=resolver)

Cache semantics
---------------
* TTL-based: digests are re-fetched after ``ttl_seconds`` (default 1 h).
* Per-process: starting a fresh scheduler always re-resolves from scratch
  (no on-disk cache).  This means each scheduler restart pins to "whatever
  is in ACR right now" and stays on that for the session.
* Mid-session rebuild detection: if a re-fetch returns a digest that
  differs from the previously-cached value for the same tag, the resolver
  emits a ``WARNING`` log naming both digests so operators can spot
  rebuilds during long-running workforces.

Failure mode
------------
By default the resolver is fail-open: if ACR is unreachable, returns a
5xx, or the principal lacks AcrPull, ``resolve()`` falls back to the
original tag-based URI with a ``WARNING`` log.  Image pinning is a
quality improvement, not a correctness invariant — don't break hiring
because ACR had a transient hiccup.  Pass ``fail_open=False`` to require
successful resolution.

Logging is terminal-only via the standard ``ai4s.jobq.image_resolver``
logger (no metrics emission).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import requests
from azure.identity import DefaultAzureCredential

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

LOG = logging.getLogger(__name__)

# Default TTL — match the cadence of typical image rebuilds (weekly to monthly).
# 1 hour means a long-running scheduler's workforce will see at most one
# digest-change boundary per hour, well below the noise floor of normal
# AML hiring activity.
_DEFAULT_TTL_SECONDS = 3600

# How long we wait on individual ACR HTTP calls before falling back.
# ACR's manifest endpoint is normally <100 ms; 10 s is generous headroom.
_ACR_HTTP_TIMEOUT_S = 10

# AAD scope used to obtain the access token we exchange with ACR's
# /oauth2/exchange endpoint.  This is the standard ACR-as-AAD-resource
# pattern; see https://learn.microsoft.com/azure/container-registry/container-registry-authentication
_AAD_SCOPE_FOR_ACR = "https://management.azure.com/.default"

# Manifest media types accepted on the GET — covers Docker v2, OCI, and
# multi-arch index variants.  Without this Accept header ACR returns the
# v1 manifest by default which has a different (deprecated) digest.
_MANIFEST_ACCEPT = (
    "application/vnd.docker.distribution.manifest.v2+json,"
    "application/vnd.oci.image.manifest.v1+json,"
    "application/vnd.docker.distribution.manifest.list.v2+json,"
    "application/vnd.oci.image.index.v1+json"
)


@dataclass(frozen=True)
class _CacheEntry:
    """Cached digest resolution for a single image tag."""

    digest: str
    resolved_at_monotonic: float


class ImageDigestResolver:
    """Resolve ACR ``registry/repo:tag`` references to ``registry/repo@sha256:...``.

    Thread-safe: a single instance can be shared across all Workforces in a
    MultiRegionWorkforce.  Use one resolver per scheduler process.

    Args:
        ttl_seconds: How long a cached digest is reused before re-fetching.
            Defaults to 1 h.  Set to 0 to disable caching (re-fetch every
            call; useful for tests, costly in production).
        fail_open: When True (default), unresolvable images fall back to
            the original tag-based URI with a WARNING.  Set to False to
            require successful resolution (raises the underlying error).
        credential: Optional AAD credential.  Defaults to
            :class:`DefaultAzureCredential`.  Override in tests or when a
            specific managed-identity client-id is required.
    """

    def __init__(
        self,
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
        fail_open: bool = True,
        credential: TokenCredential | None = None,
    ):
        self._ttl_seconds = ttl_seconds
        self._fail_open = fail_open
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()
        self._credential: TokenCredential = credential or DefaultAzureCredential()

    def resolve(self, image_uri: str) -> str:
        """Return ``image_uri`` rewritten with its content digest.

        If ``image_uri`` already contains ``@sha256:``, it's returned
        unchanged.  Otherwise the digest is fetched (or read from cache),
        and a digest-pinned URI is returned.  On any error and with
        ``fail_open=True``, the original ``image_uri`` is returned and a
        WARNING is logged.

        Examples:
            ``"reg.azurecr.io/foo/bar:2026-04-21"`` →
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
                # ACR rebuild during a live scheduler session — ops-relevant.
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
        """HTTP-fetch the manifest digest for an ACR ``registry/repo:tag``."""
        registry, repo, tag = _parse_image_uri(image_uri)
        token = self._get_acr_repo_token(registry, repo)
        url = f"https://{registry}/v2/{repo}/manifests/{tag}"
        resp = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": _MANIFEST_ACCEPT,
            },
            timeout=_ACR_HTTP_TIMEOUT_S,
        )
        resp.raise_for_status()
        digest: str | None = resp.headers.get("Docker-Content-Digest")
        if not digest or not digest.startswith("sha256:"):
            raise RuntimeError(
                f"manifest GET returned no/invalid Docker-Content-Digest header "
                f"(got: {digest!r}) for {image_uri}"
            )
        return digest

    def _get_acr_repo_token(self, registry: str, repo: str) -> str:
        """Exchange an AAD access token for an ACR repo-pull access token.

        Two-step OAuth dance per ACR's docs:
          1. POST /oauth2/exchange with the AAD token -> ACR refresh token
          2. POST /oauth2/token with the refresh token + repo scope ->
             ACR access token

        The principal must have AcrPull (or stronger) on the registry.
        """
        aad_token = self._credential.get_token(_AAD_SCOPE_FOR_ACR).token

        exchange_resp = requests.post(
            f"https://{registry}/oauth2/exchange",
            data={
                "grant_type": "access_token",
                "service": registry,
                "access_token": aad_token,
            },
            timeout=_ACR_HTTP_TIMEOUT_S,
        )
        exchange_resp.raise_for_status()
        refresh_token = exchange_resp.json()["refresh_token"]

        token_resp = requests.post(
            f"https://{registry}/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "service": registry,
                "scope": f"repository:{repo}:pull",
                "refresh_token": refresh_token,
            },
            timeout=_ACR_HTTP_TIMEOUT_S,
        )
        token_resp.raise_for_status()
        access_token: str = token_resp.json()["access_token"]
        return access_token


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
