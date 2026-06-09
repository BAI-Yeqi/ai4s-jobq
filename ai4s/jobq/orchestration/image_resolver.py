# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Resolve ACR image tags to immutable content digests at hire time.

Mutable tags (even dated ones) can be re-pushed after some workers in a
workforce session have already been hired.  Because AML pulls images on
each node allocation, two workers in the same session can end up running
different image content — poison for reproducibility.

This module looks up the immutable content digest (``sha256:...``) of a
tag via the ``azure-containerregistry`` SDK, caches it under a TTL, and
rewrites the image reference from ``registry/repo:tag`` to
``registry/repo@sha256:...``.

Only Azure Container Registry (``*.azurecr.io``) is supported.  Non-ACR
images are returned unchanged with a warning log.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

LOG = logging.getLogger(__name__)

_DEFAULT_TTL_SECONDS = 3600


@dataclass(frozen=True)
class _CacheEntry:
    digest: str
    resolved_at_monotonic: float


class ImageDigestResolver:
    """Resolve ``registry/repo:tag`` to ``registry/repo@sha256:...``.

    Thread-safe.  A single instance can be shared across all Workforces in
    a MultiRegionWorkforce.

    Args:
        credential: Azure ``TokenCredential`` used to authenticate against
            ACR.  Defaults to ``ChainedTokenCredential(AzureCliCredential(),
            DefaultAzureCredential())`` — matching the operator's interactive
            login on dev boxes while falling back to managed identity in
            production.
        ttl_seconds: How long a cached digest is reused before re-fetching.
        fail_open: When True (default), unresolvable images fall back to
            the original tag-based URI.  Set to False to raise on failure.
    """

    def __init__(
        self,
        credential: TokenCredential | None = None,
        *,
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
        fail_open: bool = True,
    ):
        self._credential = credential or self._default_credential()
        self._ttl_seconds = ttl_seconds
        self._fail_open = fail_open
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _default_credential() -> TokenCredential:
        from azure.identity import (
            AzureCliCredential,
            ChainedTokenCredential,
            DefaultAzureCredential,
        )

        return ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential())

    def resolve(self, image_uri: str) -> str:
        """Rewrite ``image_uri`` to use its content digest.

        Returns the URI unchanged if it is already digest-pinned, if the
        registry is not ACR, or (with ``fail_open=True``) on any error.
        """
        if "@sha256:" in image_uri:
            return image_uri

        registry, repo, tag = _parse_image_uri(image_uri)

        if not registry.endswith(".azurecr.io"):
            LOG.warning(
                "image_digest_skip image=%s reason=non-ACR registry; "
                "only *.azurecr.io is supported for digest pinning",
                image_uri,
            )
            return image_uri

        with self._lock:
            entry = self._cache.get(image_uri)
            now = time.monotonic()
            if entry is not None and (now - entry.resolved_at_monotonic) < self._ttl_seconds:
                return _format_digest_uri(image_uri, entry.digest)

            try:
                digest = self._fetch_digest(registry, repo, tag)
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

            self._cache[image_uri] = _CacheEntry(digest=digest, resolved_at_monotonic=now)
            return _format_digest_uri(image_uri, digest)

    def _fetch_digest(self, registry: str, repo: str, tag: str) -> str:
        from azure.containerregistry import ContainerRegistryClient  # type: ignore[import-untyped]

        endpoint = f"https://{registry}"
        with ContainerRegistryClient(endpoint, self._credential) as client:
            props = client.get_manifest_properties(repo, tag)
        digest: str = props.digest
        if not digest.startswith("sha256:"):
            raise RuntimeError(f"unexpected digest format from ACR: {digest!r}")
        return digest


def _parse_image_uri(image_uri: str) -> tuple[str, str, str]:
    """Split ``registry/repo:tag`` into ``(registry, repo, tag)``.

    Registry is everything before the first ``/``.  Tag is after the last
    ``:`` in the final path segment; defaults to ``latest``.
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
    if "/" in image_uri:
        prefix, last_segment = image_uri.rsplit("/", 1)
        if ":" in last_segment:
            last_segment = last_segment.rsplit(":", 1)[0]
        base = f"{prefix}/{last_segment}"
    else:
        base = image_uri.rsplit(":", 1)[0] if ":" in image_uri else image_uri
    return f"{base}@{digest}"
