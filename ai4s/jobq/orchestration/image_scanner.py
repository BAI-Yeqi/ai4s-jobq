# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Run the FedRAMP/vulnerability scan on a worker image at hire time.

The scanner is the standalone ``fedramp-scanner`` package.  It is an
*optional* dependency: importing this module never requires
it, and a :class:`~ai4s.jobq.orchestration.workforce.Workforce` only constructs
an :class:`ImageScanner` when image scanning is explicitly enabled.  Running the
scan at submission time serves two purposes:

1. **Reject gate** -- if the image about to run has blocking vulnerabilities the
   scan raises :class:`ImageVulnerableError` and the worker is not submitted.
2. **Provenance stamp** -- on a clean scan :meth:`ImageScanner.scan` returns the
   ``fedramp-scanner`` version, which the caller stamps onto the AML job as the
   ``fedramp.scan-version`` property.  A downstream alerting app flags any job
   submitted *without* that property (i.e. one whose image was never scanned).

Only Azure Container Registry (``*.azurecr.io``) images are scanned; images on
other registries are skipped with a warning (:meth:`ImageScanner.scan` returns
``None``).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from azure.core.credentials import TokenCredential

LOG = logging.getLogger(__name__)

# AML job property key read by the ingestion side of the alerting app
# (``Ai4sRunInfo_CL.FedrampScanVersion``).  Mirrors how ``amlt`` stamps
# ``amlt.version``; keep the dotted key in sync with the alerting app.
FEDRAMP_SCAN_VERSION_PROPERTY = "fedramp.scan-version"

_DEFAULT_TTL_SECONDS = 3600


class ImageScanError(Exception):
    """The vulnerability scan could not be completed (fail-closed default)."""


class ImageVulnerableError(Exception):
    """The image has blocking vulnerabilities; the worker must not be submitted."""

    def __init__(self, image_uri: str, findings: list):
        self.image_uri = image_uri
        self.findings = findings
        super().__init__(
            f"image {image_uri} has {len(findings)} blocking vulnerability finding(s); "
            "refusing to submit worker"
        )


def _scanner_version() -> str:
    """Installed ``fedramp-scanner`` version, or ``'unknown'`` if unavailable."""
    try:
        return version("fedramp-scanner")
    except PackageNotFoundError:  # pragma: no cover - only when run from source tree
        return "unknown"


@dataclass(frozen=True)
class _CacheEntry:
    version: str | None
    error: Exception | None
    resolved_at_monotonic: float


class ImageScanner:
    """Scan worker images for FedRAMP vulnerabilities and return the scanner version.

    Thread-safe.  Share a single instance across all workforces in a
    :class:`~ai4s.jobq.orchestration.multiregion_workforce.MultiRegionWorkforce`
    so each distinct image is scanned once and the verdict cached -- the hire
    path calls :meth:`scan` once per worker, and every worker in a fleet shares
    the same image.

    Args:
        subscription_ids: Subscriptions that own the container registry.  Used by
            the Defender / cached-Trivy backends to look up scan records.
        credential: Azure ``TokenCredential`` for the scanner.  When omitted the
            scanner falls back to its own ``default_credential``.
        scan_mode: ``fedramp-scanner`` scan mode (default ``"Auto"``: reuse a
            cached Trivy artifact on the image digest, else fall back to
            Defender).  See ``fedramp_scanner.ScanMode``.
        severity: Minimum severity that blocks submission (default ``"Critical"``).
        expected_job_duration_days: Vulnerabilities whose FedRAMP remediation
            deadline falls within this many days also block (default 28).
        ttl_seconds: How long a scan verdict is cached before re-scanning.
        fail_open: When True, a scan that cannot be completed (scanner missing,
            registry unreachable, no scan record) is treated as a pass with no
            stamp instead of raising.  Defaults to False (fail-closed).
    """

    def __init__(
        self,
        subscription_ids: list[str],
        credential: TokenCredential | None = None,
        *,
        scan_mode: str = "Auto",
        severity: str = "Critical",
        expected_job_duration_days: int = 28,
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
        fail_open: bool = False,
    ):
        self._subscription_ids = list(subscription_ids)
        self._credential = credential
        self._scan_mode = scan_mode
        self._severity = severity
        self._expected_job_duration_days = expected_job_duration_days
        self._ttl_seconds = ttl_seconds
        self._fail_open = fail_open
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()

    def scan(self, image_uri: str, *, created_on: datetime | None = None) -> str | None:
        """Scan ``image_uri`` and return the ``fedramp-scanner`` version on a clean pass.

        Returns ``None`` for non-ACR images (skipped), or -- when ``fail_open``
        is set -- for scans that could not be completed.  Raises
        :class:`ImageVulnerableError` if the image has blocking vulnerabilities,
        and :class:`ImageScanError` on an unrecoverable scan error when
        ``fail_open`` is False.

        Results are cached per ``image_uri`` for ``ttl_seconds`` (including the
        reject verdict) so the rest of a hire burst fails fast without
        re-scanning the same image.
        """
        registry = image_uri.split("/", 1)[0]
        if not registry.endswith(".azurecr.io"):
            LOG.warning(
                "image_scan_skip image=%s reason=non-ACR registry; only *.azurecr.io is scanned",
                image_uri,
            )
            return None

        with self._lock:
            entry = self._cache.get(image_uri)
            now = time.monotonic()
            if entry is not None and (now - entry.resolved_at_monotonic) < self._ttl_seconds:
                if entry.error is not None:
                    raise entry.error
                return entry.version

            try:
                scan_version = self._run_scan(image_uri, created_on)
            except ImageVulnerableError as exc:
                self._cache[image_uri] = _CacheEntry(None, exc, now)
                LOG.error(
                    "image_scan_vulnerable image=%s findings=%d; refusing submission",
                    image_uri,
                    len(exc.findings),
                )
                raise
            except Exception as exc:
                if self._fail_open:
                    LOG.warning(
                        "image_scan_failed image=%s error=%s; fail-open, submitting without stamp",
                        image_uri,
                        exc,
                    )
                    self._cache[image_uri] = _CacheEntry(None, None, now)
                    return None
                wrapped = ImageScanError(f"vulnerability scan failed for {image_uri}: {exc}")
                self._cache[image_uri] = _CacheEntry(None, wrapped, now)
                LOG.error("image_scan_failed image=%s error=%s; fail-closed", image_uri, exc)
                raise wrapped from exc

            self._cache[image_uri] = _CacheEntry(scan_version, None, now)
            LOG.info("image_scan_clean image=%s fedramp_scan_version=%s", image_uri, scan_version)
            return scan_version

    def _run_scan(self, image_uri: str, created_on: datetime | None) -> str:
        """Invoke ``fedramp_scanner.find_vulns``; raise on findings, else return the version."""
        from fedramp_scanner import Image, ScanMode, Severity, find_vulns

        image = Image.parse(image_uri)
        vulns, _not_a_problem = find_vulns(
            self._subscription_ids,
            image,
            severity=Severity.from_str(self._severity),
            credential=self._credential,
            expected_job_duration=self._expected_job_duration_days,
            created_on=created_on,
            scan_mode=ScanMode.from_str(self._scan_mode),
        )
        if vulns:
            raise ImageVulnerableError(image_uri, vulns)
        return _scanner_version()
