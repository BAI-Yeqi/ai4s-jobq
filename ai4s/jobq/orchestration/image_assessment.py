# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Strict, synchronized image assessment for workforce submissions."""

from __future__ import annotations

import importlib
import logging
import threading
from typing import Any, Protocol

LOG = logging.getLogger(__name__)


class _ScanVerdict(Protocol):
    scanner_version: str
    status: object

    def raise_for_job(self) -> None: ...


class _ImageAssessor(Protocol):
    def assess(self, image: str) -> _ScanVerdict: ...


class ImageAssessmentGate:
    """Serialize shared assessor access and allow only clean images."""

    def __init__(self, assessor: _ImageAssessor):
        self._assessor = assessor
        self._lock = threading.Lock()

    @classmethod
    def from_scanner(cls, **assessor_kwargs: Any) -> ImageAssessmentGate:
        """Build a gate from the optional FedRAMP scanner dependency."""
        try:
            scanner = importlib.import_module("fedramp_scanner")
        except ModuleNotFoundError as exc:
            if exc.name != "fedramp_scanner":
                raise
            message = "Install ai4s-jobq[scan] to enable worker image assessment."
            raise ModuleNotFoundError(message) from exc
        return cls(scanner.ImageAssessor.from_artifacts(**assessor_kwargs))

    def assess(self, image: str) -> _ScanVerdict:
        """Return a clean verdict or propagate the scanner's rejection/error."""
        LOG.info("image_assessment_started image=%s", image)
        try:
            with self._lock:
                verdict = self._assessor.assess(image)
        except Exception:
            LOG.exception("image_assessment_error image=%s", image)
            raise

        try:
            verdict.raise_for_job()
        except Exception:
            LOG.warning(
                "image_assessment_rejected image=%s status=%s",
                image,
                getattr(verdict.status, "value", verdict.status),
            )
            raise

        LOG.info(
            "image_assessment_clean image=%s scanner_version=%s",
            image,
            verdict.scanner_version,
        )
        return verdict
