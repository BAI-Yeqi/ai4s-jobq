# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for the strict workforce image-assessment gate."""

import sys
import time
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from ai4s.jobq.orchestration.image_assessment import ImageAssessmentGate


class _Verdict:
    def __init__(self, status: str = "clean"):
        self.status = status
        self.scanner_version = "1.2.0"

    def raise_for_job(self) -> None:
        if self.status != "clean":
            raise RuntimeError(self.status)


@pytest.mark.parametrize("status", ["vulnerable", "not_found", "skipped"])
def test_scanner_rejection_propagates(status):
    assessor = MagicMock()
    assessor.assess.return_value = _Verdict(status)

    with pytest.raises(RuntimeError, match=status):
        ImageAssessmentGate(assessor).assess("registry/repo@sha256:abc")


def test_shared_assessor_access_is_serialized():
    cache = []
    backend_calls = 0

    def assess(_image):
        nonlocal backend_calls
        if not cache:
            backend_calls += 1
            time.sleep(0.005)
            cache.append(_Verdict())
        return cache[0]

    assessor = MagicMock()
    assessor.assess.side_effect = assess
    gate = ImageAssessmentGate(assessor)
    with ThreadPoolExecutor(max_workers=8) as pool:
        verdicts = list(pool.map(gate.assess, ["registry/repo@sha256:abc"] * 16))

    assert all(verdict.status == "clean" for verdict in verdicts)
    assert backend_calls == 1


def test_missing_optional_dependency_has_install_hint(monkeypatch):
    monkeypatch.setitem(sys.modules, "fedramp_scanner", None)

    with pytest.raises(ModuleNotFoundError, match=r"ai4s-jobq\[scan\]"):
        ImageAssessmentGate.from_scanner()


def test_scanner_factory_forces_cache_only_auto_profile(monkeypatch):
    scanner = types.ModuleType("fedramp_scanner")
    scanner.ImageAssessor = MagicMock()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "fedramp_scanner", scanner)

    ImageAssessmentGate.from_scanner(
        severity="Critical",
        scan_mode="Defender",
        cache_only=False,
    )

    scanner.ImageAssessor.assert_called_once_with(  # type: ignore[attr-defined]
        severity="Critical",
        scan_mode="Auto",
        cache_only=True,
    )


def test_lifecycle_logs_distinguish_clean_rejection_and_error(caplog):
    logger = "ai4s.jobq.orchestration.image_assessment"
    assessor = MagicMock()
    clean = _Verdict()
    assessor.assess.side_effect = [
        clean,
        _Verdict("vulnerable"),
        ConnectionError("registry unavailable"),
    ]
    gate = ImageAssessmentGate(assessor)

    with caplog.at_level("INFO", logger=logger):
        assert gate.assess("registry/repo@sha256:abc") is clean
        with pytest.raises(RuntimeError, match="vulnerable"):
            gate.assess("registry/repo@sha256:abc")
        with pytest.raises(ConnectionError, match="registry unavailable"):
            gate.assess("registry/repo@sha256:abc")

    messages = [record.getMessage() for record in caplog.records]
    assert any("image_assessment_clean" in message for message in messages)
    assert any("image_assessment_rejected" in message for message in messages)
    assert any("image_assessment_error" in message for message in messages)
