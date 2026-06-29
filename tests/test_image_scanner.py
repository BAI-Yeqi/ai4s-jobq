# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for :mod:`ai4s.jobq.orchestration.image_scanner`."""

from __future__ import annotations

import logging
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from ai4s.jobq.orchestration.image_scanner import (
    FEDRAMP_SCAN_VERSION_PROPERTY,
    ImageScanError,
    ImageScanner,
    ImageVulnerableError,
)

_LOGGER = "ai4s.jobq.orchestration.image_scanner"
_REGISTRY = "msrmoldyn.azurecr.io"
_REPO = "vasp/vasp-cpu-env"
_DIGEST = "sha256:" + "a" * 64
_DIGEST_URI = f"{_REGISTRY}/{_REPO}@{_DIGEST}"


def _make_scanner(**kwargs) -> ImageScanner:
    kwargs.setdefault("subscription_ids", ["sub-1"])
    return ImageScanner(credential=MagicMock(), **kwargs)


def test_property_key_is_stable() -> None:
    # The alerting app reads this exact dotted key; guard against accidental renames.
    assert FEDRAMP_SCAN_VERSION_PROPERTY == "fedramp.scan-version"


class TestScanSkipsNonAcr:
    def test_non_acr_returns_none_with_warning(self, caplog) -> None:
        s = _make_scanner()
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            out = s.scan("ghcr.io/org/repo:latest")
        assert out is None
        assert "non-ACR" in caplog.text

    def test_non_acr_never_invokes_backend(self) -> None:
        s = _make_scanner()
        with patch.object(ImageScanner, "_run_scan") as run:
            assert s.scan("docker.io/library/python:3.12") is None
        run.assert_not_called()


class TestScanCleanPass:
    def test_returns_scanner_version(self) -> None:
        s = _make_scanner()
        with patch.object(ImageScanner, "_run_scan", return_value="12.13.0") as run:
            assert s.scan(_DIGEST_URI) == "12.13.0"
        run.assert_called_once()

    def test_result_is_cached(self) -> None:
        s = _make_scanner()
        with patch.object(ImageScanner, "_run_scan", return_value="12.13.0") as run:
            s.scan(_DIGEST_URI)
            s.scan(_DIGEST_URI)
        run.assert_called_once()  # second call served from cache

    def test_ttl_zero_rescans(self) -> None:
        s = _make_scanner(ttl_seconds=0)
        with patch.object(ImageScanner, "_run_scan", return_value="12.13.0") as run:
            s.scan(_DIGEST_URI)
            s.scan(_DIGEST_URI)
        assert run.call_count == 2


class TestScanVulnerable:
    def test_raises_image_vulnerable(self) -> None:
        s = _make_scanner()
        err = ImageVulnerableError(_DIGEST_URI, ["CVE-x", "CVE-y"])
        with (
            patch.object(ImageScanner, "_run_scan", side_effect=err),
            pytest.raises(ImageVulnerableError) as ei,
        ):
            s.scan(_DIGEST_URI)
        assert ei.value.findings == ["CVE-x", "CVE-y"]

    def test_reject_verdict_is_cached(self) -> None:
        s = _make_scanner()
        err = ImageVulnerableError(_DIGEST_URI, ["CVE-x"])
        with patch.object(ImageScanner, "_run_scan", side_effect=err) as run:
            with pytest.raises(ImageVulnerableError):
                s.scan(_DIGEST_URI)
            with pytest.raises(ImageVulnerableError):
                s.scan(_DIGEST_URI)
        run.assert_called_once()  # cached, not re-scanned


class TestScanError:
    def test_fail_closed_raises_scan_error(self) -> None:
        s = _make_scanner(fail_open=False)
        with (
            patch.object(ImageScanner, "_run_scan", side_effect=RuntimeError("acr down")),
            pytest.raises(ImageScanError, match="acr down"),
        ):
            s.scan(_DIGEST_URI)

    def test_fail_open_returns_none(self, caplog) -> None:
        s = _make_scanner(fail_open=True)
        with (
            patch.object(ImageScanner, "_run_scan", side_effect=RuntimeError("acr down")),
            caplog.at_level(logging.WARNING, logger=_LOGGER),
        ):
            out = s.scan(_DIGEST_URI)
        assert out is None
        assert "fail-open" in caplog.text


def _fake_fedramp_module(find_vulns: MagicMock) -> types.ModuleType:
    """A stand-in ``fedramp_scanner`` module exposing just what ``_run_scan`` imports."""
    mod = types.ModuleType("fedramp_scanner")

    class Image:
        def __init__(self, registry_name, repository_name, tag, digest):
            self.registry_name = registry_name
            self.repository_name = repository_name
            self.tag = tag
            self.digest = digest

        @classmethod
        def parse(cls, image_str):
            repo_part, digest = image_str.split("@", 1)
            registry, _, repo = repo_part.partition("/")
            return cls(registry, repo, None, digest)

    class ScanMode:
        @classmethod
        def from_str(cls, s):
            return f"{s.upper()}-mode"

    class Severity:
        @classmethod
        def from_str(cls, s):
            return f"{s}-sev"

    mod.Image = Image  # type: ignore[attr-defined]
    mod.ScanMode = ScanMode  # type: ignore[attr-defined]
    mod.Severity = Severity  # type: ignore[attr-defined]
    mod.find_vulns = find_vulns  # type: ignore[attr-defined]
    return mod


class TestRunScanInvokesFedrampScanner:
    """Exercise the real ``_run_scan`` against a stubbed ``fedramp_scanner`` module."""

    def test_builds_find_vulns_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        find_vulns = MagicMock(return_value=([], {}))
        monkeypatch.setitem(sys.modules, "fedramp_scanner", _fake_fedramp_module(find_vulns))
        monkeypatch.setattr(f"{_LOGGER}._scanner_version", lambda: "9.9.9")

        s = _make_scanner(scan_mode="Auto", severity="Critical", expected_job_duration_days=14)
        assert s.scan(_DIGEST_URI) == "9.9.9"

        find_vulns.assert_called_once()
        args, kwargs = find_vulns.call_args
        assert args[0] == ["sub-1"]  # subscription_ids
        assert args[1].digest == _DIGEST  # parsed Image
        assert kwargs["scan_mode"] == "AUTO-mode"
        assert kwargs["severity"] == "Critical-sev"
        assert kwargs["expected_job_duration"] == 14

    def test_findings_raise_vulnerable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        find_vulns = MagicMock(return_value=(["CVE-1"], {}))
        monkeypatch.setitem(sys.modules, "fedramp_scanner", _fake_fedramp_module(find_vulns))

        s = _make_scanner()
        with pytest.raises(ImageVulnerableError):
            s.scan(_DIGEST_URI)
