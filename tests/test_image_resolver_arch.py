# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for arch-digest resolution in :mod:`ai4s.jobq.orchestration.image_resolver`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ai4s.jobq.orchestration.image_resolver import ImageDigestResolver, ImageDigests

_REGISTRY = "reg.azurecr.io"
_URI = f"{_REGISTRY}/repo:tag"
_LIST = "sha256:" + "1" * 64
_ARCH = "sha256:" + "2" * 64


def _resolver(**kw) -> ImageDigestResolver:
    return ImageDigestResolver(credential=MagicMock(), **kw)


class _FakeManifestResult:
    def __init__(self, manifest):
        self.manifest = manifest


def _index_manifest(children):
    return {"manifests": children}


class TestResolveDigests:
    def test_multi_arch_returns_both(self):
        r = _resolver()
        children = [
            {"digest": "sha256:" + "9" * 64, "platform": {"os": "linux", "architecture": "arm64"}},
            {"digest": _ARCH, "platform": {"os": "linux", "architecture": "amd64"}},
        ]
        with (
            patch.object(r, "_fetch_digest", return_value=_LIST),
            patch(
                "azure.containerregistry.ContainerRegistryClient",
            ) as crc,
        ):
            client = crc.return_value.__enter__.return_value
            client.get_manifest.return_value = _FakeManifestResult(_index_manifest(children))
            digests = r.resolve_digests(_URI)
        assert digests == ImageDigests(list_digest=_LIST, arch_digest=_ARCH)
        assert digests.all() == {_LIST, _ARCH}

    def test_single_arch_manifest_no_children(self):
        r = _resolver()
        with (
            patch.object(r, "_fetch_digest", return_value=_LIST),
            patch("azure.containerregistry.ContainerRegistryClient") as crc,
        ):
            client = crc.return_value.__enter__.return_value
            client.get_manifest.return_value = _FakeManifestResult({"config": {}})
            digests = r.resolve_digests(_URI)
        assert digests.list_digest == _LIST
        assert digests.arch_digest is None

    def test_target_platform_absent(self):
        r = _resolver()
        children = [
            {"digest": "sha256:" + "9" * 64, "platform": {"os": "linux", "architecture": "arm64"}},
        ]
        with (
            patch.object(r, "_fetch_digest", return_value=_LIST),
            patch("azure.containerregistry.ContainerRegistryClient") as crc,
        ):
            client = crc.return_value.__enter__.return_value
            client.get_manifest.return_value = _FakeManifestResult(_index_manifest(children))
            digests = r.resolve_digests(_URI)
        assert digests.list_digest == _LIST
        assert digests.arch_digest is None

    def test_already_pinned(self):
        r = _resolver()
        digests = r.resolve_digests(f"{_REGISTRY}/repo@{_LIST}")
        assert digests == ImageDigests(list_digest=_LIST, arch_digest=None)

    def test_non_acr_returns_empty(self):
        r = _resolver()
        assert r.resolve_digests("docker.io/lib/x:1") == ImageDigests()

    def test_fail_open_on_error(self):
        r = _resolver(fail_open=True)
        with patch.object(r, "_fetch_digest", side_effect=RuntimeError("boom")):
            assert r.resolve_digests(_URI) == ImageDigests()

    def test_fail_closed_reraises(self):
        r = _resolver(fail_open=False)
        with patch.object(r, "_fetch_digest", side_effect=RuntimeError("boom")):
            import pytest

            with pytest.raises(RuntimeError):
                r.resolve_digests(_URI)

    def test_result_cached(self):
        r = _resolver()
        with (
            patch.object(r, "_fetch_digest", return_value=_LIST) as fd,
            patch.object(r, "_fetch_arch_digest", return_value=_ARCH) as fa,
        ):
            r.resolve_digests(_URI)
            r.resolve_digests(_URI)
        assert fd.call_count == 1
        assert fa.call_count == 1
