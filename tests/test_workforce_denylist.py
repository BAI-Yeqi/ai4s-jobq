# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for ``Workforce`` image-SHA denylist enforcement.

Constructs ``Workforce`` via ``__new__`` (bypassing the Azure-touching
``__init__``) and sets only the attributes the methods under test read,
mirroring ``test_workforce_parallel.py``.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

pytest.importorskip("azure.ai.ml")

from ai4s.jobq.denylist import IMAGE_DIGEST_ARCH_ENV, IMAGE_DIGEST_ENV
from ai4s.jobq.orchestration.image_resolver import ImageDigests
from ai4s.jobq.orchestration.workforce import AmlExperiment, AmlJob, Workforce

_LIST = "sha256:" + "1" * 64
_ARCH = "sha256:" + "2" * 64
_OTHER = "sha256:" + "9" * 64


def _make_job(name: str, status: str = "Running", tags: dict | None = None) -> AmlJob:
    return AmlJob(
        experiment=AmlExperiment(id="e", subscription_id="s", resource_group="rg", workspace="ws"),
        status=status,  # type: ignore[arg-type]
        name=name,
        start_time=None,
        cluster="c",
        error_msg=None,
        metrics={},
        tags=tags or {},
    )


def _bare_workforce(**overrides) -> Workforce:
    wf = Workforce.__new__(Workforce)
    wf._experiment_name = "exp"
    wf._aml_client = MagicMock()
    wf._credential = MagicMock()
    wf._image_resolver = None
    wf._denied_digests = set()
    wf._env_register_lock = threading.Lock()
    wf._registered_env_id_cache = {}
    for k, v in overrides.items():
        setattr(wf, k, v)
    return wf


class TestEnforceDenylist:
    def test_cancels_only_matching(self, monkeypatch):
        jobs = [
            _make_job("denied-list", tags={IMAGE_DIGEST_ENV: _LIST}),
            _make_job("denied-arch", tags={IMAGE_DIGEST_ARCH_ENV: _ARCH}),
            _make_job("clean", tags={IMAGE_DIGEST_ENV: _OTHER}),
            _make_job("untagged"),
        ]
        wf = _bare_workforce()
        monkeypatch.setattr(wf, "list_jobs", lambda **kw: iter(jobs))
        cancelled: list[str] = []
        monkeypatch.setattr(wf, "_cancel_one", lambda w: cancelled.append(w.name))

        selected = wf.enforce_denylist({_LIST, _ARCH}, workers=1)

        assert {j.name for j in selected} == {"denied-list", "denied-arch"}
        assert set(cancelled) == {"denied-list", "denied-arch"}

    def test_empty_denied_is_noop(self, monkeypatch):
        wf = _bare_workforce()
        called = MagicMock()
        monkeypatch.setattr(wf, "list_jobs", called)
        assert wf.enforce_denylist(set()) == []
        called.assert_not_called()

    def test_no_match_no_cancel(self, monkeypatch):
        jobs = [_make_job("clean", tags={IMAGE_DIGEST_ENV: _OTHER})]
        wf = _bare_workforce()
        monkeypatch.setattr(wf, "list_jobs", lambda **kw: iter(jobs))
        cancel = MagicMock()
        monkeypatch.setattr(wf, "_cancel_one", cancel)
        assert wf.enforce_denylist({_LIST}) == []
        cancel.assert_not_called()


class TestPrototypeDeniedDigest:
    def _wf_with_resolver(self, digests: ImageDigests) -> Workforce:
        resolver = MagicMock()
        resolver.resolve_digests.return_value = digests
        job = MagicMock()
        job.environment.image = "reg.azurecr.io/repo:tag"
        return _bare_workforce(_image_resolver=resolver, _job=job)

    def test_denied_when_list_matches(self):
        wf = self._wf_with_resolver(ImageDigests(list_digest=_LIST, arch_digest=_ARCH))
        assert wf.prototype_denied_digest({_LIST}) == _LIST

    def test_denied_when_arch_matches(self):
        wf = self._wf_with_resolver(ImageDigests(list_digest=_LIST, arch_digest=_ARCH))
        assert wf.prototype_denied_digest({_ARCH}) == _ARCH

    def test_not_denied(self):
        wf = self._wf_with_resolver(ImageDigests(list_digest=_LIST, arch_digest=_ARCH))
        assert wf.prototype_denied_digest({_OTHER}) is None

    def test_no_resolver_returns_none(self):
        wf = _bare_workforce(_image_resolver=None)
        assert wf.prototype_denied_digest({_LIST}) is None

    def test_empty_denied_returns_none(self):
        wf = self._wf_with_resolver(ImageDigests(list_digest=_LIST))
        assert wf.prototype_denied_digest(set()) is None


class TestHireRefusal:
    def test_hire_refused_when_denied(self, monkeypatch):
        resolver = MagicMock()
        resolver.resolve_digests.return_value = ImageDigests(list_digest=_LIST)
        job = MagicMock()
        job.environment.image = "reg.azurecr.io/repo:tag"
        wf = _bare_workforce(_image_resolver=resolver, _job=job)
        wf.set_denied_digests({_LIST})
        build = MagicMock()
        monkeypatch.setattr(wf, "_build_worker", build)
        submit = MagicMock()
        monkeypatch.setattr(wf, "_submit_job", submit)
        monkeypatch.setattr(wf, "_progress_iter", lambda it, **kw: it)

        wf.hire(5, progress=False)

        build.assert_not_called()
        submit.assert_not_called()

    def test_hire_proceeds_when_clean(self, monkeypatch):
        resolver = MagicMock()
        resolver.resolve_digests.return_value = ImageDigests(list_digest=_LIST)
        job = MagicMock()
        job.environment.image = "reg.azurecr.io/repo:tag"
        wf = _bare_workforce(_image_resolver=resolver, _job=job)
        wf.set_denied_digests({_OTHER})
        monkeypatch.setattr(wf, "_build_worker", lambda: MagicMock(name="w"))
        submit = MagicMock()
        monkeypatch.setattr(wf, "_submit_job", submit)
        monkeypatch.setattr(wf, "_progress_iter", lambda it, **kw: it)

        wf.hire(3, progress=False)

        assert submit.call_count == 3


class TestInjectImageDigests:
    def test_sets_env_and_tags(self):
        resolver = MagicMock()
        resolver.resolve_digests.return_value = ImageDigests(list_digest=_LIST, arch_digest=_ARCH)
        wf = _bare_workforce(_image_resolver=resolver)
        job = MagicMock()
        job.environment_variables = {}
        job.tags = {}
        wf._inject_image_digests(job, "reg.azurecr.io/repo:tag")
        assert job.environment_variables[IMAGE_DIGEST_ENV] == _LIST
        assert job.environment_variables[IMAGE_DIGEST_ARCH_ENV] == _ARCH
        assert job.tags[IMAGE_DIGEST_ENV] == _LIST
        assert job.tags[IMAGE_DIGEST_ARCH_ENV] == _ARCH

    def test_arch_omitted_when_none(self):
        resolver = MagicMock()
        resolver.resolve_digests.return_value = ImageDigests(list_digest=_LIST, arch_digest=None)
        wf = _bare_workforce(_image_resolver=resolver)
        job = MagicMock()
        job.environment_variables = {}
        job.tags = {}
        wf._inject_image_digests(job, "reg.azurecr.io/repo:tag")
        assert job.environment_variables[IMAGE_DIGEST_ENV] == _LIST
        assert IMAGE_DIGEST_ARCH_ENV not in job.environment_variables
        assert IMAGE_DIGEST_ARCH_ENV not in job.tags
