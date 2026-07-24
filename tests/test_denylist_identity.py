# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for caller-identity derivation used by ``denylist add --added-by``."""

from __future__ import annotations

import base64
import json

import pytest

from ai4s.jobq import auth
from ai4s.jobq.auth import _decode_jwt_claims, caller_identity
from ai4s.jobq.denylist import account_uses_aad


def _fake_jwt(claims: dict) -> str:
    def _b64(obj: dict) -> str:
        raw = json.dumps(obj).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    return f"{_b64({'alg': 'none'})}.{_b64(claims)}.sig"


class _FakeToken:
    def __init__(self, token: str) -> None:
        self.token = token


class _FakeCredential:
    def __init__(self, claims: dict | None = None, *, raises: bool = False) -> None:
        self._claims = claims or {}
        self._raises = raises

    async def get_token(self, *scopes: str, **kwargs: object) -> _FakeToken:
        if self._raises:
            raise RuntimeError("no credential")
        return _FakeToken(_fake_jwt(self._claims))

    async def __aenter__(self) -> _FakeCredential:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class TestDecodeJwtClaims:
    def test_roundtrip(self):
        assert _decode_jwt_claims(_fake_jwt({"upn": "a@b.com"})) == {"upn": "a@b.com"}

    def test_malformed_returns_empty(self):
        assert _decode_jwt_claims("not-a-jwt") == {}
        assert _decode_jwt_claims("") == {}
        assert _decode_jwt_claims("a.!!!.c") == {}


class TestCallerIdentity:
    async def test_prefers_upn(self, monkeypatch):
        monkeypatch.setattr(
            auth,
            "get_token_credential",
            lambda: _FakeCredential({"upn": "alice@example.com", "oid": "oid-1"}),
        )
        assert await caller_identity() == "alice@example.com"

    async def test_preferred_username_fallback(self, monkeypatch):
        monkeypatch.setattr(
            auth,
            "get_token_credential",
            lambda: _FakeCredential({"preferred_username": "bob@example.com"}),
        )
        assert await caller_identity() == "bob@example.com"

    async def test_oid_when_no_upn(self, monkeypatch):
        monkeypatch.setattr(
            auth, "get_token_credential", lambda: _FakeCredential({"oid": "obj-123"})
        )
        assert await caller_identity() == "obj-123"

    async def test_appid_last_resort(self, monkeypatch):
        monkeypatch.setattr(
            auth, "get_token_credential", lambda: _FakeCredential({"appid": "app-9"})
        )
        assert await caller_identity() == "app-9"

    async def test_none_when_no_claims(self, monkeypatch):
        monkeypatch.setattr(auth, "get_token_credential", lambda: _FakeCredential({}))
        assert await caller_identity() is None

    async def test_none_on_credential_error(self, monkeypatch):
        monkeypatch.setattr(auth, "get_token_credential", lambda: _FakeCredential(raises=True))
        assert await caller_identity() is None


class TestAccountUsesAad:
    @pytest.mark.parametrize(
        ("account", "expected"),
        [
            ("myacct", True),
            ("devstoreaccount1", False),
            ("DefaultEndpointsProtocol=https;AccountName=x;AccountKey=abc==", False),
            ("BlobEndpoint=https://x;SharedAccessSignature=sig", False),
        ],
    )
    def test_classification(self, account, expected):
        assert account_uses_aad(account) is expected
