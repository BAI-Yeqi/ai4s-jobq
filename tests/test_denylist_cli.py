# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""End-to-end tests for the ``ai4s-jobq denylist`` CLI against Azurite."""

from __future__ import annotations

import socket
import uuid

import pytest
from asyncclick.testing import CliRunner

from ai4s.jobq.denylist_cli import denylist_group


def _table_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 10002), timeout=1).close()
        return True
    except OSError:
        return False


skip_without_table = pytest.mark.skipif(
    not _table_up(), reason="Azurite table storage must be running on port 10002"
)

_HEX = "b" * 64
_DIGEST = f"sha256:{_HEX}"


@pytest.fixture
def denylist_env(monkeypatch):
    monkeypatch.setenv("JOBQ_DENYLIST_ACCOUNT", "devstoreaccount1")
    monkeypatch.setenv("JOBQ_DENYLIST_TABLE", "DenylistCli" + uuid.uuid4().hex[:12])
    monkeypatch.delenv("JOBQ_DENYLIST_DISABLE", raising=False)


@skip_without_table
class TestDenylistCli:
    async def test_add_list_check_remove_roundtrip(self, denylist_env):
        runner = CliRunner()

        res = await runner.invoke(
            denylist_group, ["add", _DIGEST, "--reason", "bad", "--shutdown-mode", "hard"]
        )
        assert res.exit_code == 0, res.output
        assert "Added" in res.output

        res = await runner.invoke(denylist_group, ["list"])
        assert res.exit_code == 0
        assert _DIGEST in res.output

        # check on a denied digest exits 1
        res = await runner.invoke(denylist_group, ["check", _DIGEST])
        assert res.exit_code == 1
        assert "DENIED" in res.output

        res = await runner.invoke(denylist_group, ["remove", _DIGEST])
        assert res.exit_code == 0
        assert "Removed" in res.output

        res = await runner.invoke(denylist_group, ["check", _DIGEST])
        assert res.exit_code == 0
        assert "Not denied" in res.output

    async def test_check_not_denied_exit_zero(self, denylist_env):
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["check", "sha256:" + "c" * 64])
        assert res.exit_code == 0
        assert "Not denied" in res.output

    async def test_effective_future_listed_but_not_enforced(self, denylist_env):
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["add", _DIGEST, "--effective", "2099-01-01"])
        assert res.exit_code == 0, res.output
        assert "pending" in res.output

        # Listed (visible) with its effective date...
        res = await runner.invoke(denylist_group, ["list"])
        assert _DIGEST in res.output
        assert "effective=2099-01-01" in res.output

        # ...but check reports not-denied and exits 0 (not yet effective).
        res = await runner.invoke(denylist_group, ["check", _DIGEST])
        assert res.exit_code == 0, res.output
        assert "Not denied" in res.output

    async def test_effective_bad_value_rejected(self, denylist_env):
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["add", _DIGEST, "--effective", "soon"])
        assert res.exit_code != 0

    async def test_list_json_empty(self, denylist_env):
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["list", "--as-json"])
        assert res.exit_code == 0
        assert res.output.strip() == "[]"

    async def test_remove_absent_reports(self, denylist_env):
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["remove", "sha256:" + "d" * 64])
        assert res.exit_code == 0
        assert "Not on denylist" in res.output

    async def test_invalid_digest_rejected(self, denylist_env):
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["add", "not-a-digest"])
        assert res.exit_code != 0

    async def test_added_by_auto_derived_for_aad(self, denylist_env, monkeypatch):
        import ai4s.jobq.auth as auth_mod
        import ai4s.jobq.denylist as dl_mod

        async def fake_identity(*a, **k):
            return "derived@example.com"

        monkeypatch.setattr(dl_mod, "account_uses_aad", lambda _acct: True)
        monkeypatch.setattr(auth_mod, "caller_identity", fake_identity)

        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["add", _DIGEST])
        assert res.exit_code == 0, res.output
        assert "by=derived@example.com" in res.output

    async def test_added_by_explicit_wins(self, denylist_env, monkeypatch):
        import ai4s.jobq.auth as auth_mod

        async def boom(*a, **k):
            raise AssertionError("must not derive identity when --added-by is given")

        monkeypatch.setattr(auth_mod, "caller_identity", boom)
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["add", _DIGEST, "--added-by", "me@x.com"])
        assert res.exit_code == 0, res.output
        assert "by=me@x.com" in res.output

    async def test_added_by_skipped_for_non_aad(self, denylist_env, monkeypatch):
        # Azurite / connection-string auth carries no user token to derive from.
        import ai4s.jobq.auth as auth_mod

        async def boom(*a, **k):
            raise AssertionError("must not derive identity for connection-string auth")

        monkeypatch.setattr(auth_mod, "caller_identity", boom)
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["add", _DIGEST])
        assert res.exit_code == 0, res.output


class TestDenylistCliUnconfigured:
    async def test_add_without_account_errors(self, monkeypatch):
        # The default account keeps the CLI configured out of the box; the
        # "no account" error path is still exercised if the resolver yields
        # None (e.g. a future build with the default removed).
        import ai4s.jobq.denylist_cli as cli_mod

        monkeypatch.setattr(cli_mod, "denylist_account", lambda: None)
        runner = CliRunner()
        res = await runner.invoke(denylist_group, ["add", _DIGEST])
        assert res.exit_code != 0
        assert "No denylist configured" in res.output
