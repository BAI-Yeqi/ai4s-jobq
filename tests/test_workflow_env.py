# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

import pytest

from ai4s.jobq.workflow.env import (
    BLOBS_ENV,
    CONFIG_ENV,
    DEFAULT_BLOB_CONTAINER,
    LEGACY_ENV_VARS,
    QUEUES_ENV,
    WORKFLOW_ENV,
    WorkflowConfig,
    WorkflowEnv,
    WorkflowEnvError,
    _check_legacy_env,
    discover_config_path,
    load_config,
    parse_blobs_value,
    parse_workflow_value,
)

OLD_WORKFLOW_ENV = "JOBQ_WORKFLOW"  # old name


@pytest.fixture(autouse=True)
def clean_workflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (WORKFLOW_ENV, QUEUES_ENV, BLOBS_ENV, CONFIG_ENV, *LEGACY_ENV_VARS):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    ("kwargs", "expected_parts"),
    [
        ({}, ["storage account", "prefix"]),
        ({"state_account": "acct"}, ["prefix"]),
        ({"prefix": "pref"}, ["storage account"]),
    ],
)
def test_from_environ_requires_missing_values(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, str],
    expected_parts: list[str],
) -> None:
    """Verify that WorkflowEnv.from_environ raises a configuration error when required workflow values are missing."""
    monkeypatch.delenv(WORKFLOW_ENV, raising=False)

    with pytest.raises(WorkflowEnvError) as exc_info:
        WorkflowEnv.from_environ(**kwargs)

    message = str(exc_info.value)
    assert "not configured" in message, (
        "error should explain that the workflow environment is not configured"
    )
    for part in expected_parts:
        assert part in message, f"error should mention missing {part}"
    assert WORKFLOW_ENV in message, (
        f"error should reference {WORKFLOW_ENV} for configuration guidance"
    )


def test_from_environ_uses_partial_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that WorkflowEnv.from_environ combines environment configuration with CLI overrides for the same workflow."""
    monkeypatch.setenv(WORKFLOW_ENV, "envacct/envprefix")
    monkeypatch.setenv(QUEUES_ENV, "sb://queue-namespace")
    monkeypatch.setenv(BLOBS_ENV, "blobacct/workflow-data")

    env = WorkflowEnv.from_environ(prefix="cli-prefix")

    assert env.state_account == "envacct", (
        "state account should come from JOBQ_WORKFLOW_PREFIX when not overridden"
    )
    assert env.prefix == "cli-prefix", (
        "CLI prefix should override the prefix from JOBQ_WORKFLOW_PREFIX"
    )
    assert env.queues == "sb://queue-namespace", (
        "queue namespace should come from the environment override"
    )
    assert env.blob_account == "blobacct", "blob account should come from JOBQ_WORKFLOW_BLOBS"
    assert env.blob_container == "workflow-data", (
        "blob container should come from JOBQ_WORKFLOW_BLOBS"
    )


def test_from_environ_allows_full_cli_overrides_without_workflow_env() -> None:
    """Verify that full CLI overrides can build a WorkflowEnv without JOBQ_WORKFLOW_PREFIX."""
    env = WorkflowEnv.from_environ(state_account="cliacct", prefix="cliprefix")

    assert env.state_account == "cliacct", (
        "CLI state account should be used when JOBQ_WORKFLOW_PREFIX is unset"
    )
    assert env.prefix == "cliprefix", (
        "CLI prefix should be preserved when JOBQ_WORKFLOW_PREFIX is unset"
    )
    assert env.queues == "cliacct", (
        "queues should default to the state account for full CLI overrides"
    )
    assert env.blob_account == "cliacct", (
        "blob account should default to the state account for full CLI overrides"
    )
    assert env.blob_container == DEFAULT_BLOB_CONTAINER, (
        "blob container should default when no blob override is provided"
    )


@pytest.mark.parametrize(
    "value",
    [
        "https://account.example/prefix",
        "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=key",
        "UseDevelopmentStorage=true",
    ],
)
def test_parse_workflow_value_rejects_urls_and_connection_strings(value: str) -> None:
    """Verify that parse_workflow_value rejects URL and connection-string inputs instead of account/prefix values."""
    with pytest.raises(WorkflowEnvError, match="cannot be a URL or connection string"):
        parse_workflow_value(value)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("account", "Missing prefix"),
        ("/prefix", "Both segments must be non-empty"),
        ("account/", "Both segments must be non-empty"),
    ],
)
def test_parse_workflow_value_validates_prefix_segments(value: str, message: str) -> None:
    """Verify that parse_workflow_value rejects workflow values with missing or empty prefix segments."""
    with pytest.raises(WorkflowEnvError, match=message):
        parse_workflow_value(value)


@pytest.mark.parametrize(
    "value",
    [
        "https://account.example/container",
        "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=key",
        "UseDevelopmentStorage=true",
    ],
)
def test_parse_blobs_value_rejects_urls_and_connection_strings(value: str) -> None:
    """Verify that parse_blobs_value rejects URL and connection-string inputs instead of account/container values."""
    with pytest.raises(WorkflowEnvError, match="cannot be a URL or connection string"):
        parse_blobs_value(value)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("account", "Missing container"),
        ("/container", "Both segments must be non-empty"),
        ("account/", "Both segments must be non-empty"),
    ],
)
def test_parse_blobs_value_validates_container_segments(value: str, message: str) -> None:
    """Verify that parse_blobs_value rejects blob values with missing or empty container segments."""
    with pytest.raises(WorkflowEnvError, match=message):
        parse_blobs_value(value)


def test_check_legacy_env_rejects_deprecated_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that legacy workflow environment variables raise a migration error with updated configuration guidance."""
    monkeypatch.setenv("JOBQ_WORKFLOW_STATE", "legacy-state")
    monkeypatch.setenv(OLD_WORKFLOW_ENV, "legacy-prefix")

    with pytest.raises(WorkflowEnvError) as exc_info:
        _check_legacy_env()

    message = str(exc_info.value)
    assert "JOBQ_WORKFLOW_STATE" in message, (
        "migration error should name the deprecated state variable"
    )
    assert OLD_WORKFLOW_ENV in message, (
        "migration error should name the deprecated workflow variable"
    )
    assert f"Set {WORKFLOW_ENV}=<account>/<prefix> instead" in message, (
        "migration error should point to the replacement workflow variable"
    )
    assert QUEUES_ENV in message, (
        f"migration error should mention {QUEUES_ENV} for queue configuration"
    )
    assert BLOBS_ENV in message, (
        f"migration error should mention {BLOBS_ENV} for blob configuration"
    )


def _write_config(tmp_path, body: str) -> str:
    path = tmp_path / "jobq.yaml"
    path.write_text(body, encoding="utf-8")
    return str(path)


def test_load_config_parses_connection_and_coordinator(tmp_path) -> None:
    """A well-formed jobq.yaml exposes its connection and coordinator sections."""
    path = _write_config(
        tmp_path,
        "connection:\n"
        "  storage: fileacct\n"
        "  prefix: FileProject\n"
        "  queues: sb://file-namespace\n"
        "  blobs: fileacct/file-data\n"
        "coordinator:\n"
        "  batch_size: 64\n"
        "  running_timeout_s: 3600\n",
    )
    cfg = load_config(path)
    assert cfg.path == path
    assert cfg.connection == {
        "storage": "fileacct",
        "prefix": "FileProject",
        "queues": "sb://file-namespace",
        "blobs": "fileacct/file-data",
    }
    assert cfg.coordinator == {"batch_size": 64, "running_timeout_s": 3600}


def test_load_config_missing_explicit_path_raises(tmp_path) -> None:
    """An explicitly-passed but missing config path is a hard error."""
    missing = str(tmp_path / "nope.yaml")
    with pytest.raises(WorkflowEnvError, match="Config file not found"):
        load_config(missing)


def test_load_config_missing_implicit_path_is_silent(monkeypatch, tmp_path) -> None:
    """Implicit discovery of a non-existent file yields an empty config."""
    monkeypatch.chdir(tmp_path)
    cfg = load_config()
    assert cfg == WorkflowConfig()


def test_load_config_rejects_unknown_connection_keys(tmp_path) -> None:
    path = _write_config(tmp_path, "connection:\n  storage: a\n  bogus: b\n")
    with pytest.raises(WorkflowEnvError, match=r"Unknown key.*bogus"):
        load_config(path)


def test_load_config_rejects_unknown_coordinator_keys(tmp_path) -> None:
    path = _write_config(tmp_path, "coordinator:\n  nope: 1\n")
    with pytest.raises(WorkflowEnvError, match=r"Unknown key.*nope"):
        load_config(path)


def test_discover_config_path_prefers_explicit(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "jobq.yaml").write_text("connection: {}\n", encoding="utf-8")
    assert discover_config_path("/explicit/path.yaml") == "/explicit/path.yaml"
    monkeypatch.setenv(CONFIG_ENV, "/from/env.yaml")
    assert discover_config_path() == "/from/env.yaml"
    monkeypatch.delenv(CONFIG_ENV, raising=False)
    assert discover_config_path() == "jobq.yaml"


def test_from_environ_precedence_flag_env_file(monkeypatch, tmp_path) -> None:
    """flag beats env beats file for each connection field."""
    path = _write_config(
        tmp_path,
        "connection:\n"
        "  storage: fileacct\n"
        "  prefix: FilePrefix\n"
        "  queues: sb://file-ns\n"
        "  blobs: fileacct/file-data\n",
    )
    monkeypatch.setenv(WORKFLOW_ENV, "envacct/envprefix")
    monkeypatch.setenv(QUEUES_ENV, "sb://env-ns")
    # storage/prefix from env; queues from flag; blobs falls through to file.
    env = WorkflowEnv.from_environ(queues="sb://flag-ns", config=path)
    assert env.state_account == "envacct"
    assert env.sources["storage"] == "env"
    assert env.prefix == "envprefix"
    assert env.queues == "sb://flag-ns"
    assert env.sources["queues"] == "flag"
    assert env.blob_account == "fileacct"
    assert env.sources["blobs"] == "file"
    assert env.config_path == path


def test_from_environ_uses_file_when_no_flag_or_env(monkeypatch, tmp_path) -> None:
    path = _write_config(
        tmp_path,
        "connection:\n  storage: fileacct\n  prefix: FilePrefix\n",
    )
    env = WorkflowEnv.from_environ(config=path)
    assert env.state_account == "fileacct"
    assert env.prefix == "FilePrefix"
    assert env.sources["storage"] == "file"
    assert env.sources["prefix"] == "file"
    # queues/blobs default to the state account.
    assert env.queues == "fileacct"
    assert env.sources["queues"] == "default"
    assert env.blob_account == "fileacct"
    assert env.blob_container == DEFAULT_BLOB_CONTAINER
    assert env.coordinator == {}
