# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

import pytest

from ai4s.jobq.workflow.env import (
    BLOBS_ENV,
    DEFAULT_BLOB_CONTAINER,
    LEGACY_ENV_VARS,
    QUEUES_ENV,
    WORKFLOW_ENV,
    WorkflowEnv,
    WorkflowEnvError,
    _check_legacy_env,
    parse_blobs_value,
    parse_workflow_value,
)

OLD_WORKFLOW_ENV = "JOBQ_WORKFLOW"  # old name


@pytest.fixture(autouse=True)
def clean_workflow_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (WORKFLOW_ENV, QUEUES_ENV, BLOBS_ENV, *LEGACY_ENV_VARS):
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
