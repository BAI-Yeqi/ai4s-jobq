# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import json
import signal
from unittest.mock import AsyncMock, patch

import pytest

from ai4s.jobq.orchestration.workforce_monitor import workforce_monitor


class FakeMessage:
    def __init__(self, body: str):
        self._body = body

    def __str__(self):
        return self._body


class FakeReceiver:
    """A fake ServiceBus subscription receiver for testing."""

    def __init__(self, messages_sequence: list):
        self._messages_sequence = messages_sequence
        self._call_count = 0
        self.completed_messages = []

    async def receive_messages(self, max_message_count=1, max_wait_time=None):
        if self._call_count >= len(self._messages_sequence):
            # Block forever to simulate waiting
            await asyncio.sleep(3600)
        item = self._messages_sequence[self._call_count]
        self._call_count += 1
        if isinstance(item, Exception):
            raise item
        return item

    async def complete_message(self, message):
        self.completed_messages.append(message)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class FakeServiceBusClient:
    def __init__(self, receiver):
        self._receiver = receiver

    def get_subscription_receiver(self, **kwargs):
        return self._receiver

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture
def env_vars(monkeypatch, tmp_path):
    monkeypatch.setenv(
        "WORKFORCE_BACK_CHANNEL_FULLY_QUALIFIED_NAMESPACE", "test.servicebus.windows.net"
    )
    monkeypatch.setenv("WORKFORCE_CONTROL_TOPIC_NAME", "test-topic")
    monkeypatch.setenv("JOBQ_PID_DIR", str(tmp_path))
    return tmp_path


def _make_credential_mock():
    credential = AsyncMock()
    credential.__aenter__ = AsyncMock(return_value=credential)
    credential.__aexit__ = AsyncMock(return_value=False)
    return credential


async def test_json_decode_error_does_not_backoff(env_vars, caplog):
    """JSONDecodeError should log accurately and NOT trigger exponential backoff."""
    bad_message = FakeMessage("not-valid-json{{{")
    good_message = FakeMessage(json.dumps({"operation": "do-not-accept-new-tasks"}))

    receiver = FakeReceiver(
        messages_sequence=[
            [bad_message],  # first call: bad JSON
            [good_message],  # second call: valid shutdown
        ]
    )
    client = FakeServiceBusClient(receiver)

    sleep_values = []
    original_sleep = asyncio.sleep

    async def capture_sleep(seconds):
        sleep_values.append(seconds)

    with (
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.get_token_credential",
            return_value=_make_credential_mock(),
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.ServiceBusClient",
            return_value=client,
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.send_signal_by_glob",
        ) as mock_signal,
        patch("asyncio.sleep", side_effect=capture_sleep),
    ):
        async with workforce_monitor("worker-1", "test-queue"):
            await original_sleep(0.1)

    # The JSON error is logged with accurate message
    assert "Could not json-decode message" in caplog.text
    # NOT the generic "Error receiving message" (that's for transient errors)
    assert "Error receiving message" not in caplog.text
    # No backoff sleep was triggered for JSON decode errors
    assert sleep_values == []
    # The monitor still processed the valid shutdown message
    mock_signal.assert_called_once_with(
        pattern=f"{env_vars}/*.pid",
        signal_to_send=signal.SIGUSR1,
    )


async def test_transient_error_triggers_backoff(env_vars, caplog):
    """Transient errors (like ServiceBusServerBusyError) should backoff before retry."""
    good_message = FakeMessage(json.dumps({"operation": "graceful-downscale"}))

    receiver = FakeReceiver(
        messages_sequence=[
            RuntimeError("ServiceBusServerBusyError: throttled"),  # transient error
            [good_message],  # recovery
        ]
    )
    client = FakeServiceBusClient(receiver)

    sleep_values = []
    original_sleep = asyncio.sleep

    async def capture_sleep(seconds):
        sleep_values.append(seconds)
        # Don't actually wait in tests

    with (
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.get_token_credential",
            return_value=_make_credential_mock(),
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.ServiceBusClient",
            return_value=client,
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.send_signal_by_glob",
        ) as mock_signal,
        patch("asyncio.sleep", side_effect=capture_sleep),
    ):
        async with workforce_monitor("worker-1", "test-queue"):
            await original_sleep(0.1)

    # Generic error message is used (not "json-decode")
    assert "Error receiving message" in caplog.text
    assert "retrying in 1.0s" in caplog.text
    # Backoff sleep was called with 1.0 second
    assert 1.0 in sleep_values
    # The monitor still recovered and processed the shutdown
    mock_signal.assert_called_once_with(
        pattern=f"{env_vars}/*.pid",
        signal_to_send=signal.SIGUSR2,
    )


async def test_exponential_backoff_caps_at_60s(env_vars, caplog):
    """Backoff should double each time but cap at 60 seconds."""
    good_message = FakeMessage(json.dumps({"operation": "do-not-accept-new-tasks"}))

    # 8 consecutive errors: backoff should be 1, 2, 4, 8, 16, 32, 60, 60
    errors = [RuntimeError("busy") for _ in range(8)]
    receiver = FakeReceiver(messages_sequence=[*errors, [good_message]])
    client = FakeServiceBusClient(receiver)

    sleep_values = []
    original_sleep = asyncio.sleep

    async def capture_sleep(seconds):
        sleep_values.append(seconds)
        # Don't actually sleep in tests

    with (
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.get_token_credential",
            return_value=_make_credential_mock(),
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.ServiceBusClient",
            return_value=client,
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.send_signal_by_glob",
        ),
        patch("asyncio.sleep", side_effect=capture_sleep),
    ):
        async with workforce_monitor("worker-1", "test-queue"):
            await original_sleep(0.2)

    assert sleep_values == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]


async def test_backoff_resets_after_success(env_vars):
    """After a successful receive, backoff should reset to zero."""
    bad_json_msg = FakeMessage("not-json")
    good_message = FakeMessage(json.dumps({"operation": "do-not-accept-new-tasks"}))

    receiver = FakeReceiver(
        messages_sequence=[
            RuntimeError("busy"),  # triggers backoff=1
            [bad_json_msg],  # success (receive worked, JSON failed) → resets backoff
            RuntimeError("busy again"),  # should start at 1 again, not 2
            [good_message],  # final success
        ]
    )
    client = FakeServiceBusClient(receiver)

    sleep_values = []
    original_sleep = asyncio.sleep

    async def capture_sleep(seconds):
        sleep_values.append(seconds)

    with (
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.get_token_credential",
            return_value=_make_credential_mock(),
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.ServiceBusClient",
            return_value=client,
        ),
        patch(
            "ai4s.jobq.orchestration.workforce_monitor.send_signal_by_glob",
        ),
        patch("asyncio.sleep", side_effect=capture_sleep),
    ):
        async with workforce_monitor("worker-1", "test-queue"):
            await original_sleep(0.1)

    # First backoff=1, then reset, then backoff=1 again (not 2)
    assert sleep_values == [1.0, 1.0]
