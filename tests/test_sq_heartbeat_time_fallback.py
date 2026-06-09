# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Test that Storage Queue heartbeat declares lock lost after visibility timeout elapses.

When heartbeat updates fail with non-HTTP errors (network issues, timeouts),
the heartbeat loop must detect that the visibility timeout has elapsed without
a successful update and set lock_lost_event — rather than retrying indefinitely.
"""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from ai4s.jobq.backend.storage_queue import StorageQueueBackend


@pytest.fixture
def mock_queue_client():
    """Create a mock QueueClient that fails update_message with a generic error."""
    client = AsyncMock()
    client.update_message = AsyncMock(side_effect=OSError("network unreachable"))
    return client


async def test_heartbeat_declares_lock_lost_after_visibility_timeout(mock_queue_client):
    """If all heartbeat attempts fail for longer than visibility_timeout,
    lock_lost_event must be set.
    """
    backend = StorageQueueBackend.__new__(StorageQueueBackend)
    backend.queue_client = mock_queue_client

    # Use very short timings for fast test execution
    visibility_timeout = timedelta(seconds=2)
    interval = 0.5  # heartbeat every 0.5s; visibility is 2s

    message = MagicMock()
    message.id = "test-msg-123"
    message.pop_receipt = "pop-receipt-abc"

    cancel_heartbeat_event = asyncio.Event()
    heartbeat_cancelled_event = asyncio.Event()
    lock_lost_event = asyncio.Event()

    async with backend._heartbeat_worker(
        message,
        interval=interval,
        visibility_timeout=visibility_timeout,
        cancel_heartbeat_event=cancel_heartbeat_event,
        heartbeat_cancelled_event=heartbeat_cancelled_event,
        lock_lost_event=lock_lost_event,
    ):
        # Wait for the lock_lost_event to be set (should happen after ~2s)
        try:
            await asyncio.wait_for(lock_lost_event.wait(), timeout=5.0)
        except TimeoutError:
            pytest.fail("lock_lost_event was not set within 5s")

    assert lock_lost_event.is_set()


async def test_heartbeat_does_not_declare_lock_lost_on_transient_failure_within_timeout(
    mock_queue_client,
):
    """If heartbeat fails but then succeeds before visibility_timeout elapses,
    lock_lost_event must NOT be set.
    """
    backend = StorageQueueBackend.__new__(StorageQueueBackend)
    backend.queue_client = mock_queue_client

    visibility_timeout = timedelta(seconds=4)
    interval = 0.3

    # First call fails, second call succeeds
    success_response = MagicMock()
    success_response.pop_receipt = "new-receipt"
    mock_queue_client.update_message = AsyncMock(
        side_effect=[OSError("transient"), success_response, success_response, success_response]
    )

    message = MagicMock()
    message.id = "test-msg-456"
    message.pop_receipt = "pop-receipt-def"

    cancel_heartbeat_event = asyncio.Event()
    heartbeat_cancelled_event = asyncio.Event()
    lock_lost_event = asyncio.Event()

    async with backend._heartbeat_worker(
        message,
        interval=interval,
        visibility_timeout=visibility_timeout,
        cancel_heartbeat_event=cancel_heartbeat_event,
        heartbeat_cancelled_event=heartbeat_cancelled_event,
        lock_lost_event=lock_lost_event,
    ):
        # Let a few heartbeats run (transient failure then success)
        await asyncio.sleep(1.5)
        # Stop heartbeat gracefully
        cancel_heartbeat_event.set()

    assert not lock_lost_event.is_set()
