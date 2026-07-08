# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for StorageQueueEnvelope metadata exposed to the coordinator.

The workflow coordinator uses ``envelope.delivery_count`` to detect
permanently stuck completion messages (REAPPLY / DROP thresholds in
``Coordinator._handle_completion``). When the Storage Queue backend
omits this attribute, the coordinator falls back to ``0`` and the
escape hatch never trips — producing an infinite redelivery loop.
This test pins the contract.
"""

from __future__ import annotations

import asyncio

from azure.storage.queue import QueueMessage

from ai4s.jobq.backend.storage_queue import StorageQueueEnvelope
from ai4s.jobq.entities import Task


def _make_envelope(dequeue_count: int | None) -> StorageQueueEnvelope:
    msg = QueueMessage(
        id="msg-id",
        content="{}",
        dequeue_count=dequeue_count,
        pop_receipt="popr",
    )
    task = Task(id="task-id", kwargs={}, num_retries=0)
    return StorageQueueEnvelope(
        message=msg,
        task=task,
        backend=None,  # type: ignore[arg-type]
        cancel_heartbeat_event=asyncio.Event(),
        heartbeat_cancelled_event=asyncio.Event(),
    )


def test_envelope_exposes_delivery_count_from_dequeue_count() -> None:
    env = _make_envelope(dequeue_count=7)
    assert env.delivery_count == 7


def test_envelope_delivery_count_defaults_to_zero_when_missing() -> None:
    env = _make_envelope(dequeue_count=None)
    assert env.delivery_count == 0


def test_envelope_delivery_count_starts_at_one_on_first_receive() -> None:
    # Azure Storage Queue increments dequeue_count to 1 on the first
    # successful receive.  The envelope passes that through unchanged
    # so the coordinator's REAPPLY/DROP thresholds use Azure-native
    # semantics (1 = first delivery).
    env = _make_envelope(dequeue_count=1)
    assert env.delivery_count == 1
