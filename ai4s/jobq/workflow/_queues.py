# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Backend-agnostic JobQ openers for workflow components.

Shared helpers used by both the coordinator and the client to open
queue connections from a single ``queues_account`` string covering
Service Bus, real Storage Queue, and Azurite endpoints.  Kept in its
own module so user-surface modules (``client.py``, ``worker.py``) can
build queue handles without importing the coordinator.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING

from ai4s.jobq import JobQ

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from types import TracebackType


def _parse_queue_spec(queue_name: str, default_account: str) -> tuple[str, str]:
    """Parse queue specification to extract account and queue name.

    Supports:
    * ``account/queue-name`` — extract account from queue spec
    * ``queue-name`` — use default account
    * ``sb://namespace/queue-name`` — Service Bus format with namespace override
    * ``sb://namespace`` — Service Bus namespace only (queue name from queue_name)

    Returns:
        (account, queue_name) tuple
    """
    # Handle sb:// Service Bus format with potential queue name
    if queue_name.startswith("sb://"):
        if "/" in queue_name[len("sb://") :]:
            # Format: sb://namespace/queue-name
            parts = queue_name.split("/", 2)  # Split on first two /
            return (f"sb://{parts[2]}", parts[3] if len(parts) > 3 else parts[2])
        # Just sb://namespace, use as account
        return (queue_name, "")

    # Handle account/queue-name format
    if "/" in queue_name:
        parts = queue_name.split("/", 1)
        return (parts[0], parts[1])

    # Default: use default account with queue name as-is
    return (default_account, queue_name)


@asynccontextmanager
async def open_jobq(queue_name: str, queues_account: str) -> AsyncIterator[JobQ]:
    """Open a :class:`JobQ` for *queue_name* on the given backend.

    Accepts queue specifications:

    * ``account/queue-name`` — use account from queue spec, not default
    * ``queue-name`` — use default queues_account
    * ``sb://namespace`` or ``sb://namespace/queue-name`` — Service Bus
    * ``devstoreaccount1`` — Azurite (uses the well-known dev connection string).
    * Connection strings — when the value contains ``AccountKey=`` or
      ``SharedAccessSignature=``.

    The *queues_account* parameter serves as the default for queue specs
    that don't specify an account.
    """
    if not queues_account:
        raise ValueError("queues_account is required to open queue connections")

    # Parse queue spec to extract account and actual queue name
    account, actual_queue_name = _parse_queue_spec(queue_name, queues_account)

    if account.startswith("sb://"):
        from ai4s.jobq.auth import get_token_credential

        fqns = account[len("sb://") :] + ".servicebus.windows.net"
        # If queue name wasn't specified in the spec, use the original queue_name
        q_name = actual_queue_name or queue_name.rsplit("/", 1)[-1]
        async with JobQ.from_service_bus(
            q_name,
            fqns=fqns,
            credential=get_token_credential(),
            exist_ok=True,
        ) as q:
            yield q
            return

    if account == "devstoreaccount1":
        from ai4s.jobq.backend.storage_queue import azurite_conn_str

        async with JobQ.from_connection_string(
            actual_queue_name, connection_string=azurite_conn_str(), exist_ok=True
        ) as q:
            yield q
            return

    if "AccountKey=" in account or "SharedAccessSignature=" in account:
        async with JobQ.from_connection_string(
            actual_queue_name, connection_string=account, exist_ok=True
        ) as q:
            yield q
            return

    from ai4s.jobq.auth import get_token_credential

    async with JobQ.from_storage_queue(
        actual_queue_name,
        storage_account=account,
        credential=get_token_credential(),
        exist_ok=True,
    ) as q:
        yield q


class JobQPool:
    """Lazy pool of one :class:`JobQ` per queue name.

    All queue connections share one :class:`AsyncExitStack` so they
    close cleanly when the owner shuts down.  Concurrent ``get`` calls
    for the same queue are serialized to avoid double-open.
    """

    def __init__(self, queues_account: str) -> None:
        self._queues_account = queues_account
        self._stack = AsyncExitStack()
        self._cache: dict[str, JobQ] = {}
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> JobQPool:
        await self._stack.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._stack.__aexit__(exc_type, exc, tb)

    async def get(self, queue_name: str) -> JobQ:
        if queue_name in self._cache:
            return self._cache[queue_name]
        async with self._lock:
            if queue_name in self._cache:
                return self._cache[queue_name]
            cm = open_jobq(queue_name, self._queues_account)
            jobq = await self._stack.enter_async_context(cm)
            self._cache[queue_name] = jobq
            return jobq
