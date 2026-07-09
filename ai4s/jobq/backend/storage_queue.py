# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import logging
import os
import time
import typing as ty
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import replace as replace_in_dataclass
from datetime import timedelta
from types import TracebackType

import azure.core.exceptions
from azure.core.credentials_async import AsyncTokenCredential
from azure.storage.queue import QueueMessage
from azure.storage.queue.aio import QueueClient

from ai4s.jobq.entities import EmptyQueue, Response, Task

from .common import Envelope, JobQBackend

LOG = logging.getLogger(__name__)

# Azurite's well-known account key (public, non-secret).
_AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


def azurite_conn_str(
    *,
    queue_port: int | None = None,
    service: str = "queue",
    port: int | None = None,
) -> str:
    """Build the Azurite connection string for one of its services.

    Args:
        service: ``"queue"`` (default), ``"table"``, or ``"blob"``.
        port: Override the default port. If unset, the corresponding
            ``QUEUE_PORT``/``TABLE_PORT``/``BLOB_PORT`` env var is
            consulted, then falls back to the standard Azurite port
            (10001/10002/10000).
        queue_port: Backwards-compatible alias for ``port`` when
            ``service="queue"``. Prefer ``port=…`` in new code.
    """
    default_ports = {"queue": 10001, "table": 10002, "blob": 10000}
    env_vars = {"queue": "QUEUE_PORT", "table": "TABLE_PORT", "blob": "BLOB_PORT"}
    endpoints = {"queue": "QueueEndpoint", "table": "TableEndpoint", "blob": "BlobEndpoint"}
    if service not in default_ports:
        raise ValueError(f"Unknown service {service!r}; expected queue/table/blob")
    chosen_port = (
        port or queue_port or int(os.environ.get(env_vars[service], str(default_ports[service])))
    )
    return (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        f"AccountKey={_AZURITE_ACCOUNT_KEY};"
        f"{endpoints[service]}=http://127.0.0.1:{chosen_port}/devstoreaccount1;"
    )


class StorageQueueEnvelope(Envelope):
    def __init__(
        self,
        message: QueueMessage,
        task: Task,
        backend: "StorageQueueBackend",
        cancel_heartbeat_event: asyncio.Event,
        heartbeat_cancelled_event: asyncio.Event,
        lock_lost_event: asyncio.Event | None = None,
    ):
        self.message = message
        self.backend = backend
        self._task = task
        self.cancel_heartbeat_event = cancel_heartbeat_event
        self.heartbeat_cancelled_event = heartbeat_cancelled_event
        self._lock_lost_event = lock_lost_event or asyncio.Event()

    @property
    def id(self) -> str:
        return self.message.id

    @property
    def task(self) -> Task:
        return self._task

    @property
    def lock_lost_event(self) -> asyncio.Event:
        return self._lock_lost_event

    @property
    def delivery_count(self) -> int:
        """Number of times this message has been received from the queue.

        Surfaces Azure Storage Queue's ``dequeue_count`` (1 on first
        receipt, incremented on every subsequent redelivery) so the
        workflow coordinator's stale-completion escape hatch
        (REAPPLY / DROP thresholds) can fire on a wedged completion.
        Returns ``0`` if the broker did not report a count (e.g. a
        unit-test fixture or a peeked message).
        """
        return int(self.message.dequeue_count or 0)

    async def delete(self, success: bool, error: str | None = None) -> None:
        if not success:
            await self.backend.add_to_dead_letter_queue(self.task, error)
        await self.backend.delete(self.message)

    async def cancel_heartbeat(self) -> None:
        LOG.debug("Cancelling heartbeat for %s", self.message.id)
        self.cancel_heartbeat_event.set()
        await self.heartbeat_cancelled_event.wait()

    async def requeue(self) -> None:
        LOG.debug("Requeueing %s", self.message.id)
        assert self.backend.queue_client is not None
        await self.backend.queue_client.update_message(
            self.message,
            content=self.task.serialize(),
            visibility_timeout=0,  # return back into the queue
        )

    async def replace(self, task: Task) -> None:
        assert self.backend.queue_client is not None
        self._task = task
        await self.backend.queue_client.update_message(
            self.message,
            content=task.serialize(),
            visibility_timeout=0,  # return back into the queue
        )

    async def reply(self, response: Response) -> None:
        raise NotImplementedError("Reply is not implemented for StorageQueueBackend")


class StorageQueueBackend(JobQBackend):
    def __init__(
        self,
        queue_name: str,
        *,
        storage_account: str | None = None,
        connection_string: str | None = None,
        credential: str | AsyncTokenCredential | None = None,
        connection_pool_size: int | None = None,
    ):
        self.connection_string = connection_string
        self.storage_account = storage_account
        self.queue_name = queue_name
        self.queue_client: QueueClient | None = None
        self.dead_letter_queue_client: QueueClient | None = None
        self.credential = credential
        # When set, override the per-client HTTP connection pool size
        # (aiohttp ``TCPConnector(limit=...)``).  Defaults to the
        # azure-core / aiohttp default of 100 concurrent connections.
        # Bumping this can reduce per-call latency on a coordinator
        # process that has many concurrent in-flight pushes against the
        # same queue host; the value is read at __aenter__ and applied
        # to both the main and dead-letter queue clients.
        # Falls back to the ``JOBQ_HTTP_POOL_SIZE`` env var when not
        # explicitly set.
        self.connection_pool_size = connection_pool_size

        if self.queue_name == "my-unique-queue":
            raise ValueError("Don't be lazy and change the queue name to something unique.")

    @property
    def dead_letter_queue_name(self) -> str:
        return f"{self.queue_name}-failed"

    def _client_kwargs(self) -> dict[str, ty.Any]:
        """Extra kwargs to inject into every ``QueueClient`` we create.

        Used to thread the optional ``connection_pool_size`` override
        through both the main and dead-letter clients without
        duplicating the conditional.
        """
        from ai4s.jobq._http_transport import transport_kwargs_for_pool_size

        return transport_kwargs_for_pool_size(self.connection_pool_size)

    async def __aenter__(self) -> "StorageQueueBackend":
        if self.connection_string:
            self.queue_client = QueueClient.from_connection_string(
                self.connection_string, self.queue_name, **self._client_kwargs()
            )
            self.dead_letter_queue_client = QueueClient.from_connection_string(
                self.connection_string, self.dead_letter_queue_name, **self._client_kwargs()
            )
        elif self.credential:
            self.queue_client = QueueClient(
                account_url=f"https://{self.storage_account}.queue.core.windows.net",
                queue_name=self.queue_name,
                credential=self.credential,
                **self._client_kwargs(),
            )
            self.dead_letter_queue_client = QueueClient(
                account_url=f"https://{self.storage_account}.queue.core.windows.net",
                queue_name=self.dead_letter_queue_name,
                credential=self.credential,
                **self._client_kwargs(),
            )
        else:
            raise ValueError("Either connection_string or credential must be provided.")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self.queue_client is not None
        await self.queue_client.__aexit__(exc_type, exc, tb)
        if self.dead_letter_queue_client is not None:
            await self.dead_letter_queue_client.__aexit__(exc_type, exc, tb)

    async def push(self, task: Task) -> str:
        assert self.queue_client is not None
        if task.reply_requested:
            raise NotImplementedError("Reply is not supported for StorageQueueBackend.")
        msg = await self.queue_client.send_message(task.serialize(), time_to_live=-1)
        return msg.id

    @asynccontextmanager
    async def receive_message(
        self, visibility_timeout: timedelta, with_heartbeat: bool = False, **kwargs
    ) -> ty.AsyncGenerator[StorageQueueEnvelope, None]:
        assert self.queue_client is not None
        envelope = await self.queue_client.receive_message(
            visibility_timeout=int(visibility_timeout.total_seconds())
        )
        if envelope is None:
            raise EmptyQueue(f"The queue {self.name} has no more tasks.")
        cancel_heartbeat_event = asyncio.Event()
        heartbeat_cancelled_event = asyncio.Event()
        lock_lost_event = asyncio.Event()
        async with AsyncExitStack() as stack:
            assert envelope is not None
            if with_heartbeat:
                heartbeat_interval = visibility_timeout.total_seconds() / 2

                assert heartbeat_interval > 0, (
                    "Visibility timeout must be at least 2 seconds for heartbeat to work."
                )

                await stack.enter_async_context(
                    self._heartbeat_worker(
                        envelope,
                        interval=heartbeat_interval,
                        visibility_timeout=visibility_timeout,
                        cancel_heartbeat_event=cancel_heartbeat_event,
                        heartbeat_cancelled_event=heartbeat_cancelled_event,
                        lock_lost_event=lock_lost_event,
                    )
                )
            else:
                heartbeat_cancelled_event.set()

            try:
                task = Task.deserialize(envelope["content"])
            except Exception:
                LOG.exception(
                    "Stopping processing due to deserialization error to prevent potential data loss.",
                )
                raise
            else:
                yield StorageQueueEnvelope(
                    envelope,
                    task,
                    self,
                    cancel_heartbeat_event,
                    heartbeat_cancelled_event,
                    lock_lost_event,
                )

    async def receive_messages_batch(
        self,
        max_messages: int,
        visibility_timeout: timedelta,
    ) -> list[QueueMessage]:
        """Receive up to *max_messages* in a single HTTP call.

        Returns raw ``QueueMessage`` objects (no heartbeat, no
        deserialization).  The caller is responsible for wrapping each
        into an ``Envelope`` via :meth:`receive_message` or equivalent.

        Raises ``EmptyQueue`` if the queue has no visible messages.
        """
        assert self.queue_client is not None
        vis = int(visibility_timeout.total_seconds())
        messages: list[QueueMessage] = []
        async for msg in self.queue_client.receive_messages(
            messages_per_page=min(max_messages, 32),
            visibility_timeout=vis,
            max_messages=max_messages,
        ):
            messages.append(msg)
            if len(messages) >= max_messages:
                break
        if not messages:
            raise EmptyQueue(f"The queue {self.name} has no more tasks.")
        return messages

    def wrap_message(
        self,
        message: QueueMessage,
        visibility_timeout: timedelta,
        with_heartbeat: bool = False,
    ) -> ty.AsyncContextManager["StorageQueueEnvelope"]:
        """Wrap a raw ``QueueMessage`` into a heartbeat-managed envelope.

        Use with messages obtained from :meth:`receive_messages_batch`.
        Returns a context manager identical to :meth:`receive_message`.
        """
        return self._wrap_message_cm(message, visibility_timeout, with_heartbeat)

    @asynccontextmanager
    async def _wrap_message_cm(
        self,
        message: QueueMessage,
        visibility_timeout: timedelta,
        with_heartbeat: bool,
    ) -> ty.AsyncGenerator[StorageQueueEnvelope, None]:
        cancel_heartbeat_event = asyncio.Event()
        heartbeat_cancelled_event = asyncio.Event()
        lock_lost_event = asyncio.Event()
        async with AsyncExitStack() as stack:
            if with_heartbeat:
                heartbeat_interval = visibility_timeout.total_seconds() / 2
                assert heartbeat_interval > 0
                await stack.enter_async_context(
                    self._heartbeat_worker(
                        message,
                        interval=heartbeat_interval,
                        visibility_timeout=visibility_timeout,
                        cancel_heartbeat_event=cancel_heartbeat_event,
                        heartbeat_cancelled_event=heartbeat_cancelled_event,
                        lock_lost_event=lock_lost_event,
                    )
                )
            else:
                heartbeat_cancelled_event.set()

            try:
                task = Task.deserialize(message["content"])
            except Exception:
                LOG.exception(
                    "Deserialization error in batch-received message — skipping.",
                )
                raise
            else:
                yield StorageQueueEnvelope(
                    message,
                    task,
                    self,
                    cancel_heartbeat_event,
                    heartbeat_cancelled_event,
                    lock_lost_event,
                )

    async def add_to_dead_letter_queue(self, task: Task, error: str | None = None) -> None:
        if self.dead_letter_queue_client is None:
            return
        task = replace_in_dataclass(task, error=error)
        LOG.debug("Adding task %r to dead letter queue %r", task.id, self.dead_letter_queue_name)
        await self.dead_letter_queue_client.send_message(task.serialize(), time_to_live=-1)

    async def delete(self, message: QueueMessage) -> None:
        assert self.queue_client is not None
        await self.queue_client.delete_message(message)

    async def create(self, exist_ok: bool = True) -> None:
        assert self.queue_client is not None
        try:
            await self.queue_client.create_queue()
        except azure.core.exceptions.ResourceExistsError:
            if not exist_ok:
                raise
        except azure.core.exceptions.HttpResponseError as e:
            if "invalid characters" in e.message:
                raise ValueError(
                    f"Invalid queue name '{self.queue_name}'. Only lowercase alphanumeric characters and hyphens are allowed."
                ) from e

        if self.dead_letter_queue_client is not None:
            try:
                await self.dead_letter_queue_client.create_queue()
            except azure.core.exceptions.ResourceExistsError:
                if not exist_ok:
                    raise

    async def clear(self) -> None:
        assert self.queue_client is not None
        await self.queue_client.clear_messages()

    async def __len__(self) -> int:
        assert self.queue_client is not None
        props = await self.queue_client.get_queue_properties()
        assert props.approximate_message_count is not None
        return props.approximate_message_count

    @property
    def name(self) -> str:
        assert self.queue_client is not None
        account_name = self.queue_client.account_name
        queue_name = self.queue_client.queue_name
        return f"{account_name}/{queue_name}"

    @asynccontextmanager
    async def _heartbeat_worker(
        self,
        message: QueueMessage,
        *,
        interval: float,
        visibility_timeout: timedelta = timedelta(hours=1),
        cancel_heartbeat_event: asyncio.Event,
        heartbeat_cancelled_event: asyncio.Event,
        lock_lost_event: asyncio.Event,
    ) -> ty.AsyncGenerator[None, None]:
        """Keeps a queue entry reserved by sending heartbeats.

        Args:
            message: The message to keep reserved.
            queue: The queue to send heartbeats to.
            interval: The interval at which to send heartbeats, in seconds.
            visibility_timeout: The message will stop being reserved if there is no heartbeat for this number of seconds.
        """

        async def _heartbeat() -> None:
            last_success = time.monotonic()
            lock_duration = visibility_timeout.total_seconds()
            lock_lost_logged = False
            try:
                # Wait *before* the first heartbeat.  Most messages
                # complete well within the visibility timeout (often in
                # tens of milliseconds), so an unconditional first
                # update_message call would be a wasted round-trip on
                # every receive — the message lease is already valid for
                # the full ``visibility_timeout`` from the moment we
                # received it.  We only need to renew if processing
                # actually approaches the lease horizon.  Sleeping
                # ``interval`` (= visibility_timeout / 2) here keeps the
                # original safety margin (one heartbeat per half-window)
                # without paying for messages that finish quickly.
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(cancel_heartbeat_event.wait(), interval)
                while not cancel_heartbeat_event.is_set():
                    pop_receipt = message.pop_receipt
                    try:
                        assert self.queue_client is not None

                        LOG.debug(
                            "Sending Heartbeat update for %s, %s",
                            message.id,
                            pop_receipt,
                        )
                        pop_receipt = (
                            await self.queue_client.update_message(
                                message,
                                pop_receipt=pop_receipt,
                                visibility_timeout=int(lock_duration),
                                timeout=round(interval),
                            )
                        ).pop_receipt
                        message.pop_receipt = pop_receipt
                        last_success = time.monotonic()
                        lock_lost_logged = False
                        LOG.debug(
                            "Received Heartbeat pop receipt for %s: %s",
                            message.id,
                            pop_receipt,
                        )
                    except azure.core.exceptions.HttpResponseError as e:
                        if e.status_code in (400, 404):
                            LOG.warning(
                                "Lock lost for message %s: heartbeat update failed with "
                                "status %d. Another worker may process this message.",
                                message.id,
                                e.status_code,
                            )
                            lock_lost_event.set()
                            return
                        LOG.exception(
                            f"Failed to send heartbeat for message {message.id}: ", exc_info=e
                        )
                    except Exception as e:
                        LOG.exception(
                            f"Failed to send heartbeat for message {message.id}: ", exc_info=e
                        )

                    # If no successful heartbeat within visibility_timeout, the
                    # message is visible again and another worker may pick it up.
                    elapsed = time.monotonic() - last_success
                    if elapsed > lock_duration and not lock_lost_logged:
                        expired_ago = elapsed - lock_duration
                        LOG.warning(
                            "Lock likely expired for message %s: no successful heartbeat "
                            "for %.0fs (visibility timeout %.0fs, expired ~%.0fs ago). "
                            "Another worker may process this message.",
                            message.id,
                            elapsed,
                            lock_duration,
                            expired_ago,
                        )
                        lock_lost_logged = True
                        lock_lost_event.set()
                        return

                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(cancel_heartbeat_event.wait(), interval)
            finally:
                # inform anyone waiting that we are done with the heartbeat.
                # Note that we cannot use task.cancel here, because it might interrupt the update_message call.
                # This in turn would mean that we have a stale pop_receipt; we would not be able to delete the message.
                heartbeat_cancelled_event.set()

        task = asyncio.create_task(_heartbeat(), name="heartbeat")

        yield

        cancel_heartbeat_event.set()
        await heartbeat_cancelled_event.wait()

        with suppress(asyncio.CancelledError):
            await task
        LOG.debug("Done with heartbeat of %s", message.id)

    async def peek(self, n: int = 1, as_json=False) -> list[QueueMessage]:
        assert self.queue_client is not None
        return await self.queue_client.peek_messages(n)

    async def get_result(self, session_id: str, timeout: timedelta | None = None) -> Response:
        raise NotImplementedError("get_result is not implemented for StorageQueueBackend")
