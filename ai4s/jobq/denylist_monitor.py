# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Worker-side denylist self-check.

A worker records its resolved image digest(s) in the
:data:`~ai4s.jobq.denylist.IMAGE_DIGEST_ENV` /
:data:`~ai4s.jobq.denylist.IMAGE_DIGEST_ARCH_ENV` environment variables (the
workforce injects these at hire time). :class:`DenylistEventHandler` polls
the shared denylist and, when it discovers the worker's own image is denied,
triggers a shutdown whose severity (``graceful`` vs ``hard``) comes from the
matching denylist row.

The handler is **fail-open** by default: an unconfigured or unreachable
denylist never shuts a worker down. The Singularity fail-closed hardening
(shut down when the store is persistently unreachable) is opt-in via
``shutdown_on_unreachable`` — used by the CLI when running where a reachable
denylist is mandatory.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import TYPE_CHECKING

from ai4s.jobq.denylist import (
    IMAGE_DIGEST_ARCH_ENV,
    IMAGE_DIGEST_ENV,
    DenylistEntry,
    ImageDenylist,
    denylist_disabled,
    denylist_poll_interval_s,
)

if TYPE_CHECKING:
    from collections.abc import Callable

LOG = logging.getLogger("ai4s.jobq")

_PROBE_DIGEST = "sha256:" + ("0" * 64)


class DenylistUnavailableError(RuntimeError):
    """Raised when a required denylist is missing or unreachable."""


async def assert_denylist_available() -> None:
    """Fail-closed startup guard for managed compute (e.g. Singularity).

    When :func:`~ai4s.jobq.denylist.denylist_require_configured` is True, the
    worker must refuse to start unless the denylist is both configured and
    reachable. Raises :class:`DenylistUnavailableError` otherwise. A no-op
    when the denylist is not required (the normal fail-open case).
    """
    from ai4s.jobq.denylist import denylist_account, denylist_require_configured

    if not denylist_require_configured():
        return
    if denylist_account() is None:
        raise DenylistUnavailableError(
            "Running on managed compute where the image-SHA denylist is "
            "mandatory, but JOBQ_DENYLIST_ACCOUNT is not set. Configure the "
            "denylist or set JOBQ_DENYLIST_REQUIRE=0 to opt out."
        )
    try:
        store = await ImageDenylist.open()
        if store is None:
            raise DenylistUnavailableError("denylist store could not be opened")
        async with store:
            # Point-read probe: exercises auth + connectivity. A missing row
            # is fine; only auth/network errors surface as failures.
            await store.is_denied(_PROBE_DIGEST)
    except DenylistUnavailableError:
        raise
    except Exception as exc:
        raise DenylistUnavailableError(
            f"image-SHA denylist is required here but unreachable: {exc}"
        ) from exc
    LOG.info("denylist preflight ok (required on this compute)")


def worker_image_digests() -> set[str]:
    """The current worker's own recorded image digests (may be empty)."""
    digests: set[str] = set()
    for key in (IMAGE_DIGEST_ENV, IMAGE_DIGEST_ARCH_ENV):
        value = os.environ.get(key, "").strip()
        if value:
            digests.add(value)
    return digests


class DenylistEventHandler:
    """Poll the denylist and shut the worker down when its image is denied.

    Args:
        on_denied: Called once with the matching :class:`DenylistEntry` when
            the worker's own image is found on the denylist. Should trigger
            the appropriate shutdown (inspect ``entry.shutdown_mode``).
        poll_interval_s: Seconds between checks. Defaults to
            :func:`~ai4s.jobq.denylist.denylist_poll_interval_s`.
        digests: The worker's own image digests. Defaults to
            :func:`worker_image_digests` (read from the environment).
        shutdown_on_unreachable: When True, repeated store failures beyond
            ``unreachable_grace_s`` trigger ``on_unreachable`` (fail-closed).
            Defaults to False (fail-open).
        unreachable_grace_s: How long the store may stay unreachable before
            ``on_unreachable`` fires. Defaults to ``max(poll*5, 300)``.
        on_unreachable: Called once when the store is persistently
            unreachable and ``shutdown_on_unreachable`` is True.
    """

    def __init__(
        self,
        on_denied: Callable[[DenylistEntry], None],
        *,
        poll_interval_s: float | None = None,
        digests: set[str] | None = None,
        shutdown_on_unreachable: bool = False,
        unreachable_grace_s: float | None = None,
        on_unreachable: Callable[[], None] | None = None,
    ) -> None:
        self._on_denied = on_denied
        self._poll_interval_s = (
            poll_interval_s if poll_interval_s is not None else denylist_poll_interval_s()
        )
        self._digests = digests if digests is not None else worker_image_digests()
        self._shutdown_on_unreachable = shutdown_on_unreachable
        self._unreachable_grace_s = (
            unreachable_grace_s
            if unreachable_grace_s is not None
            else max(self._poll_interval_s * 5, 300.0)
        )
        self._on_unreachable = on_unreachable
        self._task: asyncio.Task | None = None
        self._store: ImageDenylist | None = None
        self._fired = False

    async def __aenter__(self) -> DenylistEventHandler:
        if denylist_disabled():
            LOG.debug("denylist self-check disabled via JOBQ_DENYLIST_DISABLE")
            return self
        if not self._digests:
            LOG.debug(
                "denylist self-check inactive: worker has no %s/%s to check",
                IMAGE_DIGEST_ENV,
                IMAGE_DIGEST_ARCH_ENV,
            )
            return self
        try:
            self._store = await ImageDenylist.open()
        except Exception as exc:
            LOG.warning("denylist self-check store open failed: %s", exc)
            self._store = None
        if self._store is None and not self._shutdown_on_unreachable:
            LOG.debug("denylist self-check inactive: no store configured")
            return self
        self._task = asyncio.create_task(self._poll(), name="denylist-self-check")
        LOG.info(
            "denylist self-check started interval=%.0fs digests=%s",
            self._poll_interval_s,
            sorted(self._digests),
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._store is not None:
            with contextlib.suppress(Exception):
                await self._store.close()

    async def _poll(self) -> None:
        unreachable_since: float | None = None
        loop = asyncio.get_event_loop()
        try:
            while True:
                try:
                    if self._store is None:
                        # Fail-closed path: keep trying to (re)open.
                        self._store = await ImageDenylist.open()
                    if self._store is None:
                        raise RuntimeError("denylist store not configured")
                    entry = await self._store.is_denied(*self._digests)
                    unreachable_since = None
                    if entry is not None and not self._fired:
                        self._fired = True
                        LOG.warning(
                            "denylist_self_match digest=%s mode=%s reason=%s; shutting down",
                            entry.digest,
                            entry.shutdown_mode,
                            entry.reason,
                        )
                        self._on_denied(entry)
                        return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    now = loop.time()
                    if unreachable_since is None:
                        unreachable_since = now
                    LOG.warning("denylist_self_check_error: %s (fail-open)", exc)
                    if (
                        self._shutdown_on_unreachable
                        and self._on_unreachable is not None
                        and not self._fired
                        and (now - unreachable_since) >= self._unreachable_grace_s
                    ):
                        self._fired = True
                        LOG.error(
                            "denylist unreachable for %.0fs; shutting down (fail-closed)",
                            now - unreachable_since,
                        )
                        self._on_unreachable()
                        return
                await asyncio.sleep(self._poll_interval_s)
        except asyncio.CancelledError:
            LOG.debug("denylist self-check cancelled")
