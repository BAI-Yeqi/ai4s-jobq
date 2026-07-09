# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from __future__ import annotations

import asyncio
import os
import time
from logging import getLogger
from typing import Any

from azure.core.credentials import (
    AccessToken,  # noqa: TC002 — used at runtime
    TokenCredential,  # noqa: TC002 — used at runtime
)
from azure.core.credentials_async import AsyncTokenCredential

LOG = getLogger(__name__)


class _TokenCachingCredential(AsyncTokenCredential):
    """Wraps an AsyncTokenCredential and caches tokens in-process.

    ``DefaultAzureCredential`` (especially via ``AzureCliCredential``)
    forks ``az account get-access-token`` on every ``get_token()`` call.
    When many Azure SDK clients (Table, Blob, Service Bus) share the
    same credential, this causes a storm of ``az`` invocations.

    This wrapper caches the ``AccessToken`` per scope and refreshes
    60 seconds before expiry — one ``az`` call instead of dozens.
    A lock serializes refresh so concurrent callers don't all fork
    ``az`` simultaneously.
    """

    def __init__(self, inner: AsyncTokenCredential) -> None:
        self._inner = inner
        self._cache: dict[tuple[str, ...], AccessToken] = {}
        self._lock = asyncio.Lock()

    async def get_token(
        self,
        *scopes: str,
        claims: str | None = None,
        tenant_id: str | None = None,
        enable_cae: bool = False,
        **kwargs: Any,
    ) -> AccessToken:
        key = scopes
        # Fast path: token is still valid — no lock needed.
        cached = self._cache.get(key)
        if cached is not None and time.time() < cached.expires_on - 60:
            return cached
        # Slow path: acquire lock, re-check, then refresh.
        async with self._lock:
            cached = self._cache.get(key)
            if cached is not None and time.time() < cached.expires_on - 60:
                return cached
            token = await self._inner.get_token(
                *scopes,
                claims=claims,
                tenant_id=tenant_id,
                enable_cae=enable_cae,
                **kwargs,
            )
            self._cache[key] = token
            return token

    async def close(self) -> None:
        close = getattr(self._inner, "close", None)
        if close is not None:
            await close()

    async def __aenter__(self) -> _TokenCachingCredential:
        enter = getattr(self._inner, "__aenter__", None)
        if enter is not None:
            await enter()
        return self

    async def __aexit__(self, *args: object) -> None:
        exit_ = getattr(self._inner, "__aexit__", None)
        if exit_ is not None:
            await exit_(*args)


def get_token_credential() -> AsyncTokenCredential:
    """Create a token credential for async use.

    Returns a **new instance** each time so that each call site can manage its
    own lifecycle independently via ``async with``.  Sharing a single credential
    across multiple async-context-manager scopes is unsafe because the first
    scope to exit will close the HTTP transport for all others.

    Resolution order:

    * ``AI4S_JOBQ_AUTH=default`` → :class:`DefaultAzureCredential` with
      ``exclude_cli_credential=True`` (opt in when you specifically want
      the full credential chain without the ``az`` CLI).
    * ``DEFAULT_IDENTITY_CLIENT_ID`` set → user-assigned
      :class:`ManagedIdentityCredential` (eg an AML compute target).
    * Otherwise → :class:`AzureCliCredential` (the default; works with a
      local ``az login`` session).

    Long-lived callers should ``async with`` the returned credential so
    its internal aiohttp session is released cleanly — otherwise
    Python's GC will print ``Unclosed client session`` warnings on
    interpreter exit.

    All returned credentials are wrapped in :class:`_TokenCachingCredential`
    to avoid redundant ``az`` CLI / IMDS calls when multiple Azure SDK
    clients share the same credential.
    """
    from azure.identity.aio import (
        AzureCliCredential,
        DefaultAzureCredential,
        ManagedIdentityCredential,
    )

    auth_mode = os.environ.get("AI4S_JOBQ_AUTH", "").strip().lower()
    if auth_mode == "default":
        LOG.info(
            "Authenticating with DefaultAzureCredential(exclude_cli_credential=True) "
            "(AI4S_JOBQ_AUTH=default)"
        )
        return _TokenCachingCredential(DefaultAzureCredential(exclude_cli_credential=True))
    if "DEFAULT_IDENTITY_CLIENT_ID" in os.environ:
        LOG.info("Authenticating with ManagedIdentityCredential()")
        return _TokenCachingCredential(
            ManagedIdentityCredential(client_id=os.environ["DEFAULT_IDENTITY_CLIENT_ID"])
        )
    LOG.info("Authenticating with AzureCliCredential()")
    return _TokenCachingCredential(AzureCliCredential())


def get_sync_token_credential() -> TokenCredential:
    """Create a token credential for sync use.

    Returns a **new instance** each time — see :func:`get_token_credential` for
    the rationale.

    See :func:`get_token_credential` for the resolution order; this
    sync variant honours the same ``AI4S_JOBQ_AUTH`` and
    ``DEFAULT_IDENTITY_CLIENT_ID`` environment variables.
    """
    from azure.identity import (
        AzureCliCredential,
        DefaultAzureCredential,
        ManagedIdentityCredential,
    )

    auth_mode = os.environ.get("AI4S_JOBQ_AUTH", "").strip().lower()
    if auth_mode == "default":
        LOG.info(
            "Authenticating with DefaultAzureCredential(exclude_cli_credential=True) "
            "(AI4S_JOBQ_AUTH=default)"
        )
        return DefaultAzureCredential(exclude_cli_credential=True)
    if "DEFAULT_IDENTITY_CLIENT_ID" in os.environ:
        LOG.info("Authenticating with ManagedIdentityCredential()")
        return ManagedIdentityCredential(client_id=os.environ["DEFAULT_IDENTITY_CLIENT_ID"])
    LOG.info("Authenticating with AzureCliCredential()")
    return AzureCliCredential()


async def close_cached_credentials() -> None:
    """No-op retained for backward compatibility.

    :func:`get_token_credential` no longer caches a process-wide
    credential — it returns a fresh instance each call so each
    ``async with`` scope owns its own lifecycle.  There is therefore
    nothing to close here.  Callers should close the credential they
    obtained (via ``async with`` or ``await cred.close()``) instead.
    """
    return
