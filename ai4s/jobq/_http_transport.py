# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Shared HTTP transport tuning for Azure SDK clients.

Both the storage-queue backend and the workflow Table store hit the
same Azure endpoint from a single process; the SDK's default
``aiohttp`` pool of 100 concurrent connections can become the
bottleneck on a busy coordinator running many in-flight ops.  This
module centralises the optional pool-size override so both client
types can pick it up without duplicating the env-var / transport
construction logic.
"""

from __future__ import annotations

import logging
import os
import typing as ty

LOG = logging.getLogger(__name__)


_POOL_SIZE_ENV_VAR = "JOBQ_HTTP_POOL_SIZE"


def transport_kwargs_for_pool_size(override: int | None = None) -> dict[str, ty.Any]:
    """Return ``{"transport": ...}`` when a non-default pool size is requested.

    *override* takes precedence over the ``JOBQ_HTTP_POOL_SIZE`` env
    var.  Returns ``{}`` when neither is set, so callers can splat the
    result into any Azure SDK client constructor:

    .. code-block:: python

        QueueClient(account_url, queue_name, **transport_kwargs_for_pool_size())

    The returned transport owns its session and is closed alongside
    the SDK client.
    """
    pool_size = override
    if pool_size is None:
        env_val = os.environ.get(_POOL_SIZE_ENV_VAR)
        if env_val:
            try:
                pool_size = int(env_val)
            except ValueError:
                LOG.warning(
                    "Ignoring invalid %s=%r (expected integer)",
                    _POOL_SIZE_ENV_VAR,
                    env_val,
                )
    if pool_size is None or pool_size <= 0:
        return {}

    # Local imports keep the optional aiohttp dependency lazy; modules
    # that never construct an Azure SDK client (e.g. CLI ``--help``
    # bootstraps) shouldn't have to pay the import cost.
    import aiohttp
    from azure.core.pipeline.transport import AioHttpTransport

    connector = aiohttp.TCPConnector(limit=pool_size)
    session = aiohttp.ClientSession(connector=connector)
    transport = AioHttpTransport(session=session, session_owner=True)
    return {"transport": transport}
