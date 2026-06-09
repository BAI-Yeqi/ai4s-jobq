# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import os
from logging import getLogger

from azure.core.credentials import TokenCredential
from azure.core.credentials_async import AsyncTokenCredential

LOG = getLogger(__name__)


def get_token_credential() -> AsyncTokenCredential:
    """Create a token credential for async use.

    Returns a **new instance** each time so that each call site can manage its
    own lifecycle independently via ``async with``.  Sharing a single credential
    across multiple async-context-manager scopes is unsafe because the first
    scope to exit will close the HTTP transport for all others.

    We avoid using DefaultAzureCredential, since on sandboxes, this picks up the managed identity.
    We check whether DEFAULT_IDENTITY_CLIENT_ID is set, in which case this is
    likely a user-assigned managed identity, eg of an aml cluster.

    Finally, we return the AzureCliCredential.
    """
    from azure.identity.aio import (
        AzureCliCredential,
        ManagedIdentityCredential,
    )

    if "DEFAULT_IDENTITY_CLIENT_ID" in os.environ:
        LOG.info("Authenticating with ManagedIdentityCredential()")
        return ManagedIdentityCredential(client_id=os.environ["DEFAULT_IDENTITY_CLIENT_ID"])
    LOG.info("Authenticating with AzureCliCredential()")
    return AzureCliCredential()


def get_sync_token_credential() -> TokenCredential:
    """Create a token credential for sync use.

    Returns a **new instance** each time — see :func:`get_token_credential` for
    the rationale.

    We avoid using DefaultAzureCredential, since on sandboxes, this picks up the managed identity.
    We check whether DEFAULT_IDENTITY_CLIENT_ID is set, in which case this is
    likely a user-assigned managed identity, eg of an aml cluster.

    Finally, we return the AzureCliCredential.
    """
    from azure.identity import (
        AzureCliCredential,
        ManagedIdentityCredential,
    )

    if "DEFAULT_IDENTITY_CLIENT_ID" in os.environ:
        LOG.info("Authenticating with ManagedIdentityCredential()")
        return ManagedIdentityCredential(client_id=os.environ["DEFAULT_IDENTITY_CLIENT_ID"])
    LOG.info("Authenticating with AzureCliCredential()")
    return AzureCliCredential()
