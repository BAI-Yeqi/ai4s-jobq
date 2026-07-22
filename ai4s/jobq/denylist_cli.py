# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""CLI for managing the image-SHA denylist (``ai4s-jobq denylist``).

Operators add/remove/list/check container-image digests that must not run.
The store is the shared Azure Table configured via :envvar:`JOBQ_DENYLIST_ACCOUNT`
(see :mod:`ai4s.jobq.denylist`).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import asyncclick as click

from ai4s.jobq.denylist import (
    VALID_SHUTDOWN_MODES,
    DenylistEntry,
    ImageDenylist,
    denylist_account,
    denylist_table_name,
    normalize_digest,
)

LOG = logging.getLogger("ai4s.jobq")


def _parse_effective(value: str) -> datetime | None:
    """Parse an ``--effective`` value into a UTC datetime, or ``None`` for now.

    Accepts an ISO-8601 date (``2026-07-29``) or datetime
    (``2026-07-29T00:00:00+00:00``), or the literal ``now`` for immediate
    effect. Naive values are interpreted as UTC.
    """
    text = value.strip()
    if not text or text.lower() == "now":
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise click.BadParameter(
            f"invalid --effective {value!r}; expected an ISO date/datetime or 'now'"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def _open_store() -> ImageDenylist:
    account = denylist_account()
    if account is None:
        raise click.ClickException(
            "No denylist configured. Set JOBQ_DENYLIST_ACCOUNT to the storage "
            "account (or connection string) hosting the denylist table."
        )
    return await ImageDenylist.from_account(account, table_name=denylist_table_name())


def _fmt_entry(entry: DenylistEntry) -> str:
    parts = [entry.digest, f"mode={entry.shutdown_mode}"]
    if entry.reason:
        parts.append(f"reason={entry.reason!r}")
    if entry.added_by:
        parts.append(f"by={entry.added_by}")
    if entry.added_at:
        parts.append(f"added={entry.added_at.isoformat()}")
    if entry.effective_at:
        marker = "" if entry.is_effective() else " (pending)"
        parts.append(f"effective={entry.effective_at.isoformat()}{marker}")
    return "  ".join(parts)


def _entry_dict(entry: DenylistEntry) -> dict:
    return {
        "digest": entry.digest,
        "shutdown_mode": entry.shutdown_mode,
        "reason": entry.reason,
        "added_by": entry.added_by,
        "added_at": entry.added_at.isoformat() if entry.added_at else None,
        "effective_at": entry.effective_at.isoformat() if entry.effective_at else None,
        "effective": entry.is_effective(),
    }


@click.group("denylist")
def denylist_group() -> None:
    """Manage the image-SHA denylist (workers running denied images are stopped).

    Accepts either the multi-arch manifest digest or an arch-specific child
    digest, in ``sha256:<hex>``, bare 64-hex, or ``ref@sha256:<hex>`` form.

    Configure the store with JOBQ_DENYLIST_ACCOUNT (storage account name or
    connection string) and, optionally, JOBQ_DENYLIST_TABLE.
    """


@denylist_group.command("add")
@click.argument("sha")
@click.option("--reason", default="", help="Why this image is denied.")
@click.option(
    "--added-by",
    default="",
    help="Who added the entry (audit). Defaults to the UPN/object id from your Azure token.",
)
@click.option(
    "--shutdown-mode",
    type=click.Choice(VALID_SHUTDOWN_MODES),
    default="graceful",
    show_default=True,
    help="How a matching running worker should stop.",
)
@click.option(
    "--effective",
    "effective",
    default="now",
    show_default=True,
    help="When the deny takes effect: an ISO date/datetime (e.g. 2026-07-29) or "
    "'now'. Until then the entry is listed but not enforced.",
)
async def denylist_add(
    sha: str, reason: str, added_by: str, shutdown_mode: str, effective: str
) -> None:
    """Add (or overwrite) a denied image digest."""
    effective_at = _parse_effective(effective)
    try:
        normalize_digest(sha)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc
    if not added_by:
        from ai4s.jobq.auth import caller_identity
        from ai4s.jobq.denylist import account_uses_aad

        account = denylist_account()
        if account and account_uses_aad(account):
            added_by = await caller_identity() or ""
    store = await _open_store()
    async with store:
        entry = await store.add(
            sha,
            reason=reason,
            added_by=added_by,
            shutdown_mode=shutdown_mode,
            effective_at=effective_at,
        )
    click.echo(f"Added: {_fmt_entry(entry)}")


@denylist_group.command("remove")
@click.argument("sha")
async def denylist_remove(sha: str) -> None:
    """Remove a denied image digest."""
    try:
        normalize_digest(sha)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc
    store = await _open_store()
    async with store:
        removed = await store.remove(sha)
    if removed:
        click.echo(f"Removed: {normalize_digest(sha)}")
    else:
        click.echo(f"Not on denylist: {normalize_digest(sha)}")


@denylist_group.command("list")
@click.option("--as-json", is_flag=True, help="Emit JSON instead of text.")
async def denylist_list(as_json: bool) -> None:
    """List denied image digests."""
    store = await _open_store()
    async with store:
        entries = await store.list_entries()
    if as_json:
        click.echo(json.dumps([_entry_dict(e) for e in entries], indent=2))
        return
    if not entries:
        click.echo("Denylist is empty.")
        return
    for entry in entries:
        click.echo(_fmt_entry(entry))


@denylist_group.command("check")
@click.argument("sha")
@click.option("--as-json", is_flag=True, help="Emit JSON instead of text.")
async def denylist_check(sha: str, as_json: bool) -> None:
    """Report whether a SHA is currently denied (exit 1 if denied)."""
    try:
        normalize_digest(sha)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc
    store = await _open_store()
    async with store:
        entry = await store.is_denied(sha)
    if as_json:
        click.echo(
            json.dumps(
                {
                    "digest": normalize_digest(sha),
                    "denied": entry is not None,
                    "entry": _entry_dict(entry) if entry else None,
                },
                indent=2,
            )
        )
    elif entry is not None:
        click.echo(f"DENIED: {_fmt_entry(entry)}")
    else:
        click.echo(f"Not denied: {normalize_digest(sha)}")
    if entry is not None:
        raise SystemExit(1)
