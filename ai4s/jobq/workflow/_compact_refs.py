# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Compact wire encoding for ``__upstream_output_refs`` queue payloads.

Background
----------

When the coordinator (or the client, during initial dispatch) pushes a
task that has parents, it embeds an ``{parent_name → output_ref}`` map
into the task's queue message so the worker can resolve each parent's
output without an extra blob round-trip on the hot path.

In the naive encoding each ref string carries a redundant
``blob:{workflow_id}/`` prefix and a ``.json`` suffix — both fixed for
the lifetime of the workflow.  At wide fan-ins (thousands of parents)
this redundancy is the dominant cost and can push the queue message
past Azure Storage Queue's ~48 KiB raw cap.

This module defines a compact wire form that drops the redundant
prefix/suffix and the ``blob:`` scheme for the *common case* (parents
whose output is stashed under the canonical name
``{workflow_id}/{parent_name}.json``).  The original
``{parent_name → output_ref}`` mapping is fully recovered at decode
time by reconstructing the canonical ref from the embedded workflow id
and the parent name.

Wire format
-----------

``encode`` returns a dict with up to three optional keys::

    {
        "s": ["pA", "pB", ...],          # canonical-blob parents
        "i": {"pC": "<json>", ...},      # non-canonical / inline refs
        "n": ["pD", ...],                # parents with no output (ref=None)
    }

A key is omitted entirely when its section is empty.  Parents not
present in any of ``s``/``i``/``n`` are treated by :func:`decode` as
``None``-ref parents (this matches the historical worker behaviour
when a parent name was missing from the upstream refs dict).

Sizing
------

For a 2000-parent task whose parents emit blob-stashed outputs
(the cse-mindless workload) the compact form weighs in at roughly
``len(parent_name) + 3`` bytes per entry (the JSON-quoted string in
``s``) — typically ~12 B per parent versus ~50 B per parent in the
naive encoding.  This is the difference between fitting inside the
queue cap and being rejected outright.
"""

from __future__ import annotations

from typing import Any

from ai4s.jobq.workflow.ids import output_ref_for_blob

COMPACT_KWARG = "__upstream_outputs_compact"
LEGACY_KWARG = "__upstream_output_refs"


def encode(workflow_id: str, refs: dict[str, str | None]) -> dict[str, Any]:
    """Compact ``{parent → output_ref}`` for embedding in a queue message.

    See module docstring for the wire format.  The compact dict
    omits empty sections to keep the message small in the common case.
    """
    stashed: list[str] = []
    inline: dict[str, str] = {}
    void: list[str] = []
    canonical_cache: dict[str, str] = {}
    for parent, ref in refs.items():
        if ref is None:
            void.append(parent)
            continue
        canonical = canonical_cache.get(parent)
        if canonical is None:
            canonical = output_ref_for_blob(workflow_id, parent)
            canonical_cache[parent] = canonical
        if ref == canonical:
            stashed.append(parent)
        else:
            inline[parent] = ref
    out: dict[str, Any] = {}
    if stashed:
        out["s"] = stashed
    if inline:
        out["i"] = inline
    if void:
        out["n"] = void
    return out


def decode(workflow_id: str, compact: dict[str, Any]) -> dict[str, str | None]:
    """Inverse of :func:`encode`.

    Parents listed in ``s`` get the canonical blob ref reconstructed
    from *workflow_id* and the parent name.  Parents in ``i`` keep
    their literal ref string.  Parents in ``n`` map to ``None``.
    Parents not present in any section are omitted from the output.
    """
    stashed = compact.get("s") or []
    inline = compact.get("i") or {}
    void = compact.get("n") or []
    refs: dict[str, str | None] = {p: output_ref_for_blob(workflow_id, p) for p in stashed}
    refs.update(inline)
    for parent in void:
        refs[parent] = None
    return refs


def pop_upstream_refs(workflow_id: str, kwargs: dict[str, Any]) -> dict[str, str | None]:
    """Extract upstream refs from *kwargs*, accepting either wire form.

    Consumes the compact (preferred) and the legacy keys from
    *kwargs* in place.  Returns the flat ``{parent → ref}`` dict the
    worker / context code expects.

    The compact form takes precedence if both keys are present, which
    should only happen during a rolling-deploy transition.
    """
    compact = kwargs.pop(COMPACT_KWARG, None)
    legacy = kwargs.pop(LEGACY_KWARG, None)
    if compact:
        return decode(workflow_id, compact)
    if legacy:
        return dict(legacy)
    return {}
