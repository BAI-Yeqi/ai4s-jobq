# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for the compact upstream-refs wire codec.

The codec compacts ``{parent_name → output_ref}`` so it fits inside
Azure Storage Queue's ~48 KiB message budget even at very wide
fan-ins (the cse-mindless DAG has a 2060-parent leaf).
"""

from __future__ import annotations

import json

from ai4s.jobq.workflow._compact_refs import (
    COMPACT_KWARG,
    LEGACY_KWARG,
    decode,
    encode,
    pop_upstream_refs,
)
from ai4s.jobq.workflow.ids import output_ref_for_blob


def test_encode_canonical_blob_refs_compact_to_s_list() -> None:
    workflow_id = "wf-1"
    refs = {
        "A": output_ref_for_blob(workflow_id, "A"),
        "B": output_ref_for_blob(workflow_id, "B"),
    }

    compact = encode(workflow_id, refs)

    assert compact == {"s": ["A", "B"]}


def test_encode_inline_refs_compact_to_i_dict() -> None:
    refs: dict[str, str | None] = {
        "A": '{"x": 1}',
        "B": "42",
    }

    compact = encode("wf-1", refs)

    assert compact == {"i": {"A": '{"x": 1}', "B": "42"}}


def test_encode_void_refs_compact_to_n_list() -> None:
    refs: dict[str, str | None] = {"A": None, "B": None}

    compact = encode("wf-1", refs)

    assert compact == {"n": ["A", "B"]}


def test_encode_mixed_refs() -> None:
    workflow_id = "wf-1"
    refs: dict[str, str | None] = {
        "canonical": output_ref_for_blob(workflow_id, "canonical"),
        "inline": '"hello"',
        "noncanonical-blob": "blob:other-workflow/X.json",
        "void": None,
    }

    compact = encode(workflow_id, refs)

    assert compact == {
        "s": ["canonical"],
        "i": {"inline": '"hello"', "noncanonical-blob": "blob:other-workflow/X.json"},
        "n": ["void"],
    }


def test_encode_omits_empty_sections() -> None:
    assert encode("wf-1", {}) == {}
    assert encode("wf-1", {"A": output_ref_for_blob("wf-1", "A")}) == {"s": ["A"]}


def test_decode_inverts_encode_for_canonical_refs() -> None:
    workflow_id = "wf-1"
    refs: dict[str, str | None] = {
        "A": output_ref_for_blob(workflow_id, "A"),
        "B": output_ref_for_blob(workflow_id, "B"),
    }

    assert decode(workflow_id, encode(workflow_id, refs)) == refs


def test_decode_inverts_encode_for_mixed_refs() -> None:
    workflow_id = "wf-1"
    refs: dict[str, str | None] = {
        "canonical": output_ref_for_blob(workflow_id, "canonical"),
        "inline": '{"x": 1}',
        "void": None,
    }

    assert decode(workflow_id, encode(workflow_id, refs)) == refs


def test_decode_handles_missing_sections() -> None:
    assert decode("wf-1", {}) == {}
    assert decode("wf-1", {"s": []}) == {}


def test_pop_prefers_compact_over_legacy() -> None:
    """If both keys are present (rolling deploy transition window),
    the compact form wins."""
    workflow_id = "wf-1"
    canonical = output_ref_for_blob(workflow_id, "A")
    kwargs = {
        COMPACT_KWARG: {"s": ["A"]},
        LEGACY_KWARG: {"A": "stale-legacy-ref"},
        "other": "kept",
    }

    refs = pop_upstream_refs(workflow_id, kwargs)

    assert refs == {"A": canonical}
    # Both wire keys must be consumed even if only one was used.
    assert kwargs == {"other": "kept"}


def test_pop_accepts_legacy_only() -> None:
    kwargs: dict[str, object] = {LEGACY_KWARG: {"A": '"hi"', "B": None}}

    refs = pop_upstream_refs("wf-1", kwargs)

    assert refs == {"A": '"hi"', "B": None}
    assert kwargs == {}


def test_pop_returns_empty_when_neither_key_present() -> None:
    kwargs: dict[str, object] = {"cmd": "run.sh"}
    assert pop_upstream_refs("wf-1", kwargs) == {}
    assert kwargs == {"cmd": "run.sh"}


def test_compact_form_fits_in_queue_message_for_wide_fan_in() -> None:
    """A 2060-parent canonical-stash fan-in (cse-mindless's widest leaf)
    must encode to well under Azure Storage Queue's ~48 KiB raw cap
    so the coordinator's ``jobq.push`` call succeeds.

    This is the primary motivation for the compact codec.
    """
    workflow_id = "cse-mindless-001"
    n_parents = 2060
    # Match the cse-mindless naming convention (~9 chars per parent).
    refs = {f"m-{i:07d}": output_ref_for_blob(workflow_id, f"m-{i:07d}") for i in range(n_parents)}

    compact = encode(workflow_id, refs)

    # Build the full task message body the coordinator would push.
    msg = {
        "__workflow_id": workflow_id,
        "__workflow_task": "sim-1-0000355",
        "__attempt_no": 0,
        COMPACT_KWARG: compact,
        "sleep_s": 0.1,
    }
    raw = json.dumps(msg).encode()

    azure_queue_raw_cap = 48 * 1024
    assert len(raw) < azure_queue_raw_cap, (
        f"compact form for {n_parents}-parent fan-in is {len(raw)} bytes, "
        f"over the ~48 KiB raw queue cap"
    )

    # Sanity: the legacy form would be far larger — confirm the win.
    legacy_msg = {**msg}
    del legacy_msg[COMPACT_KWARG]
    legacy_msg[LEGACY_KWARG] = refs
    legacy_raw = json.dumps(legacy_msg).encode()
    assert legacy_raw > raw, "expected compact form to be smaller than legacy"
