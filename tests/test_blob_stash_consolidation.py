# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Regression tests for the BlobStash consolidation.

There is a single canonical ``BlobStash`` class living in
``ai4s.jobq.blob``. The workflow-side module ``ai4s.jobq.workflow.stash``
re-exports it so the original public import paths still work, and
``BlobProcessor.stash_as_pickle/json`` returns instances of the same
class (with legacy ``filename``/``md5sum`` attribute access preserved).
"""

from __future__ import annotations

import ai4s.jobq.blob as blob_mod
import ai4s.jobq.workflow.stash as workflow_stash_mod
from ai4s.jobq.blob import BlobStash
from ai4s.jobq.workflow import BlobStash as WorkflowBlobStash
from ai4s.jobq.workflow.stash import BlobStash as WorkflowStashModBlobStash


def test_blob_stash_has_a_single_canonical_class() -> None:
    """All three public import paths must resolve to the same class object."""
    assert BlobStash is WorkflowBlobStash
    assert BlobStash is WorkflowStashModBlobStash
    assert BlobStash.__module__ == "ai4s.jobq.blob", (
        "BlobStash must live in ai4s.jobq.blob; workflow/stash.py re-exports it"
    )


def test_blob_stash_canonical_fields() -> None:
    """The canonical class carries the workflow-richer field set."""
    stash = BlobStash(blob_name="workflow-files/wf/task/foo.pt", md5="deadbeef", size=42)
    assert stash.blob_name == "workflow-files/wf/task/foo.pt"
    assert stash.md5 == "deadbeef"
    assert stash.size == 42
    # ``filename`` is the trailing path component (last "/" segment).
    assert stash.filename == "foo.pt"


def test_blob_stash_legacy_md5sum_alias() -> None:
    """``md5sum`` remains accessible for callers of the old BlobProcessor return type."""
    stash = BlobStash(blob_name="abc.pck", md5="cafef00d", size=10)
    assert stash.md5sum == "cafef00d", (
        "BlobProcessor users access blob_stash.md5sum; this attribute must remain"
    )
    assert stash.filename == "abc.pck"


def test_blob_stash_module_object_is_the_canonical_one() -> None:
    """Attribute lookup through both modules returns the same class."""
    assert blob_mod.BlobStash is workflow_stash_mod.BlobStash
