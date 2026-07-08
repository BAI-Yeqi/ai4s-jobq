# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""File-stash markers for ``set_output``.

Tasks can emit *file* outputs (model checkpoints, parquet, tarballs, …) by
wrapping a local path in :class:`BlobStasher`::

    from ai4s.jobq.workflow import BlobStasher
    from ai4s.jobq.workflow.context import set_output

    set_output({"results": BlobStasher.from_file("foo.pt")})

The user script returns immediately — no I/O happens at construction
time. The worker that ran the script walks the JSON output after the
script exits, uploads each pending file to
``{JOBQ_WORKFLOW_BLOBS container}/workflow-files/{wf}/{task}/{filename}``
and rewrites the marker to point at the uploaded blob.

Downstream tasks see a :class:`BlobStash` instance and choose when to
materialise it::

    out = get_upstream_output("featurize")
    out["results"].download_to("local/foo.pt")

Wire format (JSON sentinel, versioned):

* Pending (set_output → worker):
  ``{"__jobq_stash__": {"v": 1, "state": "pending",
                         "local_path": "/abs/path/foo.pt",
                         "filename": "foo.pt"}}``
* Ready (worker → downstream):
  ``{"__jobq_stash__": {"v": 1, "state": "ready",
                         "blob_name": "workflow-files/wf/task/foo.pt",
                         "md5": "...", "size": 12345}}``
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai4s.jobq.blob import BlobStash

# JSON sentinel marker; bumped if the wire format changes incompatibly.
STASH_MARKER = "__jobq_stash__"
STASH_VERSION = 1


@dataclass(frozen=True)
class _PendingStash:
    """Marker for a file that the worker should upload after the script exits.

    Created via :meth:`BlobStasher.from_file`. Not meant to be constructed
    directly by users.
    """

    local_path: str
    filename: str

    def to_marker(self) -> dict[str, Any]:
        return {
            STASH_MARKER: {
                "v": STASH_VERSION,
                "state": "pending",
                "local_path": self.local_path,
                "filename": self.filename,
            }
        }


class BlobStasher:
    """Factory for file-stash markers used inside :func:`set_output`.

    See module docstring for the full flow.
    """

    @staticmethod
    def from_file(path: str | os.PathLike[str]) -> _PendingStash:
        """Mark *path* as a file output to upload after the task completes.

        Resolves *path* to an absolute path immediately so the worker
        (which may run in a different cwd) can find the file. The file
        is **not** opened or read at this point — only when the worker
        materialises the marker after the user script exits.

        Args:
            path: Path to a local file. Must exist when the worker runs
                ``_materialise_pending_stashes`` (typically a few moments
                after the script returns from ``set_output``).
        """
        abs_path = Path(os.fspath(path)).resolve()
        return _PendingStash(local_path=str(abs_path), filename=abs_path.name)


# ---------------------------------------------------------------------------
# JSON encoder / decoder hooks
# ---------------------------------------------------------------------------


def stash_json_default(obj: Any) -> Any:
    """``json.dumps(default=...)`` hook used by :func:`set_output`.

    Recognises :class:`_PendingStash` (and :class:`BlobStash`, in case a
    user round-trips one) and emits the appropriate sentinel marker.
    """
    if isinstance(obj, _PendingStash):
        return obj.to_marker()
    if isinstance(obj, BlobStash):
        return obj.to_marker()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON-serialisable")


def stash_object_hook(obj: dict[str, Any]) -> Any:
    """``json.loads(object_hook=...)`` hook used downstream.

    Recognises ``__jobq_stash__`` markers and returns :class:`BlobStash`
    instances for the "ready" form. "Pending" markers should never reach
    a downstream task — if one does, raise an explicit error so the user
    sees the bug rather than a confusing dict.
    """
    if STASH_MARKER not in obj or len(obj) != 1:
        return obj
    marker = obj[STASH_MARKER]
    if not isinstance(marker, dict):
        return obj
    version = marker.get("v")
    if version != STASH_VERSION:
        raise ValueError(
            f"Unsupported {STASH_MARKER} version {version!r}; expected {STASH_VERSION}"
        )
    state = marker.get("state")
    if state == "ready":
        try:
            return BlobStash(
                blob_name=marker["blob_name"],
                md5=marker["md5"],
                size=int(marker["size"]),
            )
        except KeyError as exc:
            raise ValueError(f"{STASH_MARKER} ready marker missing field: {exc.args[0]}") from None
    if state == "pending":
        raise ValueError(
            f"{STASH_MARKER} pending marker leaked downstream "
            f"(local_path={marker.get('local_path')!r}); "
            "the worker did not materialise it before sending the completion."
        )
    raise ValueError(f"Unknown {STASH_MARKER} state: {state!r}")


__all__ = [
    "STASH_MARKER",
    "STASH_VERSION",
    "BlobStash",
    "BlobStasher",
    "_PendingStash",
    "stash_json_default",
    "stash_object_hook",
]
