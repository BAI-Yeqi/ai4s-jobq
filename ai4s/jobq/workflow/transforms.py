# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""DAG transformations applied before workflow submission.

These transforms restructure a :class:`WorkflowDefinition` to work
around Azure Table Storage limits or to improve coordinator throughput,
without changing the logical outcome of the workflow.
"""

from __future__ import annotations

from dataclasses import replace

from ai4s.jobq.workflow.entities import WorkflowDefinition, WorkflowTask


def sequentialize_fan_in(
    definition: WorkflowDefinition,
    *,
    max_fan_in: int = 4000,
) -> WorkflowDefinition:
    """Restructure large fan-ins into sequential batches.

    Any task *T* with more than *max_fan_in* parents is rewritten into
    a chain of batches separated by lightweight merge nodes::

        [P0..PN-1] → merge-0 → [PN..P2N-1] → merge-1 → ... → T

    where ``N = max_fan_in``.  After the rewrite, every task in the
    DAG (including the merge nodes and *T* itself) has at most
    ``max_fan_in`` direct parents.  The merge nodes are dummy tasks
    (``kwargs={"__batch_merge": True}``) that the
    ``WorkflowShellCommandProcessor`` auto-completes immediately;
    custom processors must opt in by returning success for any task
    whose kwargs contain ``__batch_merge``.

    **Why cap fan-in.**  The hard constraint is the **Azure Storage
    Queue 64 KiB message size limit**: when the coordinator pushes a
    downstream task it inlines
    ``__upstream_outputs_compact`` describing every parent's output
    location (see ``coordinator._push_ready_tasks`` +
    ``ai4s.jobq.workflow._compact_refs``).  In the cheapest path —
    parents whose output is stashed under the canonical name — each
    entry costs only the parent name plus a few bytes of JSON
    framing (~12 B for short names).  In the worst path — inline
    refs or non-canonical blob names — each entry can be 50-250 B
    depending on payload size.  At a few thousand parents with
    large inline values the message starts grazing the queue limit
    and pushes begin failing outright.  Capping fan-in keeps the
    message bounded.

    Two softer concerns also improve when fan-in is capped:

    * **Coordinator flush stall**: when *N* sibling completions land
      within a small window, the coordinator processes them in
      sequential per-workflow batches.  The total wall-time cost
      scales linearly with *N* and blocks other workflows behind the
      same coordinator instance.
    * **Worker input resolution**: a downstream task that calls
      ``get_real_upstream_tasks()`` + ``get_upstream_output()`` ends up
      fetching one blob per real parent.  Without batching/concurrency
      in user code that is *N* sequential GETs.

    Note that ETag-CAS conflicts on the runtime state blob are *not*
    a meaningful concern in the v1 single-coordinator deployment: the
    main loop is the sole writer of each workflow's blob and flushes
    are serialised per workflow, so wide fan-in does not trigger CAS
    thrashing.

    **Trade-off**: the transform serialises batches that were
    originally independent, bounding push-message size and per-flush
    work at the cost of wall-clock time.  Use it when individual
    tasks dwarf the per-batch boundary overhead (typically true for
    ML / scientific workloads where each task takes seconds-to-hours).

    **Picking a value.**  Inspect the maximum existing fan-in first
    (see :ref:`workflow-large-fan-in` in ``docs/workflows.md`` for a
    one-liner).  Typical recommendations:

    * ``--max-fan-in 100``: very safe; well below queue-message limit
      even with large blob-stash refs (URL ~250 B).
    * ``--max-fan-in 500``: still safe for typical small refs
      (~100 B).
    * ``--max-fan-in 1000+``: only when you know upstream outputs are
      tiny inline values and parent names are short.

    Args:
        definition: The original workflow definition.
        max_fan_in: Maximum number of dependencies per task.  Tasks
            with fewer dependencies are left unchanged.  The default
            (4000) preserves backward compatibility with code paths
            that already tolerated very wide fan-in; new callers
            should pick a value tuned to their expected output ref
            size (see "Picking a value" above).

    Returns:
        A new ``WorkflowDefinition`` with synthetic merge nodes
        inserted.  The original definition is not modified.
    """
    if max_fan_in < 2:
        raise ValueError("max_fan_in must be >= 2")

    task_map = {t.name: t for t in definition.tasks}

    # Collect tasks that need restructuring.
    large_fan_in_tasks = [t for t in definition.tasks if len(t.depends_on) > max_fan_in]
    if not large_fan_in_tasks:
        return definition

    # Track modifications: name → replacement WorkflowTask.
    modified: dict[str, WorkflowTask] = {}
    new_merge_tasks: list[WorkflowTask] = []

    for target in large_fan_in_tasks:
        parents = target.depends_on
        batches = [parents[i : i + max_fan_in] for i in range(0, len(parents), max_fan_in)]

        prev_merge: str | None = None
        for i, batch in enumerate(batches):
            # Tasks in batches after the first must wait for the
            # previous merge to complete before they can start.
            if prev_merge is not None:
                for parent_name in batch:
                    parent = modified.get(parent_name) or task_map.get(parent_name)
                    if parent is not None:
                        new_deps = [*parent.depends_on, prev_merge]
                        modified[parent_name] = replace(parent, depends_on=new_deps)

            merge_name = f"__merge_{target.name}_{i}"
            # Each merge node depends on its batch AND the previous merge
            # node.  The merge→merge edge is redundant for scheduling (the
            # batch roots already wait for prev_merge) but enables efficient
            # transitive-closure walks through dummy nodes only.
            merge_deps = list(batch)
            if prev_merge is not None:
                merge_deps.append(prev_merge)

            new_merge_tasks.append(
                WorkflowTask(
                    name=merge_name,
                    kwargs={"__batch_merge": True, "__target": target.name, "__batch": i},
                    depends_on=merge_deps,
                )
            )
            prev_merge = merge_name

        # The original fan-in task now depends only on the last merge.
        modified[target.name] = replace(
            target, depends_on=[prev_merge] if prev_merge else target.depends_on
        )

    # Assemble the final task list preserving original order, with
    # merge nodes appended at the end.
    final_tasks = [modified.get(t.name, t) for t in definition.tasks]
    final_tasks.extend(new_merge_tasks)

    return replace(definition, tasks=final_tasks)
