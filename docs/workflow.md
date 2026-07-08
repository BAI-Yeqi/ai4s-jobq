# Workflows

ai4s-jobq workflows let you compose multi-step pipelines with task
dependencies, fan-out/fan-in, retries, and per-task queue routing
(for example, CPU prep → GPU train → CPU evaluate).

New to workflows? Start with the hands-on
[Workflow tutorial](workflow-tutorial.md), which walks through a
complete pipeline end-to-end against the local Azure Storage emulator.

## How it works

A workflow execution has four moving parts: a **client** (you),
**Azure Storage** (persistence: one Table for the index, Blob Storage
for the per-workflow runtime state and large task outputs), one or
more **task queues** (work distribution), a **completion queue** (event
stream), and a single **coordinator** process per prefix. Here is the
steady-state loop:

```text
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  1. SUBMIT          2. ACTIVATE         3. EXECUTE              │
│  ───────────────    ───────────────     ───────────────         │
│  Client writes      Coordinator picks   Workers pull from       │
│  one index row +    up the new state    task queues and         │
│  one runtime-state  blob, pushes root   run the command.        │
│  blob.  Root tasks  tasks to their                              │
│  start as READY.    target queues.                              │
│                                                                 │
│  4. REPORT          5. ADVANCE                                  │
│  ───────────────    ───────────────                             │
│  Worker writes its  Coordinator drains a                        │
│  output blob (if    batch of completions,                       │
│  large), then       groups by workflow,                         │
│  posts a            applies them to an                          │
│  completion         in-memory snapshot,                         │
│  message to the     pushes newly-ready                          │
│  completion queue.  children to their                           │
│                     queues, then writes                         │
│                     the runtime blob once                       │
│                     under ETag CAS.                             │
│                     → back to step 3.                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key design choices:**

- **One state blob per workflow.** The full per-task runtime state—
  states, attempts, dep counters, output refs—lives in a single
  JSON blob at `{prefix}-workflows/{workflow_id}.json`. Every mutation
  is an [ETag-guarded](#two-defense-mechanisms) read-modify-write
  against that blob, so the coordinator never needs multi-row
  transactions.
- **Single coordinator per prefix.** Exactly one coordinator process
  owns the state blobs for a prefix. The coordinator acquires a
  sentinel blob lease on startup—a second coordinator that starts
  against the same prefix will be rejected. Run two coordinators and
  you will see lease conflicts or, if bypassed, ETag conflicts and
  undefined behaviour.
- **One small index Table row per workflow.** The
  `{prefix}WorkflowsIndex` Azure Table stores indexable summary
  fields (status, name, terminal counts, timestamps). Clients use it
  for listing and filtering; the coordinator uses it for sweeping
  cancellation requests and stuck-ready workflows. The Table is *not*
  the source of truth—the runtime blob is.

  *Why a separate index?* A runtime-state blob for a 1 000-task
  workflow is roughly 300 KB compressed. Operations like `workflow
  list`, `workflow summary`, and `workflow cancel` need to touch every
  workflow or query by status—downloading hundreds of large blobs for
  a simple listing would be prohibitively slow and expensive. The
  index Table gives O(1) point-reads and cheap partition scans over
  lightweight rows (< 1 KB each), keeping control-plane latency
  independent of DAG size. The coordinator updates the index as a
  write-behind projection after each state-blob flush; because the
  blob is authoritative, a stale or missing index row never causes
  correctness issues—only slightly outdated listings.
- **Two queue kinds.** Task queues carry work (one per hardware
  class). The completion queue carries tiny event messages (one
  shared queue per prefix). Workers and coordinator never share a
  queue, so they scale independently.
- **Event-driven coordinator.** After the activation pass, the
  coordinator only reacts to completion messages and to a small set
  of periodic sweeps (cancel-poll, ready-repair). No tick-based DAG
  scans, no per-task polling.
- **Deferred-flush (in-memory snapshot).** The coordinator does not
  write the runtime blob after every completion. Instead it receives
  a batch of completions, groups them by workflow, and applies them
  all to an in-memory copy of the runtime state. Ready tasks are
  pushed to their queues immediately, but the blob is written only
  once at the end of the batch. This avoids wasted intermediate
  writes—especially in barrier DAGs where downstream tasks only
  unlock after all upstream completions land in the same cycle.
  Messages are acknowledged only after the flush succeeds; a crash
  before flush causes redelivery.
- **Crash-safe.** The coordinator is stateless. If it crashes,
  completion messages stay on the queue and the runtime blob keeps
  its last durable state. On restart it picks up where it left off.

## Two defense mechanisms

Crash safety rests on two primitives provided by Azure Storage:

**ETag CAS (Compare-and-Swap) on blobs.**
Every blob carries an ETag that changes on each write. The
coordinator always writes the runtime-state blob with an
`If-Match: <etag>` header—Azure rejects the write (HTTP 412) if
the blob changed since the last read. Two writers can never
silently overwrite each other; the loser detects the conflict and
re-reads. This is the single concurrency control mechanism for the
entire workflow state.

**Queue visibility timeout and redelivery.**
When a consumer reads a queue message, the message becomes
invisible for a configurable timeout—it is *not* deleted. Only an
explicit delete removes it permanently. If the consumer crashes
before deleting the message, it reappears after the timeout and
another consumer retries the work. This means any operation that
fails between "read" and "delete" is automatically retried by the
infrastructure with no manual intervention.

Together these two mechanisms guarantee that no state update is
lost (ETag CAS) and no work item is forgotten (redelivery), even
when processes crash at arbitrary points.

## Invariants

The architecture is small enough to state in a few invariants. A
correct implementation must preserve every one of these:

1. **The runtime blob is the source of truth.** Per-task states,
   dependency counters, attempt numbers, and output refs live there
   and nowhere else. The index Table is a derived projection.
2. **One writer.** Only the coordinator writes the runtime blob.
   Workers contribute by posting completion messages; they never
   touch the blob directly.
3. **[ETag CAS](#two-defense-mechanisms) guards every blob write.** A
   stale ETag forces a fresh read; no update is silently lost.
4. **Completions are idempotent.** Reprocessing the same completion
   message after a coordinator crash is a no-op once the target task
   is terminal.
5. **[At-least-once delivery](#two-defense-mechanisms) on every queue.**
   Task queues redeliver on worker death; the completion queue
   redelivers on coordinator crash. Both sides tolerate duplicates.
6. **Workflows reach a terminal state.** Provided the coordinator is
   running and completion messages are not permanently lost, every
   submitted workflow eventually settles into `completed`, `failed`,
   or `cancelled`.

## What can go wrong (and how it's handled)

Infrastructure fails. The system is designed to recover from crashes
at any point in the five-step loop above. Here is a quick overview of
the failure classes; for the full analysis—including heartbeat
mechanics, ETag-CAS contention, and duplicate-delivery
handling—see [Race conditions and failure modes](workflow-races.md).

**Submit.**
Client crashes between blob write and index-row write.
Re-submit with the same `workflow_id`; the deterministic blob path
makes it idempotent.

**Activate.**
Coordinator crashes after pushing root tasks but before persisting
READY → RUNNING.
Ready-repair sweep re-pushes stale READY tasks on restart.

**Execute.**
Worker killed (OOM, preemption) or queue message becomes visible to
a second worker.
[Message redelivers](#two-defense-mechanisms); heartbeat prevents
most duplicates; `attempt_no` deduplication handles the rest.

**Report.**
Worker crashes after running the command but before posting the
completion.
[Message redelivers](#two-defense-mechanisms); another worker
re-runs the task.

**Advance.**
Two completions for the same workflow race, or coordinator crashes
before acking the completion message.
[ETag-CAS](#two-defense-mechanisms) on the blob serializes
concurrent writers; unacked messages redeliver and
`apply_completion` is idempotent.

**Key assumption:** user tasks must be idempotent or tolerate
re-execution. This is fundamental to any at-least-once queue system.

## Task-level retries

Workflow tasks have a single retry budget, `num_retries`, set per task
in the YAML definition (default: `0`, meaning one attempt and no
retries). The number of allowed attempts is `num_retries + 1`.

**The coordinator—not the queue backend—owns retry logic.**
In base jobq (without workflows), retries rely on the queue backend's
message-update mechanism: on failure, the worker replaces the message
with a decremented retry counter. This only works on Azure Storage
Queues—Service Bus has no equivalent "update message in place"
operation.

The workflow layer moves retry orchestration out of the queue
entirely:

1. The coordinator always pushes task messages with
   `num_retries=0` at the jobq level, so the queue backend never
   retries on its own.
2. On a successful attempt the worker publishes a success completion.
   The coordinator marks the task `completed` and advances downstream
   children.
3. On a failed attempt the worker publishes a failure completion. The
   coordinator inspects the task's `attempt_no` against the
   workflow-level `num_retries`: if budget remains, it bumps
   `attempt_no` and pushes a fresh task message with a new
   deterministic ID; otherwise it marks the task `failed` and
   propagates `upstream_failed` to descendants.
4. The worker is stateless about retries: every delivery is just "run
   this task once and report the outcome."

This design works identically on both Storage Queues and Service
Bus, since it never depends on backend-specific message mutation.

## Further reading

This section collects the deeper reference material:

- [Workflow DAG support](workflows.md)—YAML schema, CLI operations,
  output passing, retries, queue routing, and architecture reference.
- [Race conditions and failure modes](workflow-races.md)—the
  correctness model and a catalog of every race the coordinator
  defends against. Worth reading before relying on workflows for
  production-critical pipelines.

```{toctree}
:hidden:
:maxdepth: 2

workflow-tutorial
workflows
workflow-races
```
