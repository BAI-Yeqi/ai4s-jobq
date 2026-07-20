# Race conditions and failure modes

The workflow engine inherits its delivery semantics from the
underlying jobq backend (Azure Storage Queues or Azure Service Bus).
If you have not already, skim the jobq
[basics page](basics.md) and the
[deduplication note](misc/40-deduplication.md)—they explain the
at-least-once guarantee that everything below builds on.

This document focuses on what the *workflow layer* adds on top of
jobq, and how it defends against the additional races that DAG state
introduces.

## Concurrency model in one paragraph

Per-workflow runtime state lives in a single JSON blob in Azure
Blob Storage at `{prefix}-workflows/{workflow_id}.json`. Every
mutation is a blob-ETag-guarded load-modify-store—concurrent
writers either serialise correctly or one of them re-reads and
retries. A separate `{prefix}WorkflowsIndex` Azure Table holds a
derived row per workflow for listings and cancel-request signalling;
it is *not* the source of truth. Exactly one coordinator process
writes the runtime blobs for a given prefix.

## Races and their mitigations

### Two completions arrive for the same workflow at the same time

The coordinator's completion loop receives messages in batches and
groups them by workflow. All completions for the same workflow are
applied sequentially to the in-memory runtime before a single flush.
This means sibling completions never race each other on the blob—they
are merged in memory and persisted with one write.

ETag conflicts can still occur when a periodic sweep (cancel-poll or
ready-repair) flushes the same workflow concurrently with the main
loop. In that case, one writer gets HTTP 412 (Precondition Failed),
re-reads the blob, and re-applies its changes. The apply step is
idempotent at `(task_name, attempt_no)` granularity, so the retry
converges. `--flush-retry-limit` (default 2) bounds the loop; if
exhausted, the completion messages are left on the queue for
redelivery.

### Coordinator crashes mid-completion

Completion messages are not acknowledged until the store succeeds.
A coordinator that crashes mid-processing leaves the message on the
queue; the next coordinator picks it up and reprocesses it. The
runtime blob's last durable version already reflects every previously
acknowledged completion, so reprocessing is a no-op once the target
task is terminal at the relevant `attempt_no`.

### Two coordinators run against the same prefix

Don't. The coordinator acquires a sentinel blob lease on startup
and will refuse to start if another coordinator already holds it.
If the previous coordinator crashed, the lease expires within 60
seconds (or use `workflow coordinator --break-lease` to force it).

If a lease is somehow bypassed—or the blob ETag CAS is the only
guard—two coordinators will fight over READY → RUNNING transitions,
push duplicate task messages, and burn ETag retries on every step.
The ETag CAS prevents silent data corruption, but behaviour under
dual-coordinator operation is undefined and unsupported.

### Worker self-applies, then crashes before publishing the completion

Workers do not write the runtime blob directly. The completion
message *is* the apply trigger: until it lands on the completion
queue, the runtime blob still shows the task as `running`. If the
worker crashes after running the user code but before posting the
completion, the task-queue message is unacked and reappears after
its visibility timeout; another worker re-runs the task. The
coordinator deduplicates duplicate completions via `attempt_no`.

If the output stash succeeds but the completion push fails (queue
outage), `_publish_completion()` retries with exponential back-off
(1 s → 60 s cap). The task-queue message is *not* deleted during
the retry loop—if the worker dies, the message reappears.
Completions are idempotent, so over-retry is harmless. A
configurable cap (`JOBQ_COMPLETION_SEND_MAX_ATTEMPTS`) bounds
retries; when hit, the method raises and the queue message is
redelivered.

### Worker dies before running the user code

The task message is still invisible (jobq has not acked it). After
the visibility timeout it reappears on the queue and the next worker
runs the task. Plain at-least-once jobq behaviour; the workflow
layer adds nothing here.

### Two workers race on the same task (duplicate delivery)

Azure Storage Queues (and Service Bus in PeekLock mode) provide
at-least-once delivery.  Core jobq mitigates duplicate delivery with
a **heartbeat**: while a worker holds a message, a background
coroutine periodically calls `update_message` to extend the
visibility timeout, keeping the message invisible to other workers
indefinitely.

However, the heartbeat is a best-effort mechanism.  It can fail to
renew in time if:

- **The process is killed without graceful shutdown** (SIGKILL, OOM,
  spot-node preemption).  No code runs, so no renewal happens and the
  message reappears after the visibility timeout.
- **A network partition or DNS failure** prevents the
  `update_message` call from reaching Azure for longer than the
  visibility timeout.
- **An event-loop freeze** (long GC pause, CPU starvation, blocking
  call on the main thread) prevents the heartbeat coroutine from
  being scheduled.
- **Azure returns 400/404** because the message was already
  dequeued—this happens when a prior renewal arrived too late and
  Azure already made the message visible.

In any of these cases a second worker receives the same message and
begins executing the task concurrently.

The **workflow layer** handles this with idempotent completion
application: `apply_completion` keys on `(task_name, attempt_no)`.
The first completion lands and transitions the task to a terminal
state; the second completion for the same attempt is dropped as a
no-op (the task is already terminal at that `attempt_no`).  The
coordinator never writes the runtime blob twice for the same event,
never pushes child tasks twice, and never double-counts completions.

### Task fails with retry budget remaining

The worker always reports the outcome—success or failure—and
returns. The coordinator inspects `num_retries` and the task's
`attempt_no`: if budget remains, it bumps `attempt_no` and pushes a
fresh task-queue message; otherwise it marks the task `failed` and
propagates `upstream_failed` to descendants. The worker is stateless
about retries—every delivery is one attempt.

### READY task is enqueued but its queue message is lost

If the coordinator pushed the task to the queue and the message was
lost (rare; queue-side bug or operator deletion), the task stays in
`READY` forever. The coordinator's ready-repair sweep
(`--ready-sweep-interval-s`, default 60 s) periodically scans
running workflows for READY tasks whose `updated_at` is older than
`--ready-repair-threshold-s` (default 300 s) and re-pushes them.
The re-push is idempotent because each task message carries
`(workflow_id, task_name, attempt_no)`; Service Bus dedups via
message ID, and a duplicate Storage-Queue message is deduped on the
worker via the `attempt_no` check before applying.

### Submit crashes between blob upload and index-row write

Submit writes the runtime blob first, then the index row. If the
client crashes after the blob upload but before the index row,
listings won't see the workflow but the blob is leaked. Re-submitting
with the same `workflow_id` overwrites the blob and writes the index
row, leaving no orphan. Truly orphaned blobs (no index row) are not
visible to any CLI command; the recommended recovery is to re-submit.

### Cancellation races task execution

`workflow cancel` flips the workflow's index-row `cancel_requested`
flag. The coordinator polls cancel-requested workflows
(`--cancel-poll-interval-s`, default 1 s) and transitions the
runtime blob to `cancelled`. In-flight workers poll the workflow's
cancel flag every `JOBQ_CANCEL_POLL_INTERVAL` seconds (default 30);
on observing `cancelled` they SIGTERM the subprocess and exit. Tasks
that had already become terminal before the cancellation stay
terminal—cancellation does not rewrite completed task entries.

### Unparseable (poison) completion message

If a message on the completion queue cannot be decoded into a
`WorkflowCompletion`—due to serialisation corruption, a client
pushing to the wrong queue, or a bug in an old worker version—the
coordinator logs a warning, atomically deletes the message, and
increments the `stats.poison_messages` counter.  No workflow state is
modified.  Poison messages are not retried.

Use `workflow doctor` to check for elevated `poison_messages` counts,
which may indicate a misconfigured worker or an incorrect queue name.

### Transient queue receive error

If the completion queue receive call raises an unexpected exception
(network timeout, DNS failure, Azure transient error), the error is
logged at `ERROR` level and the coordinator sleeps for `--idle-sleep`
seconds before retrying.  Any completions accumulated in the current
batch but not yet flushed are discarded; those messages reappear on
the queue after their visibility timeout and are processed normally on
the next delivery.  The coordinator never exits due to a receive
error.

## Scaling notes

The coordinator's hot path is the completion loop, and its limiting
factor is the blob write rate for the busiest workflow. The
deferred-flush design batches all completions for the same workflow
into a single `load → apply all → push ready → flush` cycle, so
wide fan-in/fan-out does **not** produce N-way ETag contention on
the same blob—completions are applied sequentially in memory and
persisted with one write.

ETag conflicts only arise when a periodic sweep (cancel-poll or
ready-repair) races with the main loop's flush for the same
workflow, which is rare in practice.

Two knobs help with throughput:

- **`--batch-size` / `JOBQ_COORDINATOR_BATCH_SIZE`** (default 32)
  pulls more completions per receive, amortising queue RTT. Larger
  batches also increase the chance that sibling completions for the
  same workflow land in the same cycle, yielding one flush instead
  of several.
- **`sequentialize_fan_in`** (`workflow submit --max-fan-in N`)
  rewrites high-fan-in subgraphs into sequential batches, limiting
  the number of in-flight tasks that can complete simultaneously.
  Use it when a single leaf depends on thousands of parents and the
  completion message volume exceeds the queue's receive throughput.

For end-to-end throughput on a one-workflow benchmark, the new design
sustains roughly 80 completions/second against Azurite (see
`tests/test_workflow_perf.py`).
