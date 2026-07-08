# Workflow DAG support

While jobq's core handles embarrassingly parallel tasks, **workflows** let
you define task dependencies—run A, then B and C in parallel, then D when
both finish.

Workflows store DAG state in Azure Blob Storage (one runtime-state
blob per workflow) plus a small index Table for listings, and use
either Azure Storage Queues or Azure Service Bus for task routing—both
backends are first-class. By default the queue backend lives on the
same storage account as `JOBQ_WORKFLOW_PREFIX`; set
`JOBQ_WORKFLOW_QUEUES=sb://<namespace>`
to switch to Service Bus. Install the optional extra:

```bash
pip install ai4s-jobq[workflow]
```

This page focuses on CLI-operated workflows, YAML definitions, and the
runtime architecture. For programmatic submission, custom processors,
and in-process Python helpers, see the [Workflow Python API](api.md#workflows).

## How the services interact

A workflow involves three Azure services. Each owns a distinct
responsibility:

| Service | Role | What it stores |
|---------|------|----------------|
| **Blob Storage** | Source of truth | One state blob per workflow at `{prefix}-workflows/{workflow_id}.json` holding the full runtime DAG (per-task states, dep counters, output refs). One container per prefix. Large task outputs (> 32 KB) go to `{prefix}-outputs/{wf}/{task}.json`. Every state-blob mutation is ETag-CAS guarded. |
| **Azure Table Storage** | Listings & control plane | One row per workflow in `{prefix}WorkflowsIndex`. Stores indexable summary fields (status, terminal counts, timestamps) plus a `cancel_requested` flag. Not authoritative—derived from the state blob. |
| **Task queues** (Storage Queues or Service Bus) | Work distribution | One queue per hardware class (for example, `gpu-a100`, `cpu-64core`). Workers pull from these—unchanged from regular jobq. |
| **Completion queue** (same backend as task queues) | Event stream | A per-prefix queue (`<prefix>-workflow-completions`, lower-cased). Workers post a message here when a task finishes. |

### Message flow for a diamond workflow

Consider four tasks—A → B, A → C, then D depends on both B and C.

```text
1. Submit
   WorkflowClient writes:
     • One runtime-state blob (workflow header + DAG, A=READY, B/C/D=PENDING)
     • One index row (status=pending)

2. Coordinator activates the workflow
   The coordinator's activation pass loads the runtime blob, finds A
   in READY, pushes A to queue:cpu, marks A RUNNING and the workflow
   status RUNNING, then writes the blob back under ETag CAS.

3. Worker executes A
   A worker pulls A from queue:cpu, runs it, stashes the output (if
   large) to {prefix}-outputs/{wf}/A.json, posts a WorkflowCompletion
   message to the completion queue, and acks the task-queue message.

4. Coordinator processes A's completion
   The coordinator pulls the completion, loads the runtime blob,
   applies (apply_completion): marks A=COMPLETED, increments B's and
   C's completed_deps, sees both satisfy dep_policy="all", marks
   them READY, pushes B to queue:gpu-a100 and C to queue:cpu-64core,
   marks them RUNNING, writes the blob back under ETag CAS, then
   acks the completion message.

5. Workers execute B and C in parallel
   Each worker self-applies its own result and posts its own
   completion message. The coordinator processes them independently
   (ETag CAS resolves any same-workflow concurrency).

6. Coordinator processes B's and C's completions
   For each completion, the coordinator increments D's completed_deps
   in the runtime blob. When D reaches completed_deps == 2 it
   satisfies dep_policy="all" and gets enqueued.

7. D completes → workflow done
   The coordinator marks the workflow COMPLETED in the runtime blob
   and updates the index row terminal counters.
```

### Coordinator crash safety

The coordinator is **stateless**—it reads the runtime blob and the
completion queue, makes decisions, and writes the blob back. If it
crashes:

- In-flight workers keep running (they only need the task queue).
- Completion messages accumulate in the queue (durable; pop receipts
  are not acked until the blob store succeeds).
- On restart, the coordinator picks up where it left off.
- ETag CAS on the runtime blob prevents lost updates if a previous
  coordinator partly processed a completion before the crash.

### One coordinator per prefix

Run **exactly one** coordinator process per `JOBQ_WORKFLOW_PREFIX` prefix.
There is no lease, no leader election, and no quorum: the design
assumes a single writer to each runtime blob. Two coordinators
against the same prefix will fight over READY → RUNNING transitions
and burn ETag retries on every step. For HA, deploy the coordinator
behind a single-replica scheduler (Kubernetes Deployment with
`replicas: 1` and `Recreate` strategy, systemd service, etc.) so a
new instance starts only after the previous one exits.

### How workers report completions

Workers are not aware of the DAG. The ``WorkflowShellCommandProcessor``
detects ``__workflow_id`` in the task kwargs and automatically posts a
structured ``WorkflowCompletion`` message to the completion queue after
the command finishes:

```python
# Sent automatically by WorkflowShellCommandProcessor
WorkflowCompletion(
    workflow_id="abc-123",
    task_name="featurize",
    success=True,
    attempt_no=1,
    output_ref='{"output": "abfs://data/features.parquet"}',
)
```

Workers that run non-workflow tasks behave exactly as before.

### Worker-side cancellation polling

For workflow tasks, the worker runtime spawns a background polling loop
that checks the workflow's index row every 30 seconds. If the workflow
is `cancelled` (or has `cancel_requested=true`), the poll sets a
cancellation event. The worker loop races this event against the
running callback—when it fires, the subprocess gets SIGTERM, identical
to the existing preemption and lock-lost behavior. This means
cancellation works automatically for shell commands without any code
changes.

### ETag-guarded state-blob writes

Every write to a workflow's runtime-state blob includes the ETag from
the preceding read. If another process modified the blob in between,
the write fails with a 412 (Precondition Failed) error and the
coordinator retries with a fresh read. This guarantees that concurrent
completion handlers for the same workflow never lose increments, even
when many completions arrive simultaneously for the same downstream
task.

## Task and workflow statuses

Every task and workflow carries a `status`. Knowing what
each status means makes `workflow status`, `workflow tasks`, and
`workflow doctor` output much easier to read.

### Task statuses

| Status | Meaning | Typical next state |
|--------|---------|---------------------|
| `pending` | Task is waiting on at least one dependency to finish. | `ready`, `upstream_failed`, or `skipped` |
| `ready` | Dependency policy is satisfied; the coordinator will enqueue this task as part of the next flush. | `running` |
| `running` | Task message is on its target queue or actively being processed by a worker. | `completed` or `failed` |
| `completed` | Worker reported success; output has been recorded. | terminal |
| `failed` | Worker reported failure and the task's retry budget (`num_retries`) is exhausted. | terminal (until `workflow retry` resets it) |
| `upstream_failed` | A dependency failed (or was cancelled) such that this task can no longer satisfy its `dep_policy`. The task never ran. | terminal (until `workflow retry` resets it) |
| `skipped` | The task's `condition` evaluated false, or every dependency was skipped. | terminal |
| `cancelled` | The workflow was cancelled before this task could run. | terminal |

Transient states (`pending`, `ready`, `running`) all advance under
the coordinator's control. Terminal states only change via
`workflow retry`, which moves `failed` and `upstream_failed` tasks
back to `ready`/`pending`.

### Workflow statuses

The workflow status aggregates over its tasks:

| Status | Meaning |
|--------|---------|
| `pending` | Newly submitted, or just had failed tasks reset. The coordinator picks these up and dispatches their ready tasks; the ready-repair sweep (see below) covers the submit-or-retry crash window. |
| `running` | At least one task has been enqueued; the workflow is making progress. |
| `completed` | Every task is `completed` or `skipped`. |
| `failed` | At least one task is `failed` or `upstream_failed`, and no task is still `pending` / `ready` / `running`. |
| `cancelled` | An operator called `workflow cancel`. Workers detect cancellation within `JOBQ_CANCEL_POLL_INTERVAL` (default 30&nbsp;s). |

`workflow doctor` flags `pending` workflows that have been waiting
for more than `--stale-pending-sec` (default 120&nbsp;s)—that's the
most common signal that no coordinator is running.

## Defining a workflow

A workflow is a directed acyclic graph (DAG) of tasks. Each task has a
name, keyword arguments, and an optional list of dependencies. YAML is
the recommended format for CLI-operated workflows:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/microsoft/ai4s-jobq/main/ai4s/jobq/workflow/data/workflow-definition.schema.json
name: featurize-and-train
default_queue: cpu-general
tasks:
  - name: featurize
    kwargs:
      input: abfs://data/raw.parquet
  - name: train-gnn
    kwargs: {model: schnet}
    depends_on: [featurize]
    queue: gpu-a100
  - name: train-rf
    kwargs: {model: random-forest}
    depends_on: [featurize]
    queue: cpu-64core
  - name: compare
    kwargs: {metric: mae}
    depends_on: [train-gnn, train-rf]
```

The leading `# yaml-language-server: $schema=…` directive opts the file
into autocompletion and inline validation in any editor that uses the
[YAML language server][yls] (VS Code, Neovim with `yaml-ls`, JetBrains
IDEs). The schema lives in the package at
`ai4s/jobq/workflow/data/workflow-definition.schema.json`; you can also
point the directive at a local copy if you'd rather not depend on
GitHub raw.

[yls]: https://github.com/redhat-developer/yaml-language-server

JSON files (`.json`) are also accepted; the schema and field names are
identical. The YAML language-server directive shown above is YAML-only,
so for `.json` files add a top-level `$schema` field instead:

```json
{"$schema": "https://raw.githubusercontent.com/microsoft/ai4s-jobq/main/ai4s/jobq/workflow/data/workflow-definition.schema.json"}
```

## Multi-queue routing

Each task can target a different Service Bus queue via the `queue` field.
Tasks without an explicit queue use `default_queue`. This lets you route
preprocessing to CPU workers, training to GPU workers, and evaluation
back to CPUs—all within one workflow.

Workers on each queue are unchanged—they pull tasks, execute them, and
report results. The coordinator handles routing.

### Queue naming and worker mapping

The queue name in the workflow definition (`queue:` or `default_queue:`)
maps directly to the queue name on the worker command line. They must
match exactly. For example, given:

```yaml
default_queue: cpu-general
tasks:
  - {name: train, queue: gpu-a100, ...}
```

you need at least one worker pulling from each queue:

```bash
ai4s-jobq <account>/cpu-general worker --idle-timeout 5m
ai4s-jobq <account>/gpu-a100   worker --idle-timeout 5m
```

The leading `<account>/` segment is the storage namespace (Azure
Storage account name or Service Bus namespace); the second segment is
the queue/topic name that must match the workflow definition.

A task whose `queue` does not match any running worker simply waits in
the queue until a worker for that queue starts, so the workflow makes
no progress on that branch—not an error, but easy to mistake for one.

## Task naming conventions

Use a consistent prefix to group related tasks within a workflow.
This convention enables efficient per-group queries because task names
are stored as Table Storage row keys, and prefix filters translate to
fast `RowKey` range queries within a partition.

```yaml
name: ml-pipeline
tasks:
  - name: prep-clean
    kwargs: {cmd: "python clean.py"}
  - name: prep-featurize
    kwargs: {cmd: "python featurize.py"}
  - name: train-gnn
    kwargs: {cmd: "python train.py --model schnet"}
    depends_on: [prep-clean, prep-featurize]
    queue: gpu-a100
  - name: train-rf
    kwargs: {cmd: "python train.py --model rf"}
    depends_on: [prep-clean, prep-featurize]
  - name: eval-compare
    kwargs: {cmd: "python compare.py"}
    depends_on: [train-gnn, train-rf]
```

Query by prefix from the CLI:

```bash
# All training tasks
ai4s-jobq workflow tasks --workflow abc123 --prefix train-

# Failed prep tasks
ai4s-jobq workflow tasks --workflow abc123 --prefix prep- --status failed
```

Programmatic inspection uses the same prefix filtering; see
[Submitting workflows from Python](api.md#submitting-workflows-from-python).

Prefix queries are efficient because they use `RowKey ge/lt` range
filters on Table Storage. No secondary indexes or extra rows are
needed.

## Workflow tasks for shell commands

Most workflow tasks invoke a shell command. The
``WorkflowShellCommandProcessor`` (auto-selected by the worker when
``JOBQ_WORKFLOW_PREFIX`` is set) recognises a small set of well-known
``kwargs`` keys:

| Key | Type | Description |
|-----|------|-------------|
| `cmd` | `str` (required) | The command line to execute. Run via the system shell. |
| `env` | `dict[str, str]` | Extra environment variables for the subprocess. |
| `cwd` | `str` | Working directory for the subprocess. |
| `bg_dirsync_to` | `str` | Background sync of a directory to remote storage during execution. |

Any other ``kwargs`` keys are simply ignored by the shell processor—
they are not passed to the command line. To send data into the
command, expand it into ``cmd`` (for example,
``cmd: "python train.py --epochs {epochs}"``) or set environment
variables via ``env``. To send structured data downstream from the
command, write it to ``$JOBQ_OUTPUT_FILE`` using
``set_output()`` (see "Passing data between tasks" below).

If you need full Python integration instead of shell commands, write
your own ``Processor`` subclass; see [Custom workflow processors](api.md#custom-workflow-processors).

## Submitting and inspecting

The workflow CLI is the recommended interface for operating YAML-defined
workflows. For programmatic submission and inspection, see
[Submitting workflows from Python](api.md#submitting-workflows-from-python).

### CLI

The workflow group accepts a positional ``STORAGE/PREFIX`` shorthand
(mirroring the main ``ai4s-jobq SERVICE/QUEUE`` form) so you can switch
between projects without re-exporting environment variables. Storage
and prefix are required; the positional wins when both are supplied.

```bash
# Two equivalent ways to configure storage + prefix
ai4s-jobq workflow myaccount/MyProject submit pipeline.yaml
JOBQ_WORKFLOW_PREFIX=myaccount/MyProject ai4s-jobq workflow submit pipeline.yaml

# Common operations (assuming JOBQ_WORKFLOW_PREFIX is set)
ai4s-jobq workflow submit pipeline.yaml          # submit a single workflow
ai4s-jobq workflow validate pipeline.yaml        # CI-friendly; never hits the store
ai4s-jobq workflow validate workflows/*.yaml

# Submit with a custom ID
ai4s-jobq workflow submit pipeline.yaml --id my-run-001

# Large fan-in: restructure DAGs with more than 100 deps per task
ai4s-jobq workflow submit sweep.yaml --max-fan-in 100

# Batch submit—pass multiple files or pipe from stdin
ai4s-jobq workflow submit wf-*.yaml
find workflows/ -name '*.yaml' | ai4s-jobq workflow submit

# Inspect workflows and tasks
ai4s-jobq workflow status abc123
ai4s-jobq workflow list --status running
ai4s-jobq workflow tasks --status failed
ai4s-jobq workflow tasks --queue gpu-a100 --status running

# Retry failed and upstream-failed tasks (after fixing the cause)
ai4s-jobq workflow retry abc123

# Switch projects on the fly via the positional shorthand
ai4s-jobq workflow myaccount/Experiments list
ai4s-jobq workflow myaccount/Production list
```

Connection strings and full URLs cannot be passed via the positional
shorthand (they contain ``/``). Production deployments use a bare
account name plus DefaultAzureCredential, which the positional handles
directly.

When submitting multiple files, workflow IDs are printed to stdout (one
per line) for piping. A progress bar is shown on stderr when running
interactively. Use ``--concurrency`` to control how many submissions run
in parallel (default: 20).

## Dependency policies

By default, a task runs only when **all** its dependencies succeed
(`dep_policy: all`). You can relax this in YAML:

```yaml
tasks:
  - name: compare
    depends_on: [train-gnn, train-rf, train-linear]
    dep_policy: any      # run as soon as at least one model finishes

  - name: ensemble
    depends_on: [model-a, model-b, model-c, model-d]
    dep_policy: 2        # run when at least 2 models succeed
```

| Policy | Runs when | Fails when |
|--------|-----------|------------|
| `"all"` (default) | Every dependency completes | Any dependency fails |
| `"any"` | At least one dependency completes | All dependencies fail |
| `N` (int) | At least N dependencies complete | More than `total - N` fail |

When a task has `dep_policy: any` or an integer policy, scripts can
use `get_upstream_outputs()` to discover which dependencies actually
succeeded; see [Producing outputs from Python tasks](api.md#producing-outputs-from-python-tasks).

## Conditional downstream execution

A task can declare a **condition**—a Python-like expression evaluated
against upstream task outputs. If the condition is false, the task is
marked ``skipped`` instead of running. Skipped tasks count as completed
for dependency resolution, so downstream tasks still proceed.

### Example: deploy only if accuracy is good enough

```yaml
name: ml-pipeline
tasks:
  - name: train
    kwargs: {epochs: 50}
  - name: deploy
    depends_on: [train]
    condition: inputs.train.mae < 0.1
```

The ``deploy`` task runs only if ``train`` produced an output with
``mae < 0.1``. Otherwise it is marked ``skipped``.

### Multi-input conditions

Conditions can reference outputs from multiple upstream tasks:

```yaml
name: ensemble
tasks:
  - name: train-rf
    kwargs: {model: rf}
  - name: train-gnn
    kwargs: {model: gnn}
  - name: deploy
    depends_on: [train-rf, train-gnn]
    condition: inputs["train-rf"].mae < 0.1 and inputs["train-gnn"].mae < 0.1
```

Use dot access for simple names (``inputs.train.mae``) and bracket
access for names containing special characters
(``inputs["prep/featurize"].passed``).

### Expression syntax

Conditions use a restricted Python-like syntax:

| Element | Examples |
|---|---|
| Comparisons | ``==  !=  >  >=  <  <=  in  not in`` |
| Boolean operators | ``and  or  not`` |
| Literals | ``42``, ``3.14``, ``"hello"``, ``True``, ``False``, ``None`` |
| Input references | ``inputs.task_name.field``, ``inputs["task/name"].field`` |
| Nested fields | ``inputs.train.metrics.mae`` |
| Aggregate functions | ``all``, ``any``, ``almost_all``, ``count`` |

Aliases ``true``/``false``/``null`` are accepted for
``True``/``False``/``None``.

Arbitrary code (non-aggregate function calls, imports, comprehensions)
is rejected at submission time.

### Aggregate functions

When a workflow has many similar upstream tasks, aggregate functions
let you write conditions that span all (or a subset of) inputs using
glob patterns:

```yaml
- name: deploy
  depends_on: [train-a, train-b, train-c]
  condition: all(inputs["train-*"].loss < 0.01)
```

Use ``inputs["*"]`` to match all inputs, or ``inputs["pattern"]`` with
standard glob characters (``*``, ``?``, ``[...]``). The shorthand
``inputs.*`` is converted to ``inputs["*"]`` automatically.

| Function | Meaning |
|---|---|
| ``all(condition)`` | True if *condition* holds for every matching input |
| ``any(condition)`` | True if *condition* holds for at least one matching input |
| ``count(condition)`` | Number of matching inputs where *condition* is true (use in comparisons) |
| ``almost_all(condition, N)`` | True if at most *N* matching inputs fail *condition* |

Examples:

```text
all(inputs.*.loss < 0.01)
any(inputs["train-*"].converged)
count(inputs["*"].passed) >= 3
almost_all(inputs["worker-*"].loss < 0.1, 5)
all(inputs["train-*"].loss < 0.01) and inputs.prep.status == "ok"
```

### Behavior of ``skipped`` tasks

- A skipped task is a terminal state—it will not be retried or re-evaluated.
- For dependency resolution, skipped counts as a successful completion.
  Downstream tasks with ``dep_policy="all"`` still become ready.
- ``get_upstream_output("skipped_task")`` returns ``None``.
- ``get_upstream_outputs()`` includes skipped tasks with ``None`` as the value.
- The workflow summary shows a separate ``skipped`` counter.

## Producing and consuming task outputs

### Returning output from a shell script

Scripts write output via ``set_output()``—a simple synchronous call that
writes JSON to a temp file. The processor reads it after exit, and if
the output exceeds 32 KB it is automatically uploaded to Blob Storage:

```python
#!/usr/bin/env python3
"""featurize.py—produces output for downstream tasks."""
from ai4s.jobq.workflow import set_output

# Do work...
features_path = "abfs://data/features.parquet"
stats = {"rows": 1_000_000, "columns": 128}

# Pass results to downstream tasks (JSON-serializable)
set_output({"path": features_path, "stats": stats})
```

For large outputs (over 32 KB), the processor transparently stashes the
data in Blob Storage and stores a ``blob:`` reference. Downstream tasks
call ``get_upstream_output()`` and get the data back—they never see the
blob plumbing.

Blob stash uses the ``JOBQ_WORKFLOW_PREFIX`` account by default, with
container ``jobq-workflow-data``. Override with
``JOBQ_WORKFLOW_BLOBS=<account>/<container>`` to use a different
storage account or container. Without a configured blob target, large
outputs are stored inline (which may hit Table Storage row size
limits).

### Fetching upstream outputs

Tasks can read the output of their completed dependencies:

```python
#!/usr/bin/env python3
"""train.py—consumes upstream output."""
from ai4s.jobq.workflow.context import get_upstream_output, set_output

# Fetch output from the "featurize" task
features = get_upstream_output("featurize")
data_path = features["path"]
print(f"Training on {data_path} ({features['stats']['rows']} rows)")

# ... train model ...

# Return our own output for downstream tasks
set_output({"mae": 0.03, "checkpoint": "abfs://models/schnet-v2.pt"})
```

### File outputs

Use file outputs when a task produces a real file that downstream tasks
need to consume, such as a model checkpoint, parquet file, or tarball.
Keep small metadata in regular JSON output, but wrap large files with
``BlobStasher.from_file(path)`` from a script running inside a workflow
worker:

```python
#!/usr/bin/env python3
from ai4s.jobq.workflow import BlobStasher, set_output

# ... train model and write model.pt ...
set_output({"checkpoint": BlobStasher.from_file("model.pt"), "mae": 0.03})
```

File outputs require ``JOBQ_WORKFLOW_BLOBS=<account>/<container>`` and
land under ``workflow-files/{workflow_id}/{task_name}/{filename}``. For
the ``BlobStasher`` and ``BlobStash`` API surface, see
[File outputs](api.md#file-outputs-with-blobstasher-and-blobstash).

### How blob stash works

```text
Script calls set_output(large_dict)
    │
    ▼
Writes JSON to $JOBQ_OUTPUT_FILE (temp file)
    │
    ▼
WorkflowShellCommandProcessor reads file after exit
    │
    ├── ≤ 32 KB: stored inline in Table Storage (output_ref = JSON string)
    │
    └── > 32 KB: uploaded to Blob Storage (uses JOBQ_WORKFLOW_PREFIX account by default)
                  output_ref = "blob:workflow-outputs/{wf_id}/{task}.json:{md5}"
                  │
                  ▼
        Downstream ctx.get_upstream_output("task")
            → detects "blob:" prefix
            → downloads from Blob Storage automatically
            → returns deserialized dict
```

If an output exceeds 32 KB but no blob target is reachable, the data
is stored inline up to 500 KB. Outputs larger than 500 KB without blob
storage configured will fail the task with a clear error.

### No output needed?

Tasks that don't produce output for downstream consumers can simply
skip the ``set_output()`` call. Downstream ``get_upstream_output()``
returns ``None`` in that case.

Workers that don't need upstream data can ignore the workflow context
entirely—they just receive ``kwargs`` as usual.

## Retries

Each task has its own retry budget, `num_retries`, declared in the
YAML definition. The number of allowed attempts is `num_retries + 1`,
so the default `num_retries: 0` means "one attempt, no retries."

```yaml
tasks:
  - name: flaky-download
    kwargs:
      url: https://example.com/dataset.tar.gz
    num_retries: 3         # up to 4 attempts in total
```

Retries are coordinator-driven. Workers always run a task exactly once
per delivery and report the outcome (success or failure) on the
completion queue. The coordinator consults the failed task's
`attempt_no` against `num_retries`: if budget remains, it bumps
`attempt_no`, re-enqueues a fresh task message, and the task is
re-dispatched; otherwise it marks the task `failed` and propagates
`upstream_failed` to descendants. The bookkeeping lives in the
workflow's runtime-state blob—no worker-side row writes are
involved.

Recovery from worker death is purely transport-driven: the queue
backend redelivers the message after its lock expires and the next
worker picks it up. There is no separate stuck-task timeout—set
`num_retries` generously if you expect transient failures.

For background on how `num_retries` interacts with the underlying
queue's redelivery semantics (especially the Service Bus
`MaxDeliveryCount` behaviour), see the regular jobq
[basics page](basics.md) and the
[deduplication note](misc/40-deduplication.md).

## Cancellation

```bash
ai4s-jobq workflow cancel abc123
```

Cancellation is cooperative but enforced. When a workflow is cancelled:

1. The coordinator stops enqueuing new tasks immediately.
2. Workers running workflow tasks poll the workflow's index row every
   30 seconds. When they detect the workflow is cancelled, they send
   **SIGTERM** to the running subprocess—the same graceful shutdown
   path used for preemption and lock-lost scenarios.
3. After SIGTERM, the worker waits up to `JOBQ_KILL_TIMEOUT` (default
   600 s) before escalating to SIGKILL.

The poll interval is configurable with `JOBQ_CANCEL_POLL_INTERVAL`
(default `30`, in seconds). Lowering this (for example, to `5`) makes
cancellation more responsive at the cost of extra index-row reads
per running task. Raising it (for example, to `120`) reduces read
load when you have many workers but rarely cancel workflows.

Python tasks can also poll for cancellation with ``is_cancelled()``;
see [Producing outputs from Python tasks](api.md#producing-outputs-from-python-tasks).

## Running the coordinator

The coordinator is a stateless process that watches the completion queue
and advances workflows:

```bash
ai4s-jobq workflow coordinator
```

It can be restarted at any time—all state lives in the runtime-state
blobs. Completion messages are durable on the completion queue, so
nothing is lost during downtime. Run **exactly one** coordinator
process per `JOBQ_WORKFLOW_PREFIX` prefix; the design assumes a single
writer to each runtime blob. For HA, deploy the coordinator behind a
single-replica scheduler (Kubernetes `replicas: 1` with `Recreate`
strategy, systemd, etc.) so a new instance starts only after the
previous one exits.

### Tuning flags

A handful of options on `workflow coordinator` matter when you push
the coordinator hard. Defaults are fine for everyday use.

| Flag | Env var | Default | What it does |
|------|---------|---------|--------------|
| `--batch-size` | `JOBQ_COMPLETION_BATCH_SIZE` | 32 | Max completions pulled from the completion queue per receive. Higher values raise throughput on busy workflows but increase per-iteration latency. |
| `--visibility-timeout-s` | `JOBQ_COMPLETION_VISIBILITY_TIMEOUT_S` | 60 | Visibility timeout in seconds for completion messages. If the coordinator crashes mid-batch the messages reappear after this interval. |
| `--idle-sleep-s` | `JOBQ_COORDINATOR_IDLE_SLEEP_S` | 0.5 | Sleep between empty completion-queue polls. Lower for snappier wake-up at the cost of more idle polls. |
| `--cancel-poll-interval-s` | `JOBQ_COORDINATOR_CANCEL_POLL_INTERVAL_S` | 1.0 | Interval between scans of the index for cancel-requested workflows. |
| `--flush-retry-limit` | `JOBQ_COORDINATOR_FLUSH_RETRY_LIMIT` | 2 | Max retries on ETag conflict when flushing the runtime blob. Lower values bail to queue redelivery sooner under hot-spot contention. |
| `--ready-sweep-interval-s` | `JOBQ_COORDINATOR_READY_SWEEP_INTERVAL_S` | 60 | Interval between ready-repair sweeps. The sweep re-dispatches READY tasks whose workflow hasn't been updated for at least `--ready-repair-threshold-s`—closes the submit-or-retry crash window where a task was marked READY but its queue message was never pushed. |
| `--ready-repair-threshold-s` | `JOBQ_COORDINATOR_READY_REPAIR_THRESHOLD_S` | 300 | Minimum age in seconds before the ready-repair sweep considers re-dispatching. Lower values shorten stuck-workflow recovery latency but raise duplicate-execution risk under queue backlog. |

## Architecture overview

```text
  submit ──► WorkflowClient ──► Blob Storage (state blobs)
                                 + Table (index row)
                                      │
                                      ▼
                                 Coordinator ◄── completion queue
                                      │
                          ┌───────────┼───────────┐
                          ▼           ▼           ▼
                     queue:cpu   queue:gpu   queue:eval
                          │           │           │
                       workers     workers     workers
                          │           │           │
                          └───────────┴───────────┘
                                      │
                              completion messages
                              (single shared queue)
```

Workers are unchanged from regular jobq—just use
``WorkflowShellCommandProcessor`` instead of ``ShellCommandProcessor``.
Workflow tasks include ``__workflow_id`` and ``__workflow_task`` in
their kwargs; the processor posts a completion message automatically.

## Running workflow tasks with regular workers

Workflow tasks are dispatched as regular jobq messages. The coordinator
sends them to the task queues specified in the workflow definition.
You run workers on those queues as usual—the only difference is that
workflow tasks need to report completions back.

### WorkflowShellCommandProcessor (automatic)

When ``JOBQ_WORKFLOW_PREFIX`` is set, the default ``--processor shell``
automatically upgrades to ``WorkflowShellCommandProcessor``. No extra
flags needed—just start workers normally:

```bash
# Workers auto-detect workflow tasks when JOBQ_WORKFLOW_PREFIX is set
export JOBQ_WORKFLOW_PREFIX=myaccount/MyProject
# Optional: run completions on Service Bus instead of the default account
# export JOBQ_WORKFLOW_QUEUES=sb://myns
ai4s-jobq myaccount/gpu-a100 worker
```

The processor:

1. Detects ``__workflow_id`` and ``__workflow_task`` in the task kwargs
2. Passes them to the subprocess as environment variables
3. Runs the shell command normally
4. Sends a ``WorkflowCompletion`` to the coordinator on success or failure

Non-workflow tasks pass through unchanged (no completion is sent).

Scripts running inside ``WorkflowShellCommandProcessor`` can use the
helpers shown in [Producing and consuming task outputs](#producing-and-consuming-task-outputs).
The environment variables (``JOBQ_WORKFLOW_ID``, ``JOBQ_WORKFLOW_TASK``,
``JOBQ_WORKFLOW_PREFIX``) are set automatically by the processor. For direct
Python processor integration, see [Custom workflow processors](api.md#custom-workflow-processors).

### Tuning the idle backoff for trickled task waves

Workers feeding off a workflow coordinator typically receive tasks
in waves rather than as a continuous stream—the coordinator
trickles tasks one at a time as parents complete. Combined with
`--idle-timeout` (which keeps the worker alive between waves), the
default 30 s upper bound on the empty-queue exponential backoff means
a worker pool can end up sleeping the full 30 s before noticing a new
wave, hurting wall-clock latency.

Lower it with `--max-idle-backoff` (or `JOBQ_MAX_IDLE_BACKOFF`) when
running workers behind a workflow coordinator:

```bash
ai4s-jobq myaccount/gpu-a100 worker \
    --idle-timeout 1h --max-idle-backoff 2s
```

Two seconds is a good default for workflow workers—small enough to
react quickly when a new wave arrives, large enough to avoid hammering
the queue on long idle stretches.

## Design trade-offs

This section collects the key decisions you'll face when designing
workflows for scale. For a comprehensive treatment of race conditions
and failure modes, see
[Race conditions and failure modes](workflow-races.md).

### Workflow size: many small versus few large

A workflow's runtime state lives in a single JSON blob, and every
completion triggers an ETag-CAS write to that blob. The blob format
scales to tens of thousands of tasks before serialization cost
dominates, but per-workflow CAS contention grows with concurrent
in-flight completions.

| Approach | Advantages | Disadvantages |
|---|---|---|
| Few large workflows (10,000+ tasks) | Single submission, unified status | Large state blob, higher CAS contention on hot completions, CLI inspection slow |
| Many small workflows (100–2,000 tasks) | Small state blobs, lower contention, parallel coordination | More IDs to track, parent-orchestrator pattern needed for dependencies across workflows |

**Recommendation**: aim for at most a few thousand tasks per workflow.
If your workload has tens of thousands of independent tasks, split them
into multiple workflows and use a parent workflow or external script to
submit and monitor the batch.

### Large fan-in: ``--max-fan-in``

When a single task depends on thousands of upstream tasks (for example,
an aggregation step after a large parallel sweep), every parent
reference is inlined into the downstream task's queue message as part
of ``__upstream_outputs_compact``. Azure Storage Queue messages have a
~48 KiB raw cap, so very wide fan-ins can blow the limit and the push
fails outright. The compact wire format (see
``ai4s/jobq/workflow/_compact_refs.py``) keeps the common case
(canonical blob-stashed outputs) at ~12 B/parent, which fits ~3 000
parents per message; non-canonical or inline refs cost more.

```bash
ai4s-jobq workflow submit sweep.yaml --max-fan-in 100
```

This applies the ``sequentialize_fan_in`` transform before submission.
Any task with more than ``max-fan-in`` parents is rewritten into
sequential batches separated by lightweight merge nodes:

```text
[root-0..root-99] → __merge_leaf_0
                         ↓
[root-100..root-199] → __merge_leaf_1
                         ↓
            ...
[root-59900..root-59999] → __merge_leaf_599
                              ↓
                            leaf
```

Each batch has at most ``max-fan-in`` tasks running concurrently. Merge
nodes are zero-cost dummy tasks that the
``WorkflowShellCommandProcessor`` auto-completes immediately.

| | Without ``--max-fan-in`` | With ``--max-fan-in 100`` |
|---|---|---|
| Parallelism | All roots run simultaneously | At most 100 roots at a time |
| Queue-message size for the leaf | Scales with parent count; fails at ~3 000+ canonical refs or ~200+ large inline refs | Bounded |
| Wall-clock overhead |—| ~100–500 ms per batch boundary |

For ML workloads where each task takes minutes, the batch-boundary
overhead is negligible. Increase ``max-fan-in`` for more parallelism at
the cost of a larger downstream queue message.

**Accessing upstream outputs from the leaf**: after the transform, the
leaf's ``depends_on`` only lists the final merge node. Use
``get_real_upstream_tasks()`` to discover all original upstream tasks:

```python
#!/usr/bin/env python3
"""aggregate.py—collects results from all upstream tasks."""
from ai4s.jobq.workflow import get_real_upstream_tasks, get_upstream_output

all_roots = get_real_upstream_tasks()
results = {name: get_upstream_output(name) for name in all_roots}
```

The walk only reads merge-node entities (O(num\_batches), not
O(num\_roots)), so it scales efficiently even for very large fan-ins.

**Limitations**:

- Conditions are incompatible—a task with a ``condition`` cannot be
  restructured. Submission raises an error if both are present.
- Batches run sequentially—the transform trades parallelism for bounded
  coordinator load.

The transform is also available as a Python API:

```python
from ai4s.jobq.workflow.transforms import sequentialize_fan_in

transformed = sequentialize_fan_in(definition, max_fan_in=100)
await client.submit(transformed)
```

### Queue backend: Storage Queues versus Service Bus

| | Storage Queues | Service Bus |
|---|---|---|
| Coordinator throughput | Limited by receive-RTT (~10 ms) | Higher (long-poll, batched receive) |
| Cost | Cheaper at low/medium volume | Higher base cost |
| Message ordering | Best-effort | FIFO per session (if needed) |
| Deduplication | Not built-in | 7-day window, zero-effort |
| Best for | Development, small-to-medium workflows | Production, high-throughput, large fan-out |

Switch with ``JOBQ_WORKFLOW_QUEUES=sb://<namespace>``. Nothing in the
workflow YAML changes.

### Task granularity

Every task has fixed coordinator overhead (one runtime-blob write per
completion). Extremely fine-grained tasks (sub-second execution) can
make the coordinator the bottleneck rather than the actual compute.

| Task duration | Coordinator impact | Recommendation |
|---|---|---|
| Minutes to hours | Negligible | Ideal granularity |
| Seconds | Acceptable for small DAGs | Fine for hundreds of tasks |
| Sub-second | Coordinator-bound | Batch work into fewer, larger tasks |

### Splitting state, queues, and blobs across accounts

A single Azure Storage account is rated for roughly 20,000
transactions/sec across Tables, Queues, and Blobs combined. Under heavy
load, split them:

```bash
export JOBQ_WORKFLOW_PREFIX=state-account/MyProject       # Index Table + state blobs
export JOBQ_WORKFLOW_QUEUES=sb://my-namespace      # Queues (Service Bus)
export JOBQ_WORKFLOW_BLOBS=blobs-account/container # Large outputs
```

This isolates Table contention from queue polling and blob uploads,
giving each subsystem its own throughput budget.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `JOBQ_WORKFLOW_PREFIX` | Yes (workflow) | `<account>/<prefix>`—the state account plus the resource-name prefix (index Table `<prefix>WorkflowsIndex`, blob container `<prefix>-workflows`). Same account hosts queues and large-output blobs by default. |
| `JOBQ_WORKFLOW_QUEUES` | No | Override the queue backend. Set to `sb://<namespace>` for Service Bus or to a different storage-account name to split state and queues. Defaults to the `JOBQ_WORKFLOW_PREFIX` account. |
| `JOBQ_WORKFLOW_BLOBS` | For large outputs | `<account>/<container>` for stashing outputs over 32 KB. Defaults to the `JOBQ_WORKFLOW_PREFIX` account with container `<prefix>-outputs`. |
| `JOBQ_WORKFLOW_ID` | Auto-set | Set by WorkflowShellCommandProcessor for subprocesses |
| `JOBQ_WORKFLOW_TASK` | Auto-set | Set by WorkflowShellCommandProcessor for subprocesses |
| `JOBQ_OUTPUT_FILE` | Auto-set | Temp file path for script output (written by `set_output()`) |
| `JOBQ_QUEUE` | Yes | Queue name (existing jobq variable) |
| `JOBQ_CANCEL_POLL_INTERVAL` | No | How often workers poll for cancellation (seconds, default 30) |
| `JOBQ_MAX_IDLE_BACKOFF` | No | Upper bound on the worker's empty-queue exponential backoff (default 30 s). Set to a small value (for example, `2s`) when workers feed off a workflow coordinator that trickles tasks. Only meaningful with `--idle-timeout`. |
| `JOBQ_HTTP_POOL_SIZE` | No | Size of the shared HTTP connection pool used by all Azure Storage clients (Tables, Queues, Blobs). Default is `100`. Bump it on coordinators or on workers running many parallel uploads if you see `pool full` warnings or stalled requests in the logs. |
| `JOBQ_COMPLETION_BATCH_SIZE` | No | Equivalent to `workflow coordinator --batch-size`. Default 32. |
| `JOBQ_COMPLETION_VISIBILITY_TIMEOUT_S` | No | Equivalent to `workflow coordinator --visibility-timeout-s`. Default 60. |
| `JOBQ_COORDINATOR_IDLE_SLEEP_S` | No | Equivalent to `workflow coordinator --idle-sleep-s`. Default 0.5. |
| `JOBQ_COORDINATOR_CANCEL_POLL_INTERVAL_S` | No | Equivalent to `workflow coordinator --cancel-poll-interval-s`. Default 1.0. |
| `JOBQ_COORDINATOR_FLUSH_RETRY_LIMIT` | No | Equivalent to `workflow coordinator --flush-retry-limit`. Default 2. |
| `JOBQ_COORDINATOR_READY_SWEEP_INTERVAL_S` | No | Equivalent to `workflow coordinator --ready-sweep-interval-s`. Default 60. |
| `JOBQ_COORDINATOR_READY_REPAIR_THRESHOLD_S` | No | Equivalent to `workflow coordinator --ready-repair-threshold-s`. Default 300. |
| `JOBQ_COMPLETION_SEND_MAX_ATTEMPTS` | No | Worker-side retries when posting a completion message. Default 5. |
