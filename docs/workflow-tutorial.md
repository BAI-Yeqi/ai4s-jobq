# Workflow tutorial: local walkthrough

This tutorial runs a complete workflow end-to-end on your local machine
using [Azurite](https://github.com/Azure/Azurite) (the Azure Storage
emulator). No Azure subscription needed.

## How the pieces fit together

Three roles, plus three Azure services for state and transport:

```text
  ┌──────────┐   submit         ┌──────────────────┐
  │  client  │ ───────────────▶ │   Table Storage  │  (DAG state, ETag-guarded)
  │  (you)   │                  └────────┬─────────┘
  └──────────┘                           │ poll/update
                                         ▼
                              ┌─────────────────────┐
                              │     coordinator     │  (always-on; stateless)
                              │  - dispatch ready   │
                              │  - process events   │
                              │  - apply retries    │
                              └──────┬──────────────┘
                                     │ enqueue (per-task queue)
                                     ▼
                              ┌─────────────────────┐
                              │ Service Bus / Queue │  (work distribution)
                              │      Storage        │
                              └──────┬──────────────┘
                                     │ pull
                                     ▼
                              ┌─────────────────────┐
                              │       worker(s)     │  (one per queue/SKU)
                              │  - run cmd          │
                              │  - write output     │
                              │  - post completion  │
                              └──────┬──────────────┘
                                     │ completion message
                                     ▼
                              ┌─────────────────────┐
                              │ completion queue    │  (one shared queue)
                              └──────┬──────────────┘
                                     │ consumed by coordinator → loop
                                     ▼
                                  (back to top)

  Large task outputs (>32 KB) are stashed in Blob Storage; downstream
  tasks fetch them transparently via get_upstream_output().
```

You'll start the coordinator and at least one worker as long-running
processes; the client (`workflow submit`, `workflow status`,
`workflow watch`, etc.) is short-lived and just talks to Table Storage.

## Prerequisites

```bash
pip install ai4s-jobq[workflow]
npm install azurite   # or use the global install
```

## 1. Start Azurite

Workflows need three Azurite services: queues (task dispatch), tables
(DAG state), and optionally blobs (large outputs).

```bash
npx azurite-queue --skipApiVersionCheck --inMemoryPersistence --queuePort 10001 &
npx azurite-table --skipApiVersionCheck --inMemoryPersistence --tablePort 10002 &
npx azurite-blob  --skipApiVersionCheck --inMemoryPersistence --blobPort 10000 &
```

## 2. Configure storage

Workflow CLI commands take **storage account + prefix** as a positional
argument (the **prefix** scopes the workflow tables, so multiple
projects can share one storage account without colliding):

```bash
ai4s-jobq workflow devstoreaccount1/Tutorial submit pipeline.yaml
ai4s-jobq workflow devstoreaccount1/Tutorial status <wf-id>
```

The only shell variable you'll want to export for this tutorial is the
**workflow** itself—storage account plus project prefix:

```bash
export JOBQ_WORKFLOW_PREFIX=devstoreaccount1/Tutorial   # account/prefix
```

Optional, only if your tasks emit large outputs (over 32 KB):

```bash
export JOBQ_WORKFLOW_BLOBS=devstoreaccount1/jobq-workflow-data
```

```{tip}
With `JOBQ_WORKFLOW_PREFIX` exported you can drop the positional from every
client command—`ai4s-jobq workflow status <wf-id>` works directly.
The positional still wins when both are supplied.
```

```{note}
Coming from non-workflow jobq, you may be used to the
`STORAGE/QUEUE_NAME` form (or `JOBQ_STORAGE_QUEUE`). Workflows use a
single `JOBQ_WORKFLOW_PREFIX=<account>/<prefix>` instead—the account hosts
both the state tables and (by default) the queues, while queue
**names** come from the workflow YAML's `default_queue` and per-task
`queue:` fields. One workflow can fan out across many queues without
juggling env vars. The worker CLI still takes the full
`STORAGE/QUEUE_NAME` pair positionally (see [step 7](#7-start-workers)).
```

### Production (Azure, managed identity)

In production, point `JOBQ_WORKFLOW_PREFIX` at a real Azure storage account
and authenticate with **DefaultAzureCredential** (managed identity,
Azure CLI login, etc.)—no SAS tokens, no AccountKey, no secrets in env
vars:

```bash
# State tables + queues + blobs all live on this account by default
export JOBQ_WORKFLOW_PREFIX=mystorageaccount/MyProject

# Override the queue backend to use Service Bus instead
# export JOBQ_WORKFLOW_QUEUES=sb://my-namespace

# Override the blob container for large task outputs
# export JOBQ_WORKFLOW_BLOBS=mystorageaccount/jobq-workflow-data
```

Client commands can then either rely on the env var or pass the
positional explicitly:

```bash
ai4s-jobq workflow submit pipeline.yaml                   # uses JOBQ_WORKFLOW_PREFIX
ai4s-jobq workflow mystorageaccount/MyProject submit pipeline.yaml   # explicit
```

The principal running the worker / coordinator / client needs these
RBAC roles on the target storage account:

| Variable / endpoint | Required role |
|---|---|
| `JOBQ_WORKFLOW_PREFIX` (state tables) | Storage Table Data Contributor |
| `JOBQ_WORKFLOW_PREFIX` (queues, default) | Storage Queue Data Contributor |
| `JOBQ_WORKFLOW_QUEUES=sb://…` (Service Bus) | Azure Service Bus Data Owner |
| `JOBQ_WORKFLOW_BLOBS` (large outputs) | Storage Blob Data Contributor |

### Choosing a queue backend

Workflows run on either **Azure Storage Queues** or **Azure Service
Bus**—both are first-class. By default the queue backend is the same
storage account as `JOBQ_WORKFLOW_PREFIX`; set `JOBQ_WORKFLOW_QUEUES=sb://<namespace>`
to switch to Service Bus, or to a different storage account name to
split state and queues. Per-task `queue:` fields in the YAML name a
queue on whichever backend you've chosen.

| | Storage Queues | Service Bus |
|---|---|---|
| Setup | None (storage account already exists) | Provision a Service Bus namespace |
| Cost | Cheaper at low/medium throughput | Higher base cost |
| Message size | 64 KB | 256 KB (Standard) / 1 MB (Premium) |
| Visibility timeout | Up to 7 days | Up to 5 minutes per renewal (auto-renewed) |
| Best for | Most workflows | Long tasks, very large messages, FIFO sessions |

To switch a workflow from Storage Queues to Service Bus, set
`JOBQ_WORKFLOW_QUEUES=sb://<namespace>` and make sure the named queues
exist in the new namespace—nothing in the workflow YAML itself needs
to change.

## 3. Write task scripts

Create two scripts that form a simple pipeline: **featurize** produces
data, **train** consumes it.

`featurize.py`:

```python
#!/usr/bin/env python3
from ai4s.jobq.workflow import set_output

print("Featurizing...")
set_output({"path": "/tmp/features.parquet", "rows": 50000})
print("Done.")
```

`train.py`:

```python
#!/usr/bin/env python3
from ai4s.jobq.workflow import get_upstream_output, set_output

features = get_upstream_output("featurize")
print(f"Training on {features['path']} ({features['rows']} rows)")

# ... training logic ...

set_output({"model": "/tmp/model.pt", "mae": 0.03})
print("Training complete.")
```

If `train.py` writes a real checkpoint for later tasks, return it as a
file output instead of a path string:

```python
from ai4s.jobq.workflow import BlobStasher, set_output

# ... training logic writes model.pt ...
set_output(
    {
        "model": BlobStasher.from_file("model.pt"),
        "mae": 0.03,
    }
)
```

A downstream task downloads it only when needed:

```python
from ai4s.jobq.workflow import get_upstream_output

train = get_upstream_output("train")
train["model"].download_to("models/model.pt")
```

## 4. Write the workflow definition

`pipeline.yaml`:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/microsoft/ai4s-jobq/main/ai4s/jobq/workflow/data/workflow-definition.schema.json
name: featurize-and-train
default_queue: default
tasks:
  - name: featurize
    kwargs:
      cmd: python featurize.py
  - name: train
    kwargs:
      cmd: python train.py
    depends_on: [featurize]
```

The first comment is a [yaml-language-server][yls] directive. Editors
that ship the YAML language server (VS Code, Neovim with `yaml-ls`,
JetBrains IDEs) will use it for autocompletion, hover docs, and
inline validation as you type—no setup needed beyond the comment.

[yls]: https://github.com/redhat-developer/yaml-language-server

The `cmd` key in `kwargs` is what `ShellCommandProcessor` executes.
Other keys you can put under `kwargs` for shell tasks: `env` (extra
environment variables), `cwd` (working directory), `bg_dirsync_to`
(background output sync). Each task can target a different queue
via the `queue:` field (for example, GPU tasks on `gpu-a100`); for
this tutorial everything runs on `default`. The queue name in the
worker CLI (`devstoreaccount1/default` below) must match
`default_queue` (or the per-task `queue:`) in the YAML.

### Fan-out: where workflows really pay off

A linear `featurize → train` chain shows the mechanics, but the value
of the DAG comes from fan-out. Here is a diamond: featurize once,
train two models in parallel, then compare them. Workers pull both
training tasks at the same time:

```yaml
name: featurize-and-compare
default_queue: default
tasks:
  - name: featurize
    kwargs: {cmd: python featurize.py}
  - name: train-gnn
    kwargs: {cmd: python train.py --model gnn}
    depends_on: [featurize]
  - name: train-rf
    kwargs: {cmd: python train.py --model rf}
    depends_on: [featurize]
  - name: compare
    kwargs: {cmd: python compare.py}
    depends_on: [train-gnn, train-rf]
```

`compare` reads both upstream outputs:

```python
gnn = get_upstream_output("train-gnn")
rf = get_upstream_output("train-rf")
print(f"GNN MAE={gnn['mae']}, RF MAE={rf['mae']}")
```

## 5. Submit the workflow

Set a shell variable to hold the **storage/prefix** positional so the
remaining examples stay short:

```bash
export WF=devstoreaccount1/Tutorial
ai4s-jobq workflow $WF submit pipeline.yaml
```

You'll see a one-line preview on stderr—name, task count, root tasks—plus
the workflow ID on stdout (for example, `a1b2c3d4`):

```text
Submitted "ml-pipeline" (4 tasks, 1 root: ingest) → a1b2c3d4
a1b2c3d4
```

Because the ID is the only thing on stdout, you can pipe it directly:

```bash
WF_ID=$(ai4s-jobq workflow $WF submit pipeline.yaml)
ai4s-jobq workflow $WF watch "$WF_ID"
```

The workflow is now in Table Storage with status `pending`—no tasks are
dispatched yet.

You can also pass a custom ID:

```bash
ai4s-jobq workflow $WF submit pipeline.yaml --id my-pipeline-001
```

## 6. Start the coordinator

The coordinator watches for completions and dispatches ready tasks.
Run it in a separate terminal (with `JOBQ_WORKFLOW_PREFIX` set):

```bash
export JOBQ_WORKFLOW_PREFIX=devstoreaccount1/Tutorial
ai4s-jobq workflow coordinator
```

The coordinator activates pending workflows: root tasks (no
dependencies) are immediately pushed to their target queues. It then
enters a loop, waiting for completion messages from workers.

## 7. Start workers

Workers select `WorkflowShellCommandProcessor` based on whether
`JOBQ_WORKFLOW_PREFIX` is set in their environment, so export it in the
worker terminal:

```bash
export JOBQ_WORKFLOW_PREFIX=devstoreaccount1/Tutorial
ai4s-jobq devstoreaccount1/default worker --idle-timeout 5m
```

The `--idle-timeout 5m` flag keeps the worker alive between task waves.
In a multi-stage workflow there is a brief gap (typically a few
seconds) between a task completing and the coordinator enqueuing the
next stage—without `--idle-timeout`, the worker exits the moment the
queue empties and you have to restart it for every stage.

The worker pulls tasks from the queue, runs the shell command, and
sends a completion message back to the coordinator.

You can start multiple workers on the same or different queues:

```bash
# Terminal 3 — another worker on the same queue
ai4s-jobq devstoreaccount1/default worker --idle-timeout 5m

# Terminal 4 — worker on a GPU queue (if your workflow uses one)
ai4s-jobq devstoreaccount1/gpu-a100 worker --idle-timeout 5m
```

### Worker not picking up workflow tasks?

The worker selects `WorkflowShellCommandProcessor` based on whether
`JOBQ_WORKFLOW_PREFIX` is set in its own environment when it starts. If you
set the variable after the worker is already running, restart the
worker (or export it in the same shell where you launched it). A
useful sanity check: workflow workers log
`Using WorkflowShellCommandProcessor` at startup; plain workers do
not.

For a quick end-to-end health check, run `ai4s-jobq workflow $WF doctor`
(see [Sanity-check the setup](#sanity-check-the-setup) below).

## 8. Monitor progress

### Quick status check

```bash
# Single workflow
ai4s-jobq workflow $WF status <workflow-id>

# All workflows
ai4s-jobq workflow $WF list
```

Example output:

```text
Workflow:  a1b2c3d4
Name:      featurize-and-train
Status:    running
Progress:  1/2 completed, 1 running, 0 failed

Task            Status      Queue     Depends On   Output
featurize       completed   default   —            {"path": "/tmp/features.parquet", ...}
train           running     default   featurize    —
```

For the meaning of each status (`pending`, `ready`, `running`,
`completed`, `failed`, `upstream_failed`, `skipped`, `cancelled`),
see the [task and workflow statuses
glossary](workflows.md#task-and-workflow-statuses) in the reference docs.

### Aggregate summary

```bash
ai4s-jobq workflow $WF summary
```

```text
Workflows:
  completed      3
  running        1
  total          4

Tasks:
  completed:    7
  running:      2
  failed:       0
  pending:      1
  total:        10

Progress:     70.0%
```

### List tasks across workflows

```bash
# All tasks
ai4s-jobq workflow $WF tasks

# Filter by workflow, status, or queue
ai4s-jobq workflow $WF tasks --workflow a1b2c3d4
ai4s-jobq workflow $WF tasks --status failed
ai4s-jobq workflow $WF tasks --queue gpu-a100

# JSON output for scripting
ai4s-jobq workflow $WF tasks --json | jq '.[] | select(.status == "running")'
```

### Machine-readable output

Every monitoring command supports `--json`:

```bash
ai4s-jobq workflow $WF status a1b2c3d4 --json
ai4s-jobq workflow $WF list --json
ai4s-jobq workflow $WF summary --json
```

## 9. Cancel a workflow

```bash
ai4s-jobq workflow $WF cancel <workflow-id>
```

This sets the workflow status to `cancelled` in Table Storage. Running
workers detect the cancellation within `JOBQ_CANCEL_POLL_INTERVAL`
seconds (default: 30) and terminate their subprocesses.

Tasks that have not started yet are skipped by the coordinator.

## Sanity-check the setup

When something doesn't look right—workflows stuck in `pending`,
tasks not progressing, workers seemingly idle—run `workflow doctor`
to triage the most common causes in one go:

```bash
ai4s-jobq workflow $WF doctor
```

Sample output:

```text
✔ Workflow config: account=myaccount, prefix=MyProject, JOBQ_WORKFLOW_PREFIX=myaccount/MyProject
✔ Workflow store: reachable (prefix=MyProject, 12 workflow(s))
✔ Completion queue: reachable: myproject-workflow-completions on myaccount
⚠ Pending workflows: 2 stuck in 'pending' >120s: a3f1…, b29c…
    → The coordinator may not be running. Start one with
      `ai4s-jobq workflow $WF coordinator` (the sweeper picks up pending
      workflows every ~10s).
✔ Running tasks: none stuck (>60m running)

Doctor: 4 passed, 1 warning(s).
```

It checks that the workflow account/prefix are configured, that the
state tables and completion queue are reachable, and surfaces stuck
`pending` workflows (no coordinator running) or stuck `running` tasks
(worker likely died). Use `--json` for machine-readable output and
`--stale-pending-sec` / `--stale-running-min` to tune the thresholds.
Doctor exits non-zero if any check fails.

## 10. Cleanup

### Purge all workflow data

```bash
# Interactive confirmation
ai4s-jobq workflow $WF purge

# Skip confirmation
ai4s-jobq workflow $WF purge --yes

# Also drop and recreate the tables
ai4s-jobq workflow $WF purge --drop-tables --yes
```

### Stop Azurite

```bash
# Find and stop the Azurite processes
kill $(jobs -p)
```

## Quick start: single-command workflows

For simple cases where you just want one command tracked as a workflow:

```bash
ai4s-jobq devstoreaccount1/default push --workflow -c "python train.py"
```

This creates a single-task workflow per command, visible via
`ai4s-jobq workflow $WF status`. Useful for adding tracking to
existing push-based workflows without writing a JSON definition.

## Environment variable reference

Workflows are configured by a tiny `JOBQ_WORKFLOW_PREFIX*` family. The
`<account>` segment may be a **bare storage-account name** (uses
DefaultAzureCredential) or the literal **`devstoreaccount1`** as
Azurite shorthand for local development.

| Variable | Required | Description |
|----------|----------|-------------|
| `JOBQ_WORKFLOW_PREFIX` | Yes (worker / coordinator) | `<account>/<prefix>`. The state account plus the resource-name prefix (index Table `<prefix>WorkflowsIndex`, blob container `<prefix>-workflows`). The same account hosts queues and large-output blobs by default. Client commands can use this or the positional `STORAGE/PREFIX` shorthand. |
| `JOBQ_WORKFLOW_QUEUES` | No | Override the queue backend. Set to `sb://<namespace>` for Service Bus, or to a different storage-account name to split state and queues. Defaults to the `JOBQ_WORKFLOW_PREFIX` account. |
| `JOBQ_WORKFLOW_BLOBS` | Recommended for large outputs | `<account>/<container>` for stashing outputs over 32 KB. Defaults to the `JOBQ_WORKFLOW_PREFIX` account with container `<prefix>-outputs`. Required for outputs over 500 KB. |
| `JOBQ_CANCEL_POLL_INTERVAL` | No | Cancel-poll frequency in seconds (default: 30). |

Auto-set by the processor (do not set manually):

| Variable | Description |
|----------|-------------|
| `JOBQ_WORKFLOW_ID` | Workflow ID for the current task. |
| `JOBQ_WORKFLOW_TASK` | Task name for the current task. |
| `JOBQ_OUTPUT_FILE` | Temp file path for `set_output()`. |

## Handling failures

When a task fails, the coordinator marks it as ``failed`` in Table
Storage and records the exit code and error message. Downstream tasks
that depend on it are marked ``upstream_failed``—they will not run.

### Inspecting failed tasks

```bash
# Show workflow status (includes failed count)
ai4s-jobq workflow $WF status <workflow-id>

# List all tasks with their status
ai4s-jobq workflow $WF tasks <workflow-id>

# Filter to just failed tasks
ai4s-jobq workflow $WF tasks <workflow-id> --prefix "failed-task-name"

# Get JSON with full details (error messages, exit codes)
ai4s-jobq workflow $WF tasks <workflow-id> --json
```

The JSON output includes ``error`` and ``exit_code`` fields for failed
tasks.

### Retries

Tasks can be configured with ``num_retries`` in the workflow YAML.
Each task gets up to ``num_retries + 1`` attempts (default: 0, meaning
one attempt with no retries).

```yaml
name: resilient-pipeline
tasks:
  - name: download-data
    kwargs:
      url: https://example.com/data.tar.gz
    num_retries: 3
  - name: train
    depends_on: [download-data]
```

When ``download-data`` fails, the worker publishes a failure
completion. The coordinator inspects the task's ``attempt_no``
against ``num_retries``: if budget remains, it bumps ``attempt_no``
and pushes a fresh task message; otherwise it marks the task
``failed``, which propagates ``upstream_failed`` to ``train``.
The worker is stateless about retries—every delivery is one
attempt.

### Cancelling a stuck workflow

```bash
ai4s-jobq workflow $WF cancel <workflow-id>
```

This sets the workflow to ``cancelled``. Running workers detect
the cancellation within 30 seconds and send SIGTERM to their
subprocesses. Pending tasks are not enqueued.
