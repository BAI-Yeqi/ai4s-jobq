# Workflow stress test

End-to-end stress test for the DAG workflow engine at scale (thousands
of workflows, tens of thousands of tasks) against Azure Table Storage.

| Script | Purpose |
|---|---|
| `generate.py` | Create random DAG topologies as JSON files. |
| `dummy_processor.py` | In-process workflow task simulator for workers. |
| `monitor.py` | Live aggregate dashboard with throughput-over-time. |
| `run_full_stack.py` | One-command full-stack test (coordinator + workers + queues). |
| `chemistry_stress.py` | Chemistry-shaped DAGs (datapoint = workflow: sim roots → SCF dependents → single wide-fan-in CCSD) with time-compressed sleeps and a throughput estimator. |

## Setup

```bash
pip install -e ".[workflow]"
az login
export JOBQ_WORKFLOW_PREFIX=mystorageaccount/StressTest
```

The account name resolves to `https://<account>.table.core.windows.net`
and authenticates via `DefaultAzureCredential` (so `az login` is
enough).

## Quick start: full-stack test

`run_full_stack.py` automates the entire flow—generate, submit, launch
coordinator and workers, poll until done, shut everything down:

```bash
export JOBQ_WORKFLOW_PREFIX=mystorageaccount/StressMulti

# Default: 2000 workflows, 50 workers, 1 worker process
python run_full_stack.py

# More parallelism:
python run_full_stack.py --workflows 2000 --workers 50 --worker-procs 3

# Skip steps if you already generated / submitted:
python run_full_stack.py --skip-generate --skip-submit

# Cleanup afterwards:
ai4s-jobq workflow purge --yes --drain-queues
```

The script shows a live progress table (workflows and tasks
completed, throughput) and exits when all workflows reach a terminal
state.

## Manual step-by-step

From `examples/stress_test/`:

```bash
# 1. Generate workflow definitions (local, fast)
python generate.py --count 2000 --out-dir workflows/ --seed 42

# 2. Submit to Table Storage
ls workflows/wf-*.json | ai4s-jobq workflow submit --concurrency 20

# 3a. (Second terminal) live dashboard
python monitor.py --interval 3

# 3b. Drive the workflows through their DAG with a dummy callback
ai4s-jobq workflow run-local --concurrency 50

# 4. Clean up
ai4s-jobq workflow purge --yes --drain-queues
```

`run-local` walks each workflow through its DAG in-process—no
coordinator or Service Bus needed, so this isolates Table Storage
performance.

> [!IMPORTANT]
> `run-local` is mutually exclusive with the coordinator (and with real
> workers).  Both drive the same DAGs, and running them against the
> same prefix at the same time produces orphan-sweeper repair messages
> in the coordinator log as the two drivers race on individual tasks.
> Pick one or partition the workflows across two prefixes.

To stress the **full system** (coordinator + queues + workers) manually,
run three things in parallel against the same prefix:

```bash
# Terminal 1 — coordinator
ai4s-jobq workflow coordinator --max-running-workflows 50

# Terminal 2 — N worker processes against the task queue.  The
# generated workflows all use default_queue=stress-test, and the
# processor reads sleep_s / fail_probability from each task's kwargs
# (see dummy_processor.py).
PYTHONPATH=. ai4s-jobq <account>/stress-test worker \
    --proc dummy_processor.DummyWorkflowProcessor \
    -n 20 \
    --idle-timeout 5m

# Terminal 3 — live dashboard
python monitor.py --interval 3
```

For more parallelism, launch additional `ai4s-jobq <account>/stress-test
worker ...` processes against the same queue—they share the workload
naturally via the queue's at-least-once delivery. Each worker process
forks `-n` async workers internally.

## Scripts

### `generate.py`—create workflow definitions

Random DAG topologies as JSON files. Mix of:

| Topology | Description | Tasks per workflow |
|---|---|---|
| `linear` | Sequential chain | 3–15 |
| `diamond` | Fan-out → fan-in | 5–22 |
| `wide` | Independent parallel tasks | 10–80 |
| `staircase` | Multi-layer diamond | 10–60 |
| `resilient_diamond` | Diamond with `dep_policy="any"` | 6–14 |

```bash
python generate.py --count 2000 --seed 42       # reproducible
python generate.py --count 5000 --seed 123      # different seed
```

### `run_full_stack.py`—automated full-system test

Orchestrates the complete flow in one command:

1. Generates workflow JSON files (via `generate.py`)
2. Submits them via `ai4s-jobq workflow submit`
3. Launches a coordinator process (`--single` mode)
4. Launches one or more worker processes with `DummyWorkflowProcessor`
5. Polls `WorkflowClient.summary()` with a live progress display
6. Shuts down all subprocesses when all workflows complete

```bash
python run_full_stack.py --help     # see all options
```

### `monitor.py`—live progress dashboard

Polls `WorkflowClient.summary()` and renders a live `rich` dashboard
with workflow/task counts, a progress bar, and current vs average
task throughput. The startup banner prints the storage account and
table names being polled. Queue depths and worker liveness are not
tracked—use `ai4s-jobq peek` or queue-side metrics for those.

```bash
python monitor.py --interval 3
```

For a one-shot snapshot use `ai4s-jobq workflow summary` (or
`--json`) instead.

## What it measures

| Metric | Where |
|---|---|
| Submit throughput and latency | `ai4s-jobq workflow submit` (progress to stderr) |
| Task processing throughput | `run-local` or `run_full_stack.py` (final summary) |
| Live throughput-over-time | `monitor.py` or `run_full_stack.py` |
| ETag contention rate | `ai4s-jobq workflow run-local -v` (retry logs) |
| Table Storage query cost | `monitor.py` (each poll lists all workflows) |
| Full-stack latency | `run_full_stack.py` (coordinator + queues + workers) |

## Scaling notes

- **Table Storage partitioning**: workflows use `PK="wf"` (single
  partition); tasks use `PK=workflow_id` (one partition per
  workflow). At thousands of workflows the workflow table becomes a
  hot partition—this is intentional, to find the ceiling.
- **Batch insert limit**: Table Storage transactions are limited to
  100 entities with the same partition key. Workflows with more than
  100 tasks use multiple batches.
- **ETag retries**: `increment_child_deps` uses optimistic concurrency
  with ETag-guarded updates. Under high contention (wide fan-in) you
  may see retry loops—expected and logged at DEBUG.
