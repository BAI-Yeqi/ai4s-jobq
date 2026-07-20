# Copilot Instructions for ai4s-jobq

## Build & Test

Tests require [Azurite](https://github.com/Azure/Azurite) (Azure Storage emulator) running locally:

```bash
# Start Azurite (blob on 10000, queue on 10001)
# `--disableTelemetry` is required in network-restricted environments
# (e.g. sandboxed agents) where Azurite's outbound Application
# Insights call would otherwise crash the process on DNS failure.
./node_modules/.bin/azurite-blob --skipApiVersionCheck --inMemoryPersistence --disableTelemetry --blobPort 10000 &
./node_modules/.bin/azurite-queue --skipApiVersionCheck --inMemoryPersistence --disableTelemetry --queuePort 10001 &

# Install in dev mode and run tests
pip install -e .[dev]
pytest

# Run a single test
pytest tests/test_cli.py::test_name -x

# Run live Azure tests (requires real Azure credentials)
pytest --run-live
```

## Lint & Format

Pre-commit hooks run ruff (lint + format), mypy, and security checks:

```bash
pre-commit run --all-files

# Or individually:
ruff check --fix .
ruff format .
mypy ai4s/ --no-namespace-packages
```

Ruff is configured with 100-char line length, 25+ rule groups (security, bugbear, naming,
async, performance, etc.), and extensive per-file-ignores for intentional patterns.
See `pyproject.toml` `[tool.ruff.lint]` for the full configuration.

Mypy runs with `check_untyped_defs`, `warn_redundant_casts`, `warn_unused_ignores`,
`warn_return_any`, and `no_implicit_reexport` enabled. The pre-commit hook installs
the package itself as a dependency for full type coverage.

## Documentation Lint

Docs use a mix of RST (`docs/*.rst`) and Markdown (`docs/**/*.md`, `README.md`, `CONTRIBUTING.md`),
built with Sphinx + myst_parser. Three doc linters run as pre-commit hooks:

```bash
# Run all doc linters via pre-commit
pre-commit run doc8 --all-files
pre-commit run markdownlint --all-files
pre-commit run vale --all-files

# Or individually:
doc8                                                    # RST only
npx markdownlint-cli '**/*.md' --ignore node_modules --ignore vale-styles  # Markdown only
vale docs/ README.md CONTRIBUTING.md SUPPORT.md         # prose style (MD + RST)
```

**doc8** checks RST structure (line length ≤ 100, whitespace, syntax). Config in `pyproject.toml`
under `[tool.doc8]`.

**markdownlint** checks Markdown formatting. Config in `.markdownlint.json` (MD013 line-length
disabled, MD024 siblings_only). Files excluded in `.markdownlintignore` (CHANGELOG.md,
`ai4s/jobq/data/`).

**Vale** checks prose style using the Google style guide as a base, plus custom rules in
`vale-styles/JobQ/`. Config in `.vale.ini`. Key points:

- Run `vale sync` after cloning to download the Google style package (gitignored under
  `vale-styles/Google/`).
- Custom vocabulary in `vale-styles/config/vocabularies/JobQ/accept.txt` — add new technical
  terms here when Vale flags legitimate project jargon as spelling errors.
- Custom rules: `JobQ.Headings` (sentence-case with acronym exceptions),
  `JobQ.Latin` (prefer "for example" over "e.g.").
- The pre-commit hook runs with `--minAlertLevel error` so warnings don't block commits.
- CHANGELOG.md has `Vale.Repetition` and `JobQ.Headings` disabled (changelog entries
  naturally repeat words and use an all-caps title).
- Noisy rules (passive voice, contractions, parenthetical usage) are disabled — see `.vale.ini`.

When writing or editing docs, prefer plain English over Latin abbreviations ("for example" not
"e.g.", "that is" not "i.e."), use em-dashes without spaces ("word—word" not "word — word"),
and keep headings in sentence case.

## Architecture

**`ai4s.jobq`** is a distributed job queue built on Azure Storage Queues and Azure Service Bus. Users push tasks; workers pull and process them asynchronously.

### Core layers

- **`entities.py`** — Data classes (`Task`, `Response`, `EmptyQueue`, `WorkerCanceled`). Tasks serialize to JSON with versioned schemas.
- **`jobq.py`** — `JobQ` class: the main API. Created via async context managers (`from_storage_queue`, `from_service_bus`, `from_connection_string`, `from_environment`). `JobQFuture` provides awaitable results.
- **`backend/`** — Queue backend implementations behind Protocol classes (`JobQBackend`, `JobQBackendWorker`, `Envelope`) defined in `backend/common.py`. Backends: `storage_queue.py` (Azure Storage Queue) and `servicebus.py` (Azure Service Bus).
- **`work.py`** — Worker-side processing: `Processor` ABC, `ProcessPool` for parallel execution, `ShellCommandProcessor` for CLI-driven tasks.
- **`orchestration/`** — Higher-level coordination: `batch_enqueue`, `launch_workers`, workforce management, multi-region support.
- **`cli.py`** — CLI entry point (`ai4s-jobq`) built with `asyncclick`. Subcommands: push, worker, peek, clear, etc.
- **`track/`** — Optional Dash-based monitoring dashboard (installed via `pip install ai4s-jobq[track]`).

### Workflow module (`ai4s.jobq.workflow`)

DAG-based task orchestration on top of the core jobq infrastructure.
The runtime layout is one JSON state blob per workflow plus one small
index Table row — exactly one coordinator process is supported per
prefix.

- **`entities.py`** — `WorkflowDefinition`, `WorkflowTask`, `WorkflowStatus`, `TaskStatus`,
  `WorkflowCompletion` (includes `attempt_no` for retry idempotency), plus
  `TaskState`/`WorkflowState` enums and `DepPolicy`/`ResultPolicy`.
- **`state.py`** — Pure in-memory model: `WorkflowRuntime` and `TaskRuntime` dataclasses
  with `apply_completion`, `from_definition`, `to_json`/`from_json`, `ready_tasks`,
  `is_terminal`, `request_cancel`. No IO; unit-testable without Azure.
- **`persistence.py`** — `WorkflowPersistence`: async wrapper over the runtime-state blob
  container (`{prefix}-workflows`) plus the index Table (`{prefix}WorkflowsIndex`).
  All blob writes are ETag-CAS guarded via `_retry_with_etag`. Exposes
  `submit`, `load`, `store`, `request_cancel`, `list_workflows`, `list_recent_terminal`,
  `list_tasks`, `summary`, `purge`, `fetch_output`, `stash_output`,
  `discover_queues`, `drain_queues`.
- **`coordinator.py`** — Single-process event loop. Pulls completions in batches
  (`--batch-size`, default 32), applies them to in-memory `WorkflowRuntime`, dispatches
  newly-READY tasks via the JobQ pool, flushes the state blob under ETag CAS, then acks
  the batch. Runs a periodic ready-repair sweep
  (`--ready-sweep-interval-s` / `--ready-repair-threshold-s`) that re-pushes tasks stuck
  in READY (covers the submit-or-retry crash window). Polls the index for
  `cancel_requested=true` flags via `--cancel-poll-interval-s`.
- **`client.py`** — `WorkflowClient`: high-level async client built on
  `WorkflowPersistence` + a JobQ-backed task push pool. Provides `submit`,
  `submit_batch`, `status`, `watch`, `cancel`, `retry`, `list_workflows`,
  `list_recent_terminal`, `list_tasks`, `summary`, `purge`, `total_workflows`.
- **`worker.py`** — `WorkflowShellCommandProcessor`: auto-selected when
  `JOBQ_WORKFLOW_PREFIX` is set. Reads `__workflow_id` / `__workflow_task` (and legacy
  `__task_name`) from the task message, exposes them as `JOBQ_WORKFLOW_*` env vars to
  the subprocess, sends completions with `attempt_no`, polls cancel via the index row,
  handles blob-stashing of large outputs.
- **`context.py`** — User-script API: `WorkflowContext`, `set_output`,
  `get_upstream_output`, `get_upstream_outputs`, `is_cancelled`, `get_real_upstream_tasks`
  (sync wrappers around the async `_LazyWorkflowContext` implementation in worker.py).
- **`transforms.py`** — `sequentialize_fan_in` (used by `workflow submit --max-fan-in`).
  Rewrites high-fan-in subgraphs into sequential batches via lightweight
  `__batch_merge` nodes (`kwargs={"__batch_merge": True, ...}`) auto-completed by the
  worker. `get_real_upstream_tasks()` walks past these merge nodes.
- **`condition.py`** — Condition expression evaluator for conditional task execution.
- **`ids.py`** — Naming helpers. State blob container: `{prefix.lower()}-workflows`.
  Output container: `{prefix.lower()}-outputs`. Index table: `{prefix}WorkflowsIndex`.
  Completion queue: `{prefix.lower()}-workflow-completions`. Deterministic task message
  ID: `task_message_id(wf_id, task_name, attempt_no)`.
- **`stash.py` / `_stash_io.py`** — Blob-output stashing for large task outputs.
- **`env.py`** — Environment-variable parsing helpers.
- **`cli/__init__.py`** — Workflow CLI subcommands built with `asyncclick`:
  `submit`, `validate`, `status`, `watch`, `logs`, `list`, `tasks`, `cancel`, `retry`,
  `coordinator`, `summary`, `purge`, `track`. `_get_client() -> WorkflowClient` is the
  shared client factory.
- **`cli/_doctor.py`** — `workflow doctor`: lists stuck workflows
  (`--stale-pending-sec`, default 120).
- **`cli/_explain.py`** — `workflow explain`: human-readable error explanations.
- **`cli/_shared.py`** — Shared CLI option groups and helpers.

### Key patterns

- **Async-first**: All queue operations are async. Tests use `pytest-asyncio` with `--asyncio-mode=auto` (no need for `@pytest.mark.asyncio`).
- **Async context managers**: `JobQ` and `WorkflowClient` instances must be used as
  `async with` context managers for proper resource cleanup.
- **Protocol-based backends**: `backend/common.py` defines Protocol classes; new backends implement these without inheritance.
- **Namespace package**: `ai4s/` uses `pkgutil.extend_path` — the `ai4s/__init__.py` must not contain regular imports.
- **Environment-driven config**: `JOBQ_STORAGE`, `JOBQ_QUEUE`, `JOBQ_USE_MONTY_JSON`,
  `JOBQ_DETERMINISTIC_IDS` for core jobq; `JOBQ_WORKFLOW_PREFIX`, `JOBQ_WORKFLOW_QUEUES`,
  `JOBQ_WORKFLOW_BLOBS`, `JOBQ_WORKFLOW_CONFIG` (shared `jobq.yaml`),
  `JOBQ_COORDINATOR_BATCH_SIZE`, `JOBQ_COORDINATOR_VISIBILITY_TIMEOUT_S`,
  `JOBQ_COORDINATOR_READY_SWEEP_INTERVAL_S`,
  `JOBQ_COORDINATOR_READY_REPAIR_THRESHOLD_S` for workflow modules.
- **Single coordinator per prefix**: A blob-lease guards the coordinator
  (`_lease.py`), so a second coordinator against the same `JOBQ_WORKFLOW_PREFIX`
  prefix waits for the lease rather than corrupting state. A crashed coordinator's
  lease expires after ~60s; use `workflow break-lease` (or `coordinator --break-lease`)
  to clear it immediately. Still deploy a single replica — the lease is a safety net,
  not a scheduler.
- **ETag-guarded blob writes**: `WorkflowPersistence` flushes the runtime-state blob via
  `_retry_with_etag`; coordinator and ready-repair sweep use it for every state change.
  Retry budget bounded by `--flush-retry-limit` (default 2).
- **Retry semantics**: Coordinator-driven. Worker emits one completion per delivery; the
  coordinator inspects `attempt_no` vs `num_retries` and either rearms the task with a
  new message ID (`task_message_id(wf, task, attempt_no+1)`) or marks it `failed`.
- **Strict typing**: Prefer concrete types (`TableEntity`, `Envelope`, `WorkflowTask`,
  `WorkflowStatus`, `WorkflowRuntime`) over `Any`. Use `TYPE_CHECKING` imports for Azure
  SDK types to avoid runtime import costs.

## Public Repository Policy

This is a **public** Microsoft repository. Never include internal or customer-specific
information in commit messages, PR descriptions, comments, or code:

- No Azure subscription IDs, resource group names, workspace names, or run/job IDs
- No internal hostnames or ingestion endpoints (e.g. `*.in.applicationinsights.azure.com`)
- No specific customer or team names, internal project codenames
- No specific region + SKU combinations that reveal internal infrastructure
- Keep descriptions generic: say "certain GPU SKUs" not "MI200 nodes in westus3"

## Command Output

When running long-running shell commands (builds, tests, lints), capture full
output to a temp file instead of discarding it with bare `| tail`. Use
`| tee /tmp/last-cmd.log | tail -n 50` so the complete output is recoverable
with `cat /tmp/last-cmd.log` if the truncated view isn't enough for debugging.

## Conventions

- Copyright header `# Copyright (c) Microsoft Corporation.` + `# Licensed under the MIT License.` at the top of every source file.
- Python 3.10+ required. Type hints are used throughout; mypy is configured with `strict_optional = true`.
- Logging uses `logging.getLogger("ai4s.jobq")` (or `__name__` in submodules).
- `asyncclick` (not standard `click`) for all CLI commands.
- CHANGELOG entries for unreleased versions may include today's or a future date (the release date). Do not replace such dates with "(unreleased)".
