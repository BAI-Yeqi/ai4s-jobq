# ai4s.jobq.orchestration

Build and operate fleets of Azure ML workers that drain an `ai4s.jobq` queue.

`ai4s.jobq` itself is the queue + worker primitive; this package is the
**orchestration layer** on top. It submits worker jobs, scales them to queue
depth, hardens the images they run, and shuts them down gracefully.

## High-level API

The package entry point re-exports the "easy mode" helpers from `manager.py`:

```python
from ai4s.jobq.orchestration import batch_enqueue, launch_workers, get_results
```

`batch_enqueue` pushes a batch of work, `launch_workers` starts workers to drain
it, and `get_results` collects the outputs.

## Fleet control

- **`Workforce`** (`workforce.py`): manages one experiment's worker group on a
  single AML compute target. Inspect live state, call `scale_to(n)`, or
  `hire` / `lay_off` / `resume`. Each worker job is built by `_build_worker`,
  which also applies the image hardening below.
- **`MultiRegionWorkforce`** (`multiregion_workforce.py`): spreads a fleet
  across many regions / targets and scales them together, gathering per-region
  state with bounded parallelism.

## Image hardening (applied at hire time)

Both are optional and shared across a fleet via constructor arguments or
`set_image_resolver` / `set_image_scanner`. When both are used, digest pinning
runs first, since the scan resolves its record by digest.

- **`ImageDigestResolver`** (`image_resolver.py`): pins a mutable ACR tag to its
  immutable content digest (`repo:tag` becomes `repo@sha256:...`) so every
  worker in a session runs identical image content. TTL-cached; ACR-only.
- **`ImageScanner`** (`image_scanner.py`): scans the worker image for
  vulnerabilities before submitting. A clean image is stamped on the job with
  the scanner version (the `fedramp.scan-version` property); a blocking finding
  aborts the hire. `fail_open` by default, so a scan that cannot complete
  submits the worker without a stamp rather than stalling the fleet. TTL-cached;
  ACR-only. Needs the optional `fedramp-scanner` extra
  (`pip install ai4s-jobq[scan]`).

## Graceful shutdown

- **`workforce_monitor`** (`workforce_monitor.py`): an async context manager a
  worker runs under. It listens on a Service Bus control topic for shutdown
  events and writes a PID file so the worker can be sent a signal.
- **`PidFile`, `send_signal`, `send_signal_by_glob`** (`pid_file.py`): PID-file
  bookkeeping and signal delivery used by the monitor.
