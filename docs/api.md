# Python API

The Python API is mainly interesting if you want to run small tasks where the
shell has too much overhead. Examples are:

- You want to interleave data loading/storing and processing
- You're I/O constrained and want to run *many* tasks in parallel

You get the most out of the Python API if you write (...and your performance
depends on...) `async` code, but it's not strictly necessary.


## Workflows

The workflow CLI ([`ai4s-jobq workflow ...`](workflows.md)) is the
recommended way to submit and operate workflows. Use the Python API
when you want programmatic submission, custom processors, or when
integrating workflow tasks into a longer-running Python service. The
YAML schema, CLI operations, dependency policies, and architecture are
covered in the [workflow reference](workflows.md).

### Submitting workflows from Python

You can construct workflow definitions directly in Python when a service
or notebook already owns the pipeline structure:

```python
from ai4s.jobq.workflow import WorkflowDefinition, WorkflowTask

workflow = WorkflowDefinition(
    name="featurize-and-train",
    tasks=[
        WorkflowTask(
            name="featurize",
            kwargs={"input": "abfs://data/raw.parquet", "output": "abfs://data/features.parquet"},
        ),
        WorkflowTask(
            name="train-gnn",
            kwargs={"model": "schnet", "epochs": 100, "data": "abfs://data/features.parquet"},
            depends_on=["featurize"],
            queue="gpu-a100",
        ),
        WorkflowTask(
            name="train-rf",
            kwargs={"model": "random-forest", "data": "abfs://data/features.parquet"},
            depends_on=["featurize"],
            queue="cpu-64core",
        ),
        WorkflowTask(
            name="compare",
            kwargs={"metric": "mae"},
            depends_on=["train-gnn", "train-rf"],
        ),
    ],
    default_queue="cpu-general",
)
```

Submit and inspect workflows with `WorkflowClient`:

```python
from ai4s.jobq.workflow import WorkflowClient

async with await WorkflowClient.from_environment() as client:
    wf_id = await client.submit(workflow)

    status = await client.status(wf_id)
    for name, task in status.tasks.items():
        print(f"{name}: {task.status}")

# With a custom table prefix:
async with await WorkflowClient.from_environment(prefix="MyProject") as client:
    ...
```

Task-name prefix filters use the workflow's runtime-state blob and
are O(num_tasks)—fast for typical workflows (hundreds to low
thousands of tasks). To inspect a subset of tasks programmatically:

```python
async with await WorkflowClient.from_environment() as client:
    train_tasks = await client.list_tasks(
        wf_id, name_prefix="train-"
    )
```

### Producing outputs from Python tasks

Scripts and processors use `set_output()` to return JSON-serializable
data to downstream workflow tasks. The shell-worker examples in the
[workflow reference](workflows.md#producing-and-consuming-task-outputs)
show the CLI-adjacent path; the same helpers are useful from Python code
that runs inside a workflow task.

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

Downstream tasks can fetch the output of completed dependencies:

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

When a task has `dep_policy="any"` or an integer policy, use
`get_upstream_outputs()` to discover which dependencies actually
succeeded:

```python
from ai4s.jobq.workflow.context import get_upstream_outputs

outputs = get_upstream_outputs()
# outputs = {"train-rf": {"mae": 0.42}}  (train-gnn may have failed)
for model_name, result in outputs.items():
    print(f"{model_name}: MAE={result['mae']}")
```

Long-running Python tasks can also poll for cancellation:

```python
from ai4s.jobq.workflow.context import is_cancelled, set_output

for epoch in range(1000):
    train_one_epoch()
    if is_cancelled():
        set_output({"last_epoch": epoch})
        break
else:
    set_output({"mae": 0.03, "checkpoint": "model.pt"})
```

### File outputs with BlobStasher and BlobStash

Use file outputs when a task produces a real file that downstream tasks
need to consume, such as a model checkpoint, parquet file, or tarball.
Keep small metadata in regular JSON output, but wrap large files with
`BlobStasher.from_file(path)` so the workflow worker uploads the file
once and downstream tasks can download it lazily.

Producer tasks put a `BlobStasher` marker inside `set_output()`:

```python
#!/usr/bin/env python3
from ai4s.jobq.workflow import BlobStasher
from ai4s.jobq.workflow.context import set_output

# ... train model and write model.pt ...

set_output(
    {
        "checkpoint": BlobStasher.from_file("model.pt"),
        "mae": 0.03,
    }
)
```

No upload happens inside `set_output()`. The worker reads the output
after the script exits, uploads each marked file, and then records a
reference for downstream tasks. File outputs require
`JOBQ_WORKFLOW_BLOBS=<account>/<container>`. If it is not configured,
using `BlobStasher.from_file(...)` fails the task with a clear error.

Downstream tasks receive a lazy `BlobStash` reference:

```python
#!/usr/bin/env python3
from ai4s.jobq.workflow.context import get_upstream_output

train = get_upstream_output("train")
checkpoint = train["checkpoint"]

checkpoint.download_to("local/model.pt")  # write to a local path
weights = checkpoint.read_bytes()         # or read all bytes into memory

with checkpoint.open("rb") as f:          # or use a file-like object
    header = f.read(16)
```

`BlobStash` also exposes `url`, `blob_name`, `md5`, and `size` for
logging or validation. Files land in the configured container under
`workflow-files/{workflow_id}/{task_name}/{filename}`; for example,
`jobq-workflow-data/workflow-files/wf123/train/model.pt`.

Filenames are preserved. If the same task uploads the same filename
again, the later upload overwrites the earlier blob. This matches
workflow retry semantics: retries are at least once, and a successful
retry can replace the file from an earlier attempt.

Current limitations:

- File outputs support single files only; directory uploads are not yet
  supported.
- Purging a workflow does not garbage-collect uploaded file blobs.

### Custom workflow processors

When `JOBQ_WORKFLOW_PREFIX` is set, CLI workers that use `--processor shell`
automatically upgrade to `WorkflowShellCommandProcessor`. Python services
can instantiate the processor directly when they launch workers in
process:

```python
from ai4s.jobq import JobQ, launch_workers
from ai4s.jobq.workflow import WorkflowShellCommandProcessor

async with JobQ.from_environment() as jobq:
    async with WorkflowShellCommandProcessor(
        num_workers=4,
        emulate_tty=True,
        completion_queue="myproject-workflow-completions",
    ) as processor:
        await launch_workers(jobq, processor)
```

If you write a Python `Processor` class directly instead of shell
commands, use `WorkflowContext.from_kwargs()`:

```python
from ai4s.jobq.work import Processor
from ai4s.jobq.workflow.context import WorkflowContext
from ai4s.jobq.workflow.entities import WorkflowCompletion, serialize_output


class TrainProcessor(Processor):
    async def __call__(self, **kwargs):
        wf_id = kwargs.pop("__workflow_id", None)
        task_name = kwargs.pop("__task_name", None)

        if wf_id:
            async with await WorkflowContext.from_kwargs(
                {"__workflow_id": wf_id, "__task_name": task_name}
            ) as ctx:
                features = await ctx.get_upstream_output("featurize")
                # ... train model ...

        # NOTE: With a custom Processor you must send the completion
        # yourself (or wrap with WorkflowShellCommandProcessor).
```

For custom processors, consider using `WorkflowShellCommandProcessor` as
a reference for the completion-sending logic. `WorkflowContext` also has
an async API for in-process access:

```python
async with await WorkflowContext.from_kwargs(kwargs) as ctx:
    upstream = await ctx.get_upstream_output("preprocess")
    outputs = await ctx.get_available_upstream_outputs()
    cancelled = await ctx.is_cancelled()
```

## Queueing and Running

Rather than submitting tasks one by one using the CLI as shown in the basic example, you can implement the `ai4s.jobq.orchestration.WorkSpecification` protocol in Python to list all your tasks quickly.

```python
from azure.identity import AzureCliCredential
from ai4s.jobq import JobQ, WorkSpecification


class NumberSquaring(WorkSpecification):
  async def list_tasks(self, seed=None, force=False):
    # Here you define the tasks that you want to run.
    # Tasks are dictionaries that will be passed to the __call__ method below.
    # See the WorkSpecification protocol for more options.
    for i in range(10):
      yield dict(my_number=i)  # kwargs of the `square()` method below


work_specification = NumberSquaring()
```

You then enqueue tasks like this:

```python
from ai4s.jobq import batch_enqueue
from ai4s.auth import get_token_credential


async with get_token_credential() as cred:
  async with JobQ.from_storage_queue("test-queue", storage_account="mystorageaccount", credential=cred) as jobq:
    await batch_enqueue(jobq, work_specification)
    # or, equivalently:
    await batch_enqueue(jobq, [dict(my_number=i) for i in range(10)])  # kwargs of the `square()` method below
```

And running multiple workers (in parallel with asyncio) looks like this:

```python
from ai4s.jobq import launch_workers, SequentialProcessor


async def square(my_number):
    # Here you define the work that you want to do for each task.
    print(f"{my_number} squared is {my_number**2}.")


async with get_token_credential() as cred:
  async with JobQ.from_storage_queue("test-queue", storage_account="ai4science0eastus", credential=cred) as jobq:
    await launch_workers(
      jobq,
      square,
      num_workers=10
    )
```

To use Azure Service Bus instead of Storage Queues, swap the constructor.
The rest of the API (``batch_enqueue``, ``launch_workers``, etc.) stays the same:

```python
from ai4s.auth import get_token_credential


async with get_token_credential() as cred:
  async with JobQ.from_service_bus("test-queue", fqns="mysb.servicebus.windows.net", credential=cred) as jobq:
    ...
```

Note that `square` is `async`, but does not do any asynchronous operations and
never yields control. This blocks jobq from telling the backend that the task
is still being processed ("heartbeat"). That's  OK if it is running for less
than the configured visibility timeout.

If your task runs longer than the configured visibility timeout, the backend
may consider the worker as crashed and give the same task to another worker. In
these cases, use the `SequentialProcessor` wrapper, which offloads the
computation to a separate process. This allows jobq to send a heartbeat to the
backend in regular intervals.

```python
from ai4s.jobq import SequentialProcessor


# NOTE: *not* async here!
def square(my_number):
    # Here you define the work that you want to do for each task.
    print(f"{my_number} squared is {my_number**2}.")


async with JobQ.from_storage_queue("test-queue", storage_account="ai4science0eastus", credential=AzureCliCredential()) as jobq:
  async with SequentialProcessor(square) as processor:
    await launch_workers(jobq, processor)
```

## Multiple Workers

If your tasks are genuinely asynchronous, that is, they mostly call asynchronous APIs, you can just set `num_workers=5` etc. when calling launch_workers.

Otherwise, you can use a `ProcessPool` to make sure computationally intensive work can be parallelized and does not block the queue.

```python
import os
import time
from functools import partial
from ai4s.jobq import WorkSpecification, ProcessPool, Processor


class NumberSquaring(WorkSpecification):
  async def list_tasks(self, seed=None, force=False):
    for i in range(10):
      await self.pool.submit(partial(time.sleep, 5))  # Compute intensive task
      yield dict(my_number=i)   # kwargs of the processor's __call__ below


class NumberSquaringProcessor(Processor):
  def __init__(self):
    super().__init__()
    self.pool = ProcessPool(pool_size=os.cpu_count())
    self.register_context_manager(self.pool)

  async def __call__(self, my_number):
    await self.pool.submit(partial(time.sleep, 5))  # Compute intensive task
    print(f"{my_number} squared is {my_number**2}.")


async with JobQ.from_storage_queue("test-queue", storage_account="ai4science0eastus", credential=cred) as jobq:
  async with NumberSquaringProcessor() as processor:
    await launch_workers(jobq, processor)
```

## Multi-Worker Logging

You can contextualize your logs by writing worker and job ID. To achieve this,
add the magic `_job_id` and `_worker_id` string parameters to your callback:

```python
  async def __call__(self, my_number: int, _job_id: str, _worker_id: str):
    logger = logging.getLogger(f"task.{_worker_id}.{_job_id}")
    logger.info("Working on %d", my_number)
    ...
```


## Writing an entry point

A common use case is to simply call a function for every item in the queue:

```python
import asyncio
from ai4s.jobq import SequentialProcessor, launch_workers, JobQ, setup_logging


def my_cpu_intensive_work(**kwargs):
  ...


async def main():
  async with JobQ.from_environment() as jobq:
    setup_logging(jobq.full_name)
    async with SequentialProcessor(my_cpu_intensive_work) as proc:
      await launch_workers(jobq, proc)

asyncio.run(main())
```

The `kwargs` correspond to the `dict` you queued. `my_cpu_intensive_work` is
automatically run in a process pool of size 1. The `from_environment`
constructor is a shortcut that relies on the environment variables set by
`ai4s-jobq QUEUE_SPEC amlt`: `JOBQ_STORAGE` and `JOBQ_QUEUE`.

## Working with blob storage efficiently

```python
from ai4s.jobq import WorkSpecification
from ai4s.jobq.blob import BlobContainer
from tempfile import TemporaryDirectory
import os


class BlobSizeCounting(WorkSpecification):
  def __init__(self):
    super().__init__()
    self.container = BlobContainer(storage_account="mystorageaccount", container="my-data")
    self.register_context_manager(self.container)

  async def task_seeds(self):
    # You can use top-level directories in the blob storage container to parallelize the listing of blobs.
    # `list_tasks` will be called in parallel for each of these 'seeds'.
    walk = self.container.client.walk_blobs(name_starts_with="")
    async for directory in walk:
      yield directory.name

  async def list_tasks(self, seed, force=False):
    async for blob in self.container.client.list_blobs(name_starts_with=seed):
      yield {"blob": blob.name}

  # Note: we include __call__ here in the WorkSpecification because it's
  # logically related to how tasks are listed. You can keep the Processor entirely
  # separate though, if you prefer.
  async def __call__(self, blob, **kwargs):
    # Download the blob and report its size (just as an example).
    with TemporaryDirectory(dir="/dev/shm") as tmpdir:
      filename = await self.container.download_file(blob, tmpdir)
      print(f"{blob} is {os.path.getsize(filename)} bytes.")

work_specification = BlobSizeCounting()
```

Similar to `download_file`, there's also `upload_file` and `upload_from_folder`.
The latter uploads all files in the folder concurrently.

## Checking if work has already been done before enqueuing

You can overload the `already_done` method to prevent a listed item from queueing.
This is useful when checking the item takes time. There's a separate worker
pool that does this checking, increasing the overall efficiency. The size of
the worker pools for `list_task` and `already_done` can be parameterized with the
`batch_enqueue` function.

```python
class ZipAll(WorkSpecification):
  def __init__(self):
    super().__init__()
    self.container = BlobContainer(storage_account="mystorageaccount", container="my-data")
    self.register_context_manager(self.container)

  async def list_tasks(self, seed, force=False):
    async for blob in self.container.client.list_blobs(name_starts_with=""):
      if not blob.name.endswith(".zip")
        yield {"blob": blob.name}

  async def already_done(self, blob: str):
    # return value True will cause the item not to be queued.
    return await self.container.blob_exists(blob + ".zip")

work_specification = ZipAll()
```
