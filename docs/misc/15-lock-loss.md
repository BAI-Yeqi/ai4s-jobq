# Lock loss

When a worker is processing a task, it periodically renews the message lock (visibility
timeout for Storage Queue, lock renewal for Service Bus). If renewal fails for long enough
that the lock expires, the message becomes available to other workers—causing potential
duplicate execution.

## What happens on lock loss

When the worker detects that the lock is lost, it immediately terminates the running
subprocess to prevent the task from continuing without exclusive ownership of the message.

By default, the entire subprocess process group receives **SIGKILL**—instant termination
that cannot be caught or ignored. This is the safest option because:

- The lock is already gone, so checkpointing would race with another worker.
- SIGKILL guarantees the process stops immediately.
- The process group kill ensures child processes are also terminated.

## Configuring lock-loss behavior

If your task can benefit from a brief graceful shutdown even after lock loss (for example,
to flush partial results or release external resources that would otherwise leak), you can
configure SIGTERM instead:

| Environment variable | Default | Description |
|---|---|---|
| `JOBQ_LOCK_LOST_BEHAVIOR` | `sigkill` | Signal sent to the subprocess on lock loss. `sigkill` sends SIGKILL to the entire process group (immediate termination). `sigterm` sends SIGTERM to the subprocess (allows signal handlers to run). |

```shell
JOBQ_LOCK_LOST_BEHAVIOR=sigterm ai4s-jobq myaccount/myqueue worker --num-workers 2
```

```{warning}
With `sigterm`, a subprocess that ignores or mishandles SIGTERM will continue running after
lock loss, potentially causing duplicate execution. Use `sigkill` (the default) unless you
have a specific reason to allow graceful cleanup.
```

## How lock loss is detected

Both backends track time since the last successful lock renewal:

- **Storage Queue:** If no successful `update_message` (heartbeat) within the visibility
  timeout, `lock_lost_event` is set.
- **Service Bus:** If no successful `renew_lock` within the lock duration, or if renewal
  returns HTTP 404, `lock_lost_event` is set.

Once `lock_lost_event` is set, `pull_and_execute` cancels the running callback task, which
triggers the subprocess termination via the configured signal.

## Message redelivery after lock loss

The two backends differ in where a message reappears after its lock expires:

- **Storage Queue:** The message reappears at the **end** of the queue, behind any messages
  that were enqueued after it.
- **Service Bus:** The message reappears at the **beginning** of the queue and is delivered
  to the next available worker immediately.

If your workload does not tolerate simultaneous delivery of the same task to multiple
workers—even briefly—Storage Queue is the safer choice. The delay before redelivery
gives the terminated subprocess time to exit before another worker picks up the message.
