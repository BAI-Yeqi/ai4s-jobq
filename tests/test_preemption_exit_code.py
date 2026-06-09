# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests that a non-zero exit code is ignored when preemption is in progress.

Scenario (production):
    1. PreemptionEventHandler detects a scheduled preemption event and sets
       ``shutdown_event``.
    2. The manager's worker loop detects ``shutdown_event``, calls
       ``processor.shutdown()`` which sends SIGUSR1 → pool child → SIGTERM
       to the subprocess.
    3. User code catches SIGTERM, performs cleanup, then exits non-zero
       (e.g., raises RuntimeError after checkpointing).
    4. The task should be requeued WITHOUT decrementing its retry count,
       because the failure was caused by preemption — not a real error.

Bug:
    Sometimes, despite the preemption signal chain firing correctly, the
    non-zero exit code is treated as a genuine task failure and the retry
    count is decremented.
"""

import asyncio
import textwrap
from concurrent.futures.process import BrokenProcessPool
from contextlib import suppress
from datetime import timedelta
from unittest.mock import patch

import pytest

from ai4s.jobq import JobQ
from ai4s.jobq.entities import Task, WorkerCanceled
from ai4s.jobq.work import ProcessPool, ShellCommandProcessor

# ---------------------------------------------------------------------------
# Test 1: Normal signal chain — processor.shutdown() → SIGUSR1 → SIGTERM.
#   Subprocess catches SIGTERM, exits non-zero. WorkerCanceled must be raised.
# ---------------------------------------------------------------------------


async def test_preemption_via_signal_chain_raises_worker_canceled():
    """When processor.shutdown() is called and the subprocess exits non-zero
    after receiving SIGTERM through our signal chain, WorkerCanceled must be
    raised — not RuntimeError."""
    # Use "sleep & wait" so bash can process SIGTERM immediately
    # (SIGTERM to bash PID doesn't interrupt a foreground `sleep` process).
    script = textwrap.dedent("""\
        trap 'echo "got SIGTERM, handling..."; exit 1' TERM
        echo "running"
        sleep 60 &
        wait
    """).strip()

    async with ShellCommandProcessor(num_workers=1) as proc:
        task = asyncio.create_task(proc(cmd=script, _job_id="test-preempt-1"))

        # Wait for the subprocess to start
        await asyncio.sleep(1.0)

        # Simulate preemption: shutdown sends SIGUSR1 → pool child → SIGTERM → subprocess
        await proc.shutdown()

        # The task should raise WorkerCanceled (not RuntimeError)
        with pytest.raises(WorkerCanceled):
            await task


# ---------------------------------------------------------------------------
# Test 2: Full integration with queue — verify retry count is preserved when
#   preemption is triggered via processor.shutdown() (simulating what the
#   manager does after PreemptionEventHandler sets the shutdown_event).
# ---------------------------------------------------------------------------


async def test_preemption_preserves_retry_count(azurite_connstr):
    """After preemption, the task must be requeued with its original retry
    count intact (not decremented)."""
    num_retries = 3

    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        await q.clear()
        await q.push(
            {"cmd": "trap 'sleep 0.2; exit 1' TERM; echo running; sleep 60 & wait"},
            num_retries=num_retries,
        )

    # Process the task with ShellCommandProcessor, then trigger preemption
    async with (
        ShellCommandProcessor(num_workers=1) as proc,
        JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q,
    ):
        worker_task = asyncio.create_task(
            q.pull_and_execute(
                proc,
                visibility_timeout=timedelta(seconds=30),
                with_heartbeat=False,
            )
        )

        # Wait for the subprocess to start
        await asyncio.sleep(1.5)

        # Trigger preemption via our signal chain
        await proc.shutdown()

        # pull_and_execute should raise WorkerCanceled (requeue happened inside)
        with pytest.raises(WorkerCanceled):
            await worker_task

    # Wait for the message to become visible again (requeue makes it
    # immediately visible on Storage Queues).
    await asyncio.sleep(2)

    # Check that the task is back in the queue with the ORIGINAL retry count.
    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        msgs = await q.peek(1)
        assert len(msgs) >= 1, "Task should be back in the queue after preemption"
        task = Task.deserialize(msgs[0].content)
        assert task.num_retries == num_retries, (
            f"Retry count should be preserved after preemption. "
            f"Expected {num_retries}, got {task.num_retries}. "
            f"This means the preemption was incorrectly treated as a failure."
        )


# ---------------------------------------------------------------------------
# Test 3: Simulate the full manager worker loop with shutdown_event,
#   replicating what happens when PreemptionEventHandler detects a scheduled
#   event.  This exercises the exact code path used in production.
# ---------------------------------------------------------------------------


async def test_scheduled_event_preemption_manager_loop(azurite_connstr):
    """Simulate the manager's worker loop: a task is running, the
    PreemptionEventHandler sets shutdown_event, the manager calls
    processor.shutdown(), and the subprocess exits non-zero after handling
    SIGTERM.  The task must be requeued without retry decrement.

    This replicates the code path in orchestration/manager.py lines 606-676.
    """
    num_retries = 3

    # Subprocess that traps SIGTERM, does some "checkpointing", then exits 1.
    # Uses "sleep & wait" so bash can handle SIGTERM immediately.
    script = textwrap.dedent("""\
        checkpoint_and_exit() {
            echo "SIGTERM received, checkpointing..."
            echo "checkpoint done, raising error to signal incomplete work"
            exit 1
        }
        trap checkpoint_and_exit TERM
        echo "running task"
        sleep 60 &
        wait
    """).strip()

    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        await q.clear()
        await q.push({"cmd": script}, num_retries=num_retries)

    async with (
        ShellCommandProcessor(num_workers=1) as proc,
        JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q,
    ):
        # --- Simulate the manager's worker loop ---
        # This replicates manager.py lines 606-676

        shutdown_event = asyncio.Event()

        shutdown_event_task = asyncio.create_task(shutdown_event.wait(), name="shutdown-event")

        worker_task = asyncio.create_task(
            q.pull_and_execute(
                proc,
                visibility_timeout=timedelta(seconds=30),
                with_heartbeat=False,
            ),
            name="worker",
        )

        # Wait for the subprocess to start running
        await asyncio.sleep(1.5)

        # --- Simulate PreemptionEventHandler detecting a scheduled event ---
        shutdown_event.set()

        # --- Manager logic: wait for either task to complete ---
        done, pending = await asyncio.wait(
            [worker_task, shutdown_event_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # --- Manager logic: handle preemption (pass_signal_to_subprocess=True) ---
        if shutdown_event_task in done:
            # This is the path taken when PreemptionEventHandler fires
            await proc.shutdown()
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError, WorkerCanceled):
                    await task
        else:
            # worker_task completed before shutdown was detected — this is
            # the race condition scenario
            shutdown_event_task.cancel()
            with suppress(asyncio.CancelledError):
                await shutdown_event_task

    # Check that the task is back in the queue with preserved retry count
    await asyncio.sleep(2)
    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        msgs = await q.peek(1)
        assert len(msgs) >= 1, "Task should be back in the queue after preemption"
        task = Task.deserialize(msgs[0].content)
        assert task.num_retries == num_retries, (
            f"Retry count should be preserved after scheduled event preemption. "
            f"Expected {num_retries}, got {task.num_retries}. "
            f"The non-zero exit code after SIGTERM handling was incorrectly "
            f"treated as a task failure."
        )


# ---------------------------------------------------------------------------
# Test 4: Race condition — the subprocess exits non-zero BEFORE the manager
#   detects the shutdown_event and calls processor.shutdown().
#
#   This happens when:
#   - The subprocess independently detects preemption (e.g., its own scheduled
#     events polling) and exits before our signal chain fires.
#   - OR: The PreemptionEventHandler poll interval means there's a window
#     where the subprocess can fail before shutdown_event is set.
#   - OR: worker_task wins the race against shutdown_event_task in
#     asyncio.wait (both complete at approximately the same time).
# ---------------------------------------------------------------------------


async def test_preemption_race_worker_completes_before_shutdown(azurite_connstr):
    """Simulate the race where the subprocess exits non-zero while preemption
    is already detected (shutdown_event set) but processor.shutdown() hasn't
    been called yet.

    Previously, the non-zero exit code was treated as a genuine failure and
    retries were decremented.  With the fix, ProcessPool.submit() checks
    shutdown_event after the subprocess exits and raises WorkerCanceled,
    causing pull_and_execute to requeue without retry decrement.
    """
    num_retries = 3

    # Script that simulates user code detecting preemption: catches SIGTERM,
    # performs cleanup, then exits non-zero (like raising RuntimeError in Python).
    script = textwrap.dedent("""\
        echo "running"
        sleep 0.5
        echo "exiting with error (simulating preemption-triggered RuntimeError)"
        exit 1
    """).strip()

    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        await q.clear()
        await q.push({"cmd": script}, num_retries=num_retries)

    # Simulate the scenario: PreemptionEventHandler has already detected
    # preemption and set the shutdown_event, but the manager hasn't yet
    # called processor.shutdown().
    shutdown_event = asyncio.Event()
    shutdown_event.set()  # Preemption detected before subprocess exits

    async with ShellCommandProcessor(num_workers=1) as proc:
        # Wire the shutdown_event into the pool (as the manager does)
        proc.pool._shutdown_event = shutdown_event

        async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
            # pull_and_execute should catch WorkerCanceled (from the pool's
            # shutdown_event check) and requeue without decrementing retries.
            with pytest.raises(WorkerCanceled):
                await q.pull_and_execute(
                    proc,
                    visibility_timeout=timedelta(seconds=30),
                    with_heartbeat=False,
                )

    # Check retry count — should be preserved
    await asyncio.sleep(1)
    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        msgs = await q.peek(1)
        assert len(msgs) >= 1, "Task should be back in the queue"
        task = Task.deserialize(msgs[0].content)

        assert task.num_retries == num_retries, (
            f"Retry count should be preserved when preemption caused the exit. "
            f"Expected {num_retries}, got {task.num_retries}. "
            f"BUG: The subprocess detected preemption independently and exited "
            f"before our signal chain could mark it as a preemption."
        )


# ---------------------------------------------------------------------------
# Test 5: BrokenProcessPool — the pool child is killed by process-group
#   SIGTERM (default handler), causing run_in_executor to raise
#   BrokenProcessPool instead of returning a non-zero exit code.
#
#   This happens when:
#   - The pool child hasn't installed a custom SIGTERM handler (or the default
#     handler fires before our SIGUSR1-based signal chain).
#   - The OS sends SIGTERM to the entire process group (e.g., cgroup kill).
#   - The pool child is killed while run_in_executor is awaiting it.
#
#   Without a BrokenProcessPool handler, this exception propagates to
#   pull_and_execute's generic `except Exception`, which decrements retries.
# ---------------------------------------------------------------------------


async def test_broken_process_pool_during_preemption_raises_worker_canceled():
    """When the pool child is killed during preemption (raising
    BrokenProcessPool), the exception must be converted to WorkerCanceled
    if _shutdown_event is set — not propagated as a generic failure."""

    shutdown_event = asyncio.Event()
    shutdown_event.set()

    async with ProcessPool(pool_size=1) as pool:
        pool._shutdown_event = shutdown_event

        # Patch run_in_executor to simulate BrokenProcessPool (as if the pool
        # child was killed by a process-group SIGTERM).
        async def raise_broken_pool(*args, **kwargs):
            raise BrokenProcessPool("a]process in the process pool was terminated abruptly")

        with (
            patch.object(asyncio.get_running_loop(), "run_in_executor", raise_broken_pool),
            pytest.raises(WorkerCanceled),
        ):
            await pool.submit(lambda: 0)


async def test_broken_process_pool_without_preemption_propagates():
    """When BrokenProcessPool is raised but preemption is NOT in progress,
    the exception must propagate normally (not be swallowed)."""

    async with ProcessPool(pool_size=1) as pool:
        # No shutdown_event set — this is a genuine pool failure.

        async def raise_broken_pool(*args, **kwargs):
            raise BrokenProcessPool("a process in the process pool was terminated abruptly")

        with (
            patch.object(asyncio.get_running_loop(), "run_in_executor", raise_broken_pool),
            pytest.raises(BrokenProcessPool),
        ):
            await pool.submit(lambda: 0)


async def test_broken_process_pool_preserves_retry_count(azurite_connstr):
    """Full integration: BrokenProcessPool during preemption must requeue the
    task without decrementing retries."""
    num_retries = 3

    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        await q.clear()
        await q.push({"cmd": "sleep 60"}, num_retries=num_retries)

    shutdown_event = asyncio.Event()
    shutdown_event.set()

    async with ShellCommandProcessor(num_workers=1) as proc:
        proc.pool._shutdown_event = shutdown_event

        # Patch run_in_executor on the pool to raise BrokenProcessPool
        # (simulating the pool child being killed by process-group SIGTERM).
        original_run_in_executor = asyncio.get_running_loop().run_in_executor

        async def selective_broken_pool(executor, func, *args):
            if executor is proc.pool._pool:
                raise BrokenProcessPool("a process in the process pool was terminated abruptly")
            return await original_run_in_executor(executor, func, *args)

        async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
            with patch.object(asyncio.get_running_loop(), "run_in_executor", selective_broken_pool):
                with pytest.raises(WorkerCanceled):
                    await q.pull_and_execute(
                        proc,
                        visibility_timeout=timedelta(seconds=30),
                        with_heartbeat=False,
                    )

    # Check retry count — should be preserved
    await asyncio.sleep(1)
    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        msgs = await q.peek(1)
        assert len(msgs) >= 1, "Task should be back in the queue"
        task = Task.deserialize(msgs[0].content)

        assert task.num_retries == num_retries, (
            f"Retry count should be preserved when BrokenProcessPool occurs "
            f"during preemption. Expected {num_retries}, got {task.num_retries}. "
            f"BUG: BrokenProcessPool during preemption was incorrectly treated "
            f"as a genuine task failure."
        )
