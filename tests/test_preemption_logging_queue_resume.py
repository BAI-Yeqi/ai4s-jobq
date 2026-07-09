# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Test that the multiprocessing logging queue recovers after preemption-resume.

Scenario:
1. Worker is processing tasks via ShellCommandProcessor (which uses ProcessPool).
2. A preemption is announced (shutdown_event set) → processor.shutdown() kills
   pool children and recreates the pool.
3. The preemption doesn't actually happen → processor.resume() is called.
4. New tasks are submitted — verify that their stdout/stderr still arrives
   through the log_msg_queue to the parent process's logger.

The risk: _kill_subprocesses() shuts down the ProcessPoolExecutor and creates a
new one, but the log_msg_queue consumer task (log_from_queue) might have exited
due to a BrokenPipeError/EOFError during the pool teardown.
"""

import asyncio
import contextlib
import logging
import textwrap

from ai4s.jobq.work import ShellCommandProcessor


async def test_logging_queue_works_after_preemption_resume(caplog):
    """After shutdown() + resume(), the log_msg_queue must still deliver
    subprocess output to the parent process's logger."""

    async with ShellCommandProcessor(num_workers=1) as proc:
        # 1. Run a task before preemption — verify baseline works
        with caplog.at_level(logging.INFO, logger="task"):
            await proc(cmd="echo BEFORE_PREEMPTION", _job_id="job-before")

        assert any("BEFORE_PREEMPTION" in r.message for r in caplog.records), (
            "Baseline: log_msg_queue should deliver output before preemption"
        )
        caplog.clear()

        # 2. Simulate preemption: shutdown kills subprocesses, recreates pool
        await proc.shutdown()

        # 3. Preemption didn't happen — resume
        await proc.resume()

        # 4. Run a task after resume — log_msg_queue must still be alive
        with caplog.at_level(logging.INFO, logger="task"):
            await proc(cmd="echo AFTER_RESUME", _job_id="job-after")

        assert any("AFTER_RESUME" in r.message for r in caplog.records), (
            "After preemption-resume cycle, the log_msg_queue consumer task "
            "must still be running and delivering subprocess output. "
            "If this fails, log_from_queue likely exited due to a "
            "BrokenPipeError during pool shutdown."
        )


async def test_logging_queue_works_after_multiple_preemption_cycles(caplog):
    """Multiple preemption-resume cycles should not degrade the logging queue."""

    async with ShellCommandProcessor(num_workers=1) as proc:
        for cycle in range(3):
            # Preempt
            await proc.shutdown()
            await proc.resume()

            # Verify logging still works
            marker = f"CYCLE_{cycle}_OUTPUT"
            with caplog.at_level(logging.INFO, logger="task"):
                await proc(cmd=f"echo {marker}", _job_id=f"job-cycle-{cycle}")

            assert any(marker in r.message for r in caplog.records), (
                f"Logging queue failed after preemption cycle {cycle}"
            )
            caplog.clear()


async def test_logging_queue_during_active_task_preemption(caplog):
    """If a task is running when preemption fires, after resume the queue
    must still work for subsequent tasks."""

    script = textwrap.dedent("""\
        trap 'echo "SIGTERM_HANDLED"; exit 0' TERM
        echo "TASK_STARTED"
        sleep 60 &
        wait
    """).strip()

    async with ShellCommandProcessor(num_workers=1) as proc:
        # Start a long-running task
        task = asyncio.create_task(proc(cmd=script, _job_id="job-interrupted"))

        # Let it start
        await asyncio.sleep(1.0)

        # Preempt while task is running
        await proc.shutdown()

        # Task should complete (via WorkerCanceled or normal exit after SIGTERM)
        with contextlib.suppress(Exception):
            await task

        # Resume
        await proc.resume()

        # Verify logging queue still delivers output
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="task"):
            await proc(cmd="echo POST_INTERRUPT_OK", _job_id="job-post-interrupt")

        assert any("POST_INTERRUPT_OK" in r.message for r in caplog.records), (
            "After interrupting an active task with preemption, then resuming, "
            "the log_msg_queue must still be functional."
        )
