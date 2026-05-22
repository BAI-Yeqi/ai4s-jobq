# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Test: subprocess is terminated after lock is lost.

After PR #59, ``ProcessPool.submit()`` no longer calls ``_kill_subprocesses``
on individual ``CancelledError``.  This fixed the blast-radius bug (one
lock-loss killing ALL pool tasks), but required a targeted kill mechanism:

When a lock is lost:
1. ``pull_and_execute`` detects lock loss via ``lock_lost_event``.
2. It cancels ``callback_task``.
3. ``ProcessPool.submit()`` receives ``CancelledError``.
4. The fix: ``submit()`` reads the child PID from a temp file and sends
   SIGUSR1 to that specific pool child, which forwards SIGTERM to its
   shell subprocess.
5. The subprocess terminates — no duplicate execution.
"""

import asyncio
import contextlib
import os
import tempfile
import textwrap
import time
from pathlib import Path

import pytest

from ai4s.jobq.work import ShellCommandProcessor


def _ts():
    return time.strftime("%H:%M:%S")


async def test_subprocess_keeps_running_after_cancellation():
    """After CancelledError, the pool child subprocess is terminated.

    Verifies the targeted kill: when a task is cancelled (simulating lock loss),
    SIGUSR1 is sent to the specific pool child, which forwards SIGTERM to its
    shell subprocess.  The subprocess stops — no duplicate execution.
    """
    marker_dir = tempfile.mkdtemp(prefix="jobq_lock_lost_repro_")
    heartbeat_file = os.path.join(marker_dir, "heartbeat")

    # The task writes a heartbeat file every second.  After cancellation we
    # check whether the subprocess is still writing — proving it's been killed.
    # Note: SIGKILL cannot be trapped, so no trap handler needed.
    script = textwrap.dedent(f"""\
        for i in $(seq 1 60); do
            echo $i > {heartbeat_file}
            sleep 1
        done
        echo COMPLETED > {heartbeat_file}
    """).strip()

    async with ShellCommandProcessor(num_workers=1) as proc:
        task = asyncio.create_task(
            proc(cmd=script, _job_id="lock-lost-task"),
            name="lock-lost-task",
        )

        # Wait for the subprocess to start (heartbeat file appears).
        for _ in range(20):
            await asyncio.sleep(0.5)
            if Path(heartbeat_file).exists():
                break
        else:
            task.cancel()
            pytest.fail("Subprocess did not start within 10s")

        initial_heartbeat = Path(heartbeat_file).read_text().strip()
        print(f"[{_ts()}] Subprocess started, heartbeat={initial_heartbeat}", flush=True)

        # ── Simulate lock loss: cancel the task ─────────────────────────
        # In production, this is what happens when pull_and_execute detects
        # lock_lost_event and calls callback_task.cancel().
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, RuntimeError):
            await task

        print(f"[{_ts()}] Task cancelled (simulating lock loss)", flush=True)

        # ── Check if the subprocess has been killed ─────────────────────
        # SIGKILL is immediate — give a short moment for the OS to reap, then verify.
        await asyncio.sleep(1)
        heartbeat_after_cancel = Path(heartbeat_file).read_text().strip()
        await asyncio.sleep(3)
        heartbeat_later = Path(heartbeat_file).read_text().strip()

        print(
            f"[{_ts()}] Heartbeat at cancel+1s: {heartbeat_after_cancel}, 3s later: {heartbeat_later}",
            flush=True,
        )

        subprocess_still_running = heartbeat_later != heartbeat_after_cancel

        print(f"[{_ts()}] Subprocess still running: {subprocess_still_running}", flush=True)

        # After the fix, the subprocess should have been killed via SIGUSR2→SIGKILL pgid.
        assert not subprocess_still_running, (
            f"Subprocess is STILL running after cancellation (heartbeat advanced "
            f"from {heartbeat_after_cancel} to {heartbeat_later}). "
            f"The SIGKILL did not terminate the subprocess process group."
        )


async def test_lock_lost_during_pull_and_execute(azurite_connstr):
    """End-to-end: simulate lock loss and show task subprocess keeps running.

    Uses the actual pull_and_execute path with a real queue (Azurite) and
    demonstrates that after lock_lost_event is set, the subprocess continues
    running — it will write to disk even though the message lock is gone.
    """
    from datetime import timedelta
    from unittest.mock import patch

    from ai4s.jobq import JobQ

    marker_dir = tempfile.mkdtemp(prefix="jobq_lock_lost_e2e_")
    heartbeat_file = os.path.join(marker_dir, "heartbeat")

    script = textwrap.dedent(f"""\
        for i in $(seq 1 30); do
            echo $i > {heartbeat_file}
            sleep 1
        done
        echo COMPLETED > {heartbeat_file}
    """).strip()

    async with JobQ.from_connection_string(
        "jobs-lock-lost", connection_string=azurite_connstr
    ) as q:
        await q.clear()
        await q.push({"cmd": script})

        # We'll intercept the envelope to trigger lock_lost_event manually.
        lock_lost_event: asyncio.Event | None = None

        class EnvelopeInterceptor:
            """Wraps receive_message to capture the lock_lost_event."""

            def __init__(self, original_ctx):
                self._original_ctx = original_ctx
                self._envelope = None

            async def __aenter__(self):
                nonlocal lock_lost_event
                self._envelope = await self._original_ctx.__aenter__()
                lock_lost_event = self._envelope.lock_lost_event
                return self._envelope

            async def __aexit__(self, *args):
                return await self._original_ctx.__aexit__(*args)

        async with ShellCommandProcessor(num_workers=1) as proc:

            async def run_task(cmd, _job_id=None):
                return await proc(cmd=cmd, _job_id=_job_id)

            # Patch receive_message to intercept the envelope.
            original_receive_message = q._client.receive_message

            def patched_receive(*args, **kwargs):
                return EnvelopeInterceptor(original_receive_message(*args, **kwargs))

            with patch.object(q._client, "receive_message", side_effect=patched_receive):
                worker_task = asyncio.create_task(
                    q.pull_and_execute(
                        run_task,
                        visibility_timeout=timedelta(seconds=30),
                        with_heartbeat=True,
                    )
                )

                # Wait for task to start running.
                for _ in range(20):
                    await asyncio.sleep(0.5)
                    if Path(heartbeat_file).exists():
                        break
                else:
                    worker_task.cancel()
                    pytest.fail("Task subprocess did not start")

                initial_hb = Path(heartbeat_file).read_text().strip()
                print(f"[{_ts()}] Task running, heartbeat={initial_hb}", flush=True)

                # ── Simulate lock loss ──────────────────────────────────
                assert lock_lost_event is not None
                lock_lost_event.set()
                print(f"[{_ts()}] lock_lost_event set", flush=True)

                # Wait for pull_and_execute to handle the lock loss.
                with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    await asyncio.wait_for(worker_task, timeout=5)

                print(f"[{_ts()}] pull_and_execute finished", flush=True)

                # ── Verify subprocess has been killed ─────────────────────
                # SIGKILL is immediate — short wait for OS to reap.
                await asyncio.sleep(1)
                hb_at_loss = Path(heartbeat_file).read_text().strip()
                await asyncio.sleep(3)
                hb_after = Path(heartbeat_file).read_text().strip()

                print(
                    f"[{_ts()}] Heartbeat at loss+1s: {hb_at_loss}, 3s later: {hb_after}",
                    flush=True,
                )

                subprocess_still_running = hb_after != hb_at_loss

                # After the fix, subprocess should be killed on lock loss.
                assert not subprocess_still_running, (
                    f"Subprocess is STILL running after lock loss (heartbeat advanced "
                    f"from {hb_at_loss} to {hb_after}). "
                    f"The SIGKILL did not terminate the subprocess process group."
                )
