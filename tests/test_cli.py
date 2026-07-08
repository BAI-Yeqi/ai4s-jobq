import asyncio
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import types
from datetime import datetime
from uuid import uuid4

import click
import pytest
import yaml
from asyncclick.testing import CliRunner
from azure.storage.queue.aio import QueueClient

from ai4s.jobq import cli
from ai4s.jobq.backend.storage_queue import azurite_conn_str
from ai4s.jobq.orchestration.manager import TooManyFailuresException
from ai4s.jobq.orchestration.pid_file import send_signal_by_glob
from ai4s.jobq.work import Processor

QUEUE = "testq"


@pytest.fixture
async def queue_name():
    current_number = random.choice(range(1_000_000))
    return f"queue{current_number}"


async def cli_cmd(
    *args: str,
    queue_name: str = QUEUE,
    queue_spec: list[str] | None = None,
    options: list[str] | None = None,
    exit_code=0,
) -> str:
    options = options or []
    queue_spec = queue_spec or [f"devstoreaccount1/{queue_name}"]
    if True:
        result = await CliRunner().invoke(
            cli.main, (*options, *queue_spec, *args), catch_exceptions=False
        )
    else:
        cli.main((*options, *queue_spec, *args), standalone_mode=False, catch_exceptions=False)
    assert result.exit_code == exit_code, result.output
    return result.output  # type: ignore[no-any-return]


def _run_worker_and_signal(
    worker_cmd: tuple[str, ...],
    *,
    ready_markers: list[str],
    send_signal_fn,
    env: dict[str, str] | None = None,
    ready_timeout: float = 60.0,
    exit_timeout: float = 60.0,
) -> tuple[str, datetime, datetime]:
    """Start a worker subprocess, wait until its task(s) actually begin running, then signal.

    The signal-handling tests must deliver their signal only *after* the worker has pulled and
    started executing the bash task(s); otherwise the task never installs its SIGTERM trap and the
    ``bash got signal-*`` output never appears. Racing a fixed ``communicate(timeout=2)`` is flaky
    on loaded CI runners where task pickup can take longer than the window. Instead, this polls the
    merged stdout/stderr stream until every string in ``ready_markers`` has been observed (each task
    echoes a start marker as its first action), then invokes ``send_signal_fn(proc)``.

    Returns ``(combined_output, start, end)`` where ``combined_output`` is the de-styled, merged
    stdout+stderr text and ``start``/``end`` bracket the signal-to-exit interval.
    """
    proc = subprocess.Popen(
        worker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        bufsize=1,
        env=env,
    )
    buf: list[str] = []
    lock = threading.Lock()

    def _reader() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            with lock:
                buf.append(line)

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()

    def _seen_all_markers() -> bool:
        with lock:
            text = "".join(buf)
        return all(text.count(marker) >= 1 for marker in ready_markers)

    deadline = time.monotonic() + ready_timeout
    while time.monotonic() < deadline:
        if _seen_all_markers() or proc.poll() is not None:
            break
        time.sleep(0.05)

    start = datetime.now()
    send_signal_fn(proc)
    try:
        proc.wait(timeout=exit_timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    reader.join(timeout=10)
    end = datetime.now()
    with lock:
        combined = click.unstyle("".join(buf))
    return combined, start, end


@pytest.fixture(autouse=False)
async def clean_queue():
    try:
        async with QueueClient.from_connection_string(azurite_conn_str(), QUEUE) as queue:
            async for msg in queue.receive_messages():
                await queue.delete_message(msg)
    except Exception:
        pass


class DummyProcessor(Processor):
    def __init__(self, num_workers=1) -> None:
        super().__init__()

    async def __call__(self, cmd, **kwargs) -> None:
        if "raise" in cmd:
            raise Exception(cmd)
        print(cmd)


@pytest.fixture
def dummy_processor(mocker) -> None:
    mocker.patch("ai4s.jobq.cli.ShellCommandProcessor", DummyProcessor)


@pytest.mark.asyncio
async def test_help() -> None:
    await CliRunner().invoke(cli.main, ["--help"])


@pytest.mark.asyncio
async def test_push_pull(dummy_processor) -> None:
    """
    push/pull should affect the size of the queue.
    """
    uuid = str(uuid4())

    s0 = int((await cli_cmd("size")).strip())

    output = await cli_cmd("push", "-c", f"echo {uuid}")
    assert output == ""

    s1 = int((await cli_cmd("size")).strip())

    output = await cli_cmd("pull")
    assert f"echo {uuid}" in output

    s2 = int((await cli_cmd("size")).strip())

    # sizes are approximate.
    assert s0 < s1 >= s2


@pytest.mark.asyncio
async def test_worker(dummy_processor) -> None:
    """
    workers should handle all tasks in the queue until it's empty.
    """
    output = await cli_cmd("push", "-c", "echo hello world1")
    assert output == ""
    output = await cli_cmd("push", "-c", "echo hello world2")
    assert output == ""

    output = await cli_cmd("worker")
    assert "echo hello world1" in output
    assert "echo hello world2" in output


@pytest.mark.asyncio
async def test_retry(caplog) -> None:
    """
    failing jobs should be retried the specified number of times.
    """
    output = await cli_cmd("push", "-c", "exit 1", "--num-retries", "2")
    assert output == ""

    proc_spec = ["-vv"]
    with pytest.raises(TooManyFailuresException):
        await cli_cmd("worker", "--visibility-timeout", "2s", options=proc_spec)
    assert str(caplog.messages).count("Re-queue") == 2

    await asyncio.sleep(1)
    caplog.clear()
    await cli_cmd("worker", options=proc_spec)
    assert str(caplog.messages).count("Re-queue") == 0


@pytest.mark.asyncio
async def test_envvars(caplog) -> None:
    """
    environment variables passed on the command line should be available in the worker
    """
    await cli_cmd("push", "-c", "echo XYZ-$VALUE", "-e", "VALUE=42")

    caplog.clear()
    await cli_cmd("pull")
    output = str(caplog.messages)
    assert "XYZ-42" in output


@pytest.mark.asyncio
async def test_dirsync(tmp_path) -> None:
    """
    files written to $AMLT_DIRSYNC_DIR should sync to the specified directory.
    """
    await cli_cmd(
        "push",
        "-c",
        "echo hello > $AMLT_DIRSYNC_DIR/foo.txt",
        "--bg-dirsync-to",
        str(tmp_path),
    )
    await cli_cmd("pull")
    assert tmp_path.joinpath("foo.txt").read_text() == "hello\n"


@pytest.mark.asyncio
async def test_parallel() -> None:
    """
    multiple workers can run in parallel
    """
    import time

    await cli_cmd("push", "-c", "sleep 3")
    await cli_cmd("push", "-c", "sleep 3")
    await cli_cmd("push", "-c", "sleep 3")
    start = time.time()
    await cli_cmd("worker", "-n", "3")
    elapsed = time.time() - start
    assert elapsed < 6


@pytest.mark.asyncio
async def test_amlt(mocker, tmp_path) -> None:
    """
    Check that we can launch worker via amlt
    """
    config_yml = tmp_path / "config.yml"
    config_yml.write_text(
        """
environment:
  image: foo
    """
    )

    config_dct = None
    env = None

    class DummyPopen:
        def __init__(self, cmd, **kwargs):
            nonlocal config_dct, env
            augmented_config = cmd[2]
            env = kwargs.get("env")
            with open(augmented_config) as f:
                config_dct = yaml.safe_load(f)

        def wait(self):
            return 0

    mocker.patch("subprocess.Popen", DummyPopen)
    await cli_cmd("amlt", "--time-limit", "1h", "run", str(config_yml))
    assert config_dct
    assert env
    assert (
        config_dct["environment"]["env"]["JOBQ_STORAGE"]
        == "devstoreaccount1"
        == env["JOBQ_STORAGE"]
    )
    assert config_dct["environment"]["env"]["JOBQ_QUEUE"] == "testq" == env["JOBQ_QUEUE"]
    assert config_dct["environment"]["env"].get("JOBQ_CREDENTIAL") is None
    assert config_dct["environment"]["env"]["JOBQ_TIME_LIMIT"] == "3600s" == env["JOBQ_TIME_LIMIT"]


@pytest.mark.asyncio
async def test_time_limit(mocker, tmp_path, caplog, queue_name):
    """
    Check that when the time-limit of the worker is reached, a shutdown is initiated.
    """
    await cli_cmd(
        "push",
        "-c",
        'set -o monitor; echo "working in shell $!" ; sleep 10 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; echo "Trap set up." ; wait $!',
        queue_name=queue_name,
    )

    t0 = datetime.now()
    await cli_cmd(
        "worker",
        "--time-limit",
        "3s",
        "--visibility-timeout",
        "30s",
        options=["-vv"],
        queue_name=queue_name,
    )
    t1 = datetime.now()
    assert str(caplog.messages).count("Time limit reached. Triggering shutdown handler.") == 1
    assert str(caplog.messages).count("bash got signal-A") == 1

    # should be done in less than the task's sleep time
    assert (t1 - t0).total_seconds() < 9


@pytest.mark.asyncio
@pytest.mark.live
async def test_signal_handling_servicebus(mocker, tmp_path, sb_namespace, sb_queue) -> None:
    """
    Check that we can cleanly abort workers when the top level process receives a SIGTERM.
    """

    queue_spec = f"sb://{sb_namespace}/{sb_queue}"

    await cli_cmd("clear", queue_spec=[queue_spec])

    # insert useless quotes in the print string so that we can check whether it's actually
    # executed, as opposed the command being printed.
    await cli_cmd(
        "push",
        "-c",
        'sleep 20 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; wait $!',
        queue_spec=[queue_spec],
    )
    await cli_cmd(
        "push",
        "-c",
        'sleep 20 & trap "echo ba""sh got signal-B; kill -15 $!" SIGTERM; wait $!',
        queue_spec=[queue_spec],
    )
    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        queue_spec,
        "worker",
        "-n",
        "2",
        "--visibility-timeout=30s",
    )
    with subprocess.Popen(
        worker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ) as proc:
        with pytest.raises(subprocess.TimeoutExpired):
            proc.communicate(timeout=10)
        start = datetime.now()
        proc.send_signal(signal.SIGINT)
        stdout, stderr = proc.communicate()
        stdout, stderr = click.unstyle(stdout), click.unstyle(stderr)
        end = datetime.now()

    assert "bash got signal-A" in (stderr + stdout)
    assert "bash got signal-B" in (stderr + stdout)
    assert (stderr + stdout).count("Requeueing") == 2
    # should be done in less than the task's sleep time
    assert (end - start).total_seconds() < 4


@pytest.mark.asyncio
async def test_signal_handling(mocker, tmp_path, queue_name) -> None:
    """
    Check that we can cleanly abort workers when the top level process receives a SIGTERM.
    """

    q = {"queue_name": queue_name}

    # insert useless quotes in the print string so that we can check whether it's actually
    # executed, as opposed the command being printed.
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-A"; sleep 10 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; wait $!',
        **q,
    )
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-B"; sleep 10 & trap "echo ba""sh got signal-B; kill -15 $!" SIGTERM; wait $!',
        **q,
    )
    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        "devstoreaccount1/" + queue_name,
        "worker",
        "-n",
        "2",
        "--visibility-timeout=30s",
    )
    combined, start, end = _run_worker_and_signal(
        worker_cmd,
        ready_markers=["started-A", "started-B"],
        send_signal_fn=lambda p: p.send_signal(signal.SIGINT),
    )

    assert "bash got signal-A" in combined
    assert "bash got signal-B" in combined
    assert combined.count("Requeueing") == 2
    # graceful cancel must be fast: it must not wait for the task's full sleep time (10s)
    assert (end - start).total_seconds() < 8

    await asyncio.sleep(2)  # give azurite some time

    n = 0
    async with QueueClient.from_connection_string(azurite_conn_str(), queue_name) as queue:
        async for _msg in queue.receive_messages(timeout=1):
            n += 1
    assert n == 2


@pytest.mark.asyncio
async def test_signal_handling_resume(mocker, tmp_path, queue_name) -> None:
    """
    Check that workers receives SIGTERM but then pick up next task when not preempted.
    """

    q = {"queue_name": queue_name}

    mocker.patch.dict(
        os.environ,
        {"JOBQ_PREEMPTION_TIMEOUT": "2"},
    )

    # insert useless quotes in the print string so that we can check whether it's actually
    # executed, as opposed the command being printed.
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-A"; sleep 4 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; wait $!; exit 0',
        **q,
    )
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-B"; sleep 3 & trap "echo ba""sh got signal-B; kill -15 $!" SIGTERM; wait $!; exit 0',
        **q,
    )
    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        "devstoreaccount1/" + queue_name,
        "worker",
        "--visibility-timeout=30s",
    )
    # Single worker: only the first task starts running before we signal.
    combined, start, end = _run_worker_and_signal(
        worker_cmd,
        ready_markers=["started-A"],
        send_signal_fn=lambda p: p.send_signal(signal.SIGTERM),
    )

    assert "No preemption occurred within 2 seconds." in re.sub(r"\s+", " ", combined), combined
    assert combined.count("bash got signal-A") == 1
    assert combined.count("bash got signal-B") == 0
    assert combined.count("Requeueing") == 1
    # Not preempted: the worker requeues and then resumes both tasks to completion. Bound guards
    # against hanging (for example waiting out the 30s visibility timeout), not against a tight
    # sub-second budget — the two sequential sleeps plus the 2s preemption wait take ~9s.
    assert (end - start).total_seconds() < 20

    await asyncio.sleep(2)  # give azurite some time

    n = 0
    async with QueueClient.from_connection_string(azurite_conn_str(), queue_name) as queue:
        async for _msg in queue.receive_messages(timeout=1):
            n += 1
    assert n == 0


# broken in devops CI for unknown reasons?
#     props = await clean_queue.get_queue_properties()
#     # reappears immediately despite longer visibility timeout
#     assert props["approximate_message_count"] == 1


@pytest.mark.asyncio
async def test_cli_proc_spec(mocker, tmp_path) -> None:
    """
    Check that we can launch worker using an arbitrary Processor class via ai4s jobq cli
    """

    tmp_path.joinpath("procpkg").mkdir()
    tmp_path.joinpath("procpkg").joinpath("dummy_proc.py").write_text(
        """
from contextlib import AbstractAsyncContextManager
class DummyProcessor(AbstractAsyncContextManager):
  async def __call__(self, cmd, **kwargs):
    print("Called with cmd:", cmd)

  async def __aenter__(self):
    return self

  async def __aexit__(self, exc_type, exc_value, traceback):
    pass
"""
    )
    new_sys_path = [str(tmp_path), *list(sys.path)]
    mocker.patch.object(sys, "path", new_sys_path)
    push_res = await cli_cmd("push", "-c", "custom hello world", options=["-vv"])
    assert not push_res
    await asyncio.sleep(1)
    pull_res = await cli_cmd("pull", "--proc", "procpkg.dummy_proc.DummyProcessor", options=["-vv"])
    assert "Called with cmd: custom hello world" in pull_res


@pytest.mark.asyncio
async def test_signal1_graceful_shutdown_from_servicebus_handled_correctly(
    mocker, tmp_path, queue_name
) -> None:
    """
    Check that workers receives SIGUSR1, which means do-not-accept-new-tasks. This is usually sent
    by the workforce-monitor. This test tests the orchestration/manager.py behaves correctly.
    """

    mocker.patch.dict(
        os.environ,
        {"JOBQ_PREEMPTION_TIMEOUT": "2"},
    )

    # insert useless quotes in the print string so that we can check whether it's actually
    # executed, as opposed the command being printed.
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-A"; sleep 10 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; wait $!; echo "tas""k a finished"; exit 0',
        queue_name=queue_name,
    )
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-B"; sleep 10 & trap "echo ba""sh got signal-B; kill -15 $!" SIGTERM; wait $!; echo "tas""k b finished"; exit 0',
        queue_name=queue_name,
    )
    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        "devstoreaccount1/" + queue_name,
        "worker",
        "--visibility-timeout=30s",
    )

    # Single worker: wait for task a to actually start (so its pid file exists and its SIGTERM
    # trap is installed) before signalling, then send SIGUSR1 (do-not-accept-new-tasks).
    combined, start, end = _run_worker_and_signal(
        worker_cmd,
        ready_markers=["started-A"],
        send_signal_fn=lambda p: send_signal_by_glob(
            pattern=f"{tmp_path}/*.pid",
            signal_to_send=signal.SIGUSR1,
        ),
        env={**os.environ, "JOBQ_PID_DIR": str(tmp_path), "PYTHONUNBUFFERED": "1"},
    )

    # we expect task a to finish, and task b should not be started
    # Rich may wrap long lines; collapse whitespace for reliable matching
    combined_flat = re.sub(r"\s+", " ", combined)
    assert combined.count("bash got signal-A") == 0  # task should not be cancelled
    assert combined.count("bash got signal-B") == 0  # task should not be cancelled
    assert combined.count("Requeueing") == 0
    assert (
        "Soft shutdown requested. Will not accept additional tasks and sleep until this process is terminated."
        in combined_flat
    )
    assert "Stopping as we are not accepting new tasks." in combined_flat
    assert combined.count("task a finished") == 1
    assert combined.count("task b finished") == 0
    assert "Stopping workforce monitor" in combined
    # Soft shutdown finishes the current task (sleep 10) before exiting, so the bound only guards
    # against hanging beyond that — not against a tight budget.
    assert (end - start).total_seconds() < 20

    await asyncio.sleep(2)  # give azurite some time

    n = 0
    # task-b should still be queued, we verify this here
    async with QueueClient.from_connection_string(azurite_conn_str(), queue_name) as queue:
        async for _ in queue.receive_messages(timeout=1):
            n += 1
    assert n == 1


@pytest.mark.asyncio
async def test_signal2_graceful_shutdown_from_servicebus_handled_correctly(
    mocker, tmp_path, queue_name
) -> None:
    """
    Check that workers receives SIGUSR2, which means graceful-shutdown. This is usually sent
    by the workforce-monitor. This test tests the orchestration/manager.py behaves correctly.
    """

    mocker.patch.dict(
        os.environ,
        {"JOBQ_PREEMPTION_TIMEOUT": "2"},
    )

    # insert useless quotes in the print string so that we can check whether it's actually
    # executed, as opposed the command being printed.
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-A"; sleep 10 & trap "echo ba""sh got signal-A; kill -15 $!" SIGTERM; wait $!; echo "tas""k a finished"; exit 0',
        queue_name=queue_name,
    )
    await cli_cmd(
        "push",
        "-c",
        'echo "sta""rted-B"; sleep 10 & trap "echo ba""sh got signal-B; kill -15 $!" SIGTERM; wait $!; echo "tas""k b finished"; exit 0',
        queue_name=queue_name,
    )
    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        "devstoreaccount1/" + queue_name,
        "worker",
        "--visibility-timeout=30s",
    )

    # Single worker: wait for task a to actually start (so its pid file exists and its SIGTERM
    # trap is installed) before signalling, then send SIGUSR2 (graceful shutdown).
    combined, start, end = _run_worker_and_signal(
        worker_cmd,
        ready_markers=["started-A"],
        send_signal_fn=lambda p: send_signal_by_glob(
            pattern=f"{tmp_path}/*.pid",
            signal_to_send=signal.SIGUSR2,
        ),
        env={**os.environ, "JOBQ_PID_DIR": str(tmp_path), "PYTHONUNBUFFERED": "1"},
    )

    # we expect task a to be interrupted and requeued, and task b should not be started
    combined_flat = re.sub(r"\s+", " ", combined)
    assert combined.count("bash got signal-A") == 1  # task should be cancelled
    assert combined.count("bash got signal-B") == 0  # task should not be started
    assert combined.count("Requeueing") == 1
    assert (
        "Soft shutdown requested. Will not accept additional tasks, cancel current one(s)"
        in combined_flat
    )
    assert "Sending termination signal to pool process" in combined_flat
    assert "Stopping as we are not accepting new tasks." in combined_flat
    # as we give the task some time to write a checkpoint, the task actually has time to finish
    assert combined.count("task a finished") == 1
    assert combined.count("task b finished") == 0
    assert "Stopping workforce monitor" in combined
    # Graceful shutdown cancels the current task after the preemption timeout; bound guards against
    # hanging, not against a tight budget.
    assert (end - start).total_seconds() < 15

    await asyncio.sleep(2)  # give azurite some time

    n = 0
    # task-b should still be queued, we verify this here
    async with QueueClient.from_connection_string(azurite_conn_str(), queue_name) as queue:
        async for _ in queue.receive_messages(timeout=1):
            n += 1
    assert n == 2


@pytest.mark.asyncio
async def test_empty_queue_finishes_successfully(mocker, queue_name) -> None:
    """
    Check that workers exits successfully if the queue is empty. This also implicitly tests that
    the tasks like the worforce-monitor exit.
    """

    mocker.patch.dict(
        os.environ,
        {"JOBQ_PREEMPTION_TIMEOUT": "2"},
    )

    worker_cmd = (
        shutil.which("ai4s-jobq") or "ai4s-jobq",
        "-vv",
        "devstoreaccount1/" + queue_name,
        "worker",
        "--visibility-timeout=30s",
    )

    with subprocess.Popen(
        worker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ) as proc:
        start = datetime.now()
        stdout, stderr = proc.communicate()
        stdout, stderr = click.unstyle(stdout), click.unstyle(stderr)
        end = datetime.now()

    # make sure the worker started and exited cleanly
    assert proc.returncode == 0, f"Worker exited with code {proc.returncode}\nstderr: {stderr}"
    # Heartbeat may or may not fire before the worker exits on an empty queue (race with create_task)
    assert (stderr + stdout).count("Worker is still running. Approximate queue size=0.") <= 1
    assert (stderr + stdout).count("Pool exited.") == 1
    assert (stderr + stdout).count("Stopping workforce monitor") == 1
    # should be done in less than the task's sleep time
    assert (end - start).total_seconds() < 4

    await asyncio.sleep(2)  # give azurite some time

    n = 0
    # queue should be empty
    async with QueueClient.from_connection_string(azurite_conn_str(), queue_name) as queue:
        async for _ in queue.receive_messages(timeout=1):
            n += 1
    assert n == 0


@pytest.mark.asyncio
async def test_track_accepts_local_workflow_file_without_backend(monkeypatch, tmp_path) -> None:
    workflow = tmp_path / "wf.json"
    workflow.write_text(
        """
{
  "name": "local-preview",
  "default_queue": "q",
  "tasks": [
    {"name": "a", "kwargs": {"cmd": "echo a"}},
    {"name": "b", "depends_on": ["a"], "kwargs": {"cmd": "echo b"}}
  ]
}
""".strip()
    )

    called: dict[str, object] = {}

    def _fake_run_with_default_queue(queue_name=None, debug=False, port=8050, open_browser=True):
        called["queue_name"] = queue_name
        called["debug"] = debug
        called["port"] = port
        called["open_browser"] = open_browser

    monkeypatch.setattr(cli, "_jobq_track_requirements_are_available", lambda **_: True)
    monkeypatch.setitem(
        sys.modules,
        "ai4s.jobq.track.app",
        types.SimpleNamespace(run_with_default_queue=_fake_run_with_default_queue),
    )
    monkeypatch.delenv("JOBQ_LA_WORKSPACE_ID", raising=False)
    monkeypatch.delenv("JOBQ_WORKFLOW_PREFIX", raising=False)

    result = await CliRunner().invoke(
        cli.main,
        ["track", "--workflow-file", str(workflow), "-p", "8765"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert called == {
        "queue_name": None,
        "debug": False,
        "port": 8765,
        "open_browser": True,
    }
    assert os.environ.get("JOBQ_WORKFLOW_FILE") == str(workflow)
    assert os.environ.get("JOBQ_LA_WORKSPACE_ID") is None


@pytest.mark.asyncio
async def test_track_requires_data_source(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_jobq_track_requirements_are_available", lambda **_: True)
    monkeypatch.delenv("JOBQ_LA_WORKSPACE", raising=False)
    monkeypatch.delenv("JOBQ_WORKFLOW_PREFIX", raising=False)
    monkeypatch.delenv("JOBQ_WORKFLOW_FILE", raising=False)

    result = await CliRunner().invoke(cli.main, ["track"], catch_exceptions=False)

    assert result.exit_code == 2
    assert "Specify at least one data source" in result.output
