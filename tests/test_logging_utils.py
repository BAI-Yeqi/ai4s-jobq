# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Tests for ``ai4s.jobq.logging_utils`` filter placement.

These tests pin down the invariant that ``LOG.exception(...)`` records
preserve their ``exc_info`` for the rich/plain (i.e. non-AppInsights)
handlers. The historical bug was that ``CustomDimensionsFilter`` —
installed on the ``ai4s.jobq`` logger itself — stripped ``exc_info``
*before* any handler dispatched, so every worker failure logged its
"Failure for task X" line with no traceback (see ``problem.md``).
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from ai4s.jobq.logging_utils import (
    LOG,
    TASK_LOG,
    CustomDimensionsFilter,
    setup_logging,
)


def test_custom_dimensions_filter_preserves_exc_info_when_filter_exceptions_disabled() -> None:
    """The dimension-stamp-only mode must not touch ``exc_info``.

    This is the contract the logger-level filter relies on so the
    rich/plain handlers can still format the traceback.
    """
    f = CustomDimensionsFilter({"queue": "test"}, filter_exceptions=False)

    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="ai4s.jobq",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Failure for task X.",
        args=None,
        exc_info=exc_info,
    )

    keep = f.filter(record)

    assert keep is True, "filter must not drop records"
    assert record.exc_info is exc_info, (
        "filter_exceptions=False must leave exc_info intact for downstream handlers"
    )
    assert getattr(record, "queue", None) == "test", (
        "filter must still stamp custom dimensions on the record"
    )


def test_custom_dimensions_filter_strips_exc_info_when_filter_exceptions_enabled() -> None:
    """The handler-scoped mode still strips ``exc_info`` for AppInsights ingestion.

    This guards the AppExceptions-noise-suppression behaviour that the
    handler-level filter on the Azure Monitor handler relies on.
    """
    f = CustomDimensionsFilter({"queue": "test"}, filter_exceptions=True)

    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="ai4s.jobq",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Failure for task X.",
        args=None,
        exc_info=exc_info,
    )

    keep = f.filter(record)

    assert keep is True
    assert record.exc_info is None, (
        "filter_exceptions=True must zero exc_info so AppInsights does not record an "
        "AppExceptions row for this log entry"
    )
    assert getattr(record, "exception_traceback", None), (
        "the formatted traceback should be stashed on the record so an Azure-Monitor "
        "consumer can still see it as a string field"
    )


@pytest.fixture
def isolated_logging() -> Any:
    """Snapshot and restore logger filters/handlers around a test.

    ``setup_logging`` mutates the root logger, ``ai4s.jobq``, and
    ``task`` global state.  Without isolation, one test leaks filter
    state into the next.
    """
    import ai4s.jobq.logging_utils as logging_utils

    root_logger = logging.getLogger()
    saved_root_handlers = list(root_logger.handlers)
    saved_root_level = root_logger.level
    saved_log_filters = list(LOG.filters)
    saved_log_level = LOG.level
    saved_task_filters = list(TASK_LOG.filters)
    saved_task_level = TASK_LOG.level
    saved_global_filter = logging_utils._custom_dimensions_filter

    yield

    root_logger.handlers = saved_root_handlers
    root_logger.setLevel(saved_root_level)
    LOG.filters = saved_log_filters
    LOG.setLevel(saved_log_level)
    TASK_LOG.filters = saved_task_filters
    TASK_LOG.setLevel(saved_task_level)
    logging_utils._custom_dimensions_filter = saved_global_filter


@pytest.mark.usefixtures("isolated_logging")
def test_setup_logging_does_not_strip_exc_info_at_logger_level() -> None:
    """End-to-end: ``LOG.exception(...)`` after ``setup_logging`` keeps ``exc_info``.

    Regression guard for the worker stuck-task bug described in
    ``problem.md``: pre-fix, ``LOG.exception`` records reached every
    handler with ``exc_info = None`` because the logger-level
    ``CustomDimensionsFilter`` ran first and zeroed it.  This test
    proves the logger-level filter does not destroy that diagnostic
    information.
    """
    captured: list[logging.LogRecord] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    capture_handler = CaptureHandler(level=logging.DEBUG)

    # Avoid the App-Insights branch of setup_logging by stripping the
    # env var; we only care about the logger-level filter wiring here.
    import os

    saved_connstr = os.environ.pop("APPLICATIONINSIGHTS_CONNECTION_STRING", None)
    try:
        setup_logging(queue_spec="test-queue")
        logging.getLogger().addHandler(capture_handler)

        try:
            raise RuntimeError("simulated worker failure")
        except RuntimeError:
            LOG.exception("Failure for task X.")
    finally:
        if saved_connstr is not None:
            os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = saved_connstr

    matching = [r for r in captured if r.name == "ai4s.jobq" and "Failure for task X" in r.msg]
    assert matching, (
        f"expected the captured handler to receive the 'Failure for task X' "
        f"record; got names={[r.name for r in captured]!r}"
    )
    record = matching[0]
    assert record.exc_info is not None, (
        "LOG.exception(...) must preserve exc_info so the rich/plain "
        "handlers can format the traceback — this was the root cause of "
        "the silent-failure diagnostic gap in problem.md"
    )
    exc_type, exc_value, _tb = record.exc_info
    assert exc_type is RuntimeError
    assert str(exc_value) == "simulated worker failure"


@pytest.mark.usefixtures("isolated_logging")
def test_setup_logging_installs_dimension_stamping_only_at_logger() -> None:
    """The logger-level filter must be the stamp-only variant.

    Pinning the wiring: any future refactor that re-installs an
    exc-info-stripping filter on ``LOG`` itself would silently
    re-introduce the original bug, so we assert the contract directly.
    """
    import os

    saved_connstr = os.environ.pop("APPLICATIONINSIGHTS_CONNECTION_STRING", None)
    try:
        setup_logging(queue_spec="test-queue")
    finally:
        if saved_connstr is not None:
            os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = saved_connstr

    log_filters = [f for f in LOG.filters if isinstance(f, CustomDimensionsFilter)]
    assert log_filters, "expected a CustomDimensionsFilter on the ai4s.jobq logger"
    for f in log_filters:
        assert f.filter_exceptions is False, (
            "any CustomDimensionsFilter attached to LOG must have "
            "filter_exceptions=False so it cannot strip exc_info before "
            "the rich/plain handler dispatches"
        )

    task_filters = [f for f in TASK_LOG.filters if isinstance(f, CustomDimensionsFilter)]
    for f in task_filters:
        assert f.filter_exceptions is False, (
            "any CustomDimensionsFilter attached to TASK_LOG must have "
            "filter_exceptions=False for the same reason"
        )
