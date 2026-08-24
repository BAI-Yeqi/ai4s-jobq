from ai4s.jobq.workflow._queues import _parse_queue_spec


def test_parse_service_bus_queue_spec_with_explicit_queue_name() -> None:
    account, queue_name = _parse_queue_spec(
        "sb://myproject/myqueue",
        default_account="unused",
    )

    assert account == "sb://myproject"
    assert queue_name == "myqueue"
