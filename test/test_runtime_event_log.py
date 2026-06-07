import logging
from io import StringIO

from iii_drone_contracts import CommandResultMessage, EventSource
from iii_drone_runtime.api.events import RuntimeEventLog


def test_runtime_event_log_is_bounded_and_contract_typed():
    log = RuntimeEventLog(max_events=2)

    log.record_availability_change(label="action-server", available=False, reason="missing")
    log.record_runtime_validation_failure(message="bad request", request_id="req-1")
    log.record_command_result(
        CommandResultMessage(request_id="req-2", command_id="px4.hold", status="succeeded")
    )

    events = log.recent()
    assert len(events) == 2
    assert events[0].category == "validation_failure"
    assert events[1].category == "command_result"


def test_mutating_command_decisions_are_written_to_service_logs():
    log = RuntimeEventLog()
    import iii_drone_runtime.api.events as events_module

    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)
    events_module.LOGGER.addHandler(handler)
    try:
        log.record_command_decision(
            command_id="runtime.stop",
            request_id="req-3",
            accepted=True,
            reason=None,
            mutating=True,
        )
        log.record_command_decision(
            command_id="runtime.shutdown",
            request_id="req-4",
            accepted=False,
            reason="vehicle armed",
            mutating=True,
        )
    finally:
        events_module.LOGGER.removeHandler(handler)

    output = stream.getvalue()
    assert "command accepted: runtime.stop" in output
    assert "command rejected: runtime.shutdown - vehicle armed" in output


def test_remote_cli_rejection_event_includes_metadata_and_source_label():
    log = RuntimeEventLog()

    event = log.record_cli_rejection(
        command_id="runtime.stop",
        request_id="cli-1",
        client_label="remote-cli",
        reason="blocked by active GUI",
    )

    assert event.source == EventSource.CLI
    assert event.category == "remote_cli_conflict"
    assert event.details["client_label"] == "remote-cli"
    assert event.command_id == "runtime.stop"


def test_event_source_labels_distinguish_runtime_and_local_sources():
    log = RuntimeEventLog()

    runtime_event = log.record_availability_change(label="daemon", available=True)
    cli_event = log.record_cli_rejection(
        command_id="runtime.stop",
        request_id="cli-2",
        client_label=None,
        reason="blocked",
    )

    assert runtime_event.source == EventSource.RUNTIME
    assert cli_event.source == EventSource.CLI
