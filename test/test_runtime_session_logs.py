from __future__ import annotations

import json
from pathlib import Path

import pytest

from iii_drone_contracts import EventSource, OperatorEvent
from iii_drone_runtime.api.session_logs import RuntimeSessionLogs


def event(message: str = "event") -> OperatorEvent:
    return OperatorEvent(
        event_id=f"event-{message}",
        source=EventSource.RUNTIME,
        category="runtime",
        severity="info",
        message=message,
    )


def test_events_are_written_immediately_without_a_clock_gate(tmp_path: Path) -> None:
    logs = RuntimeSessionLogs(tmp_path / "logs")
    logs.append(event("first"))

    records = [
        json.loads(line)
        for line in (logs.session_root / "runtime-api.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert records[0]["event"]["message"] == "first"
    logs.close()


def test_debug_logging_is_explicit_and_stays_writable(tmp_path: Path) -> None:
    logs = RuntimeSessionLogs(tmp_path / "logs", debug_enabled=True)
    logs.append_debug(source="api", message="trace", details={"step": 1})
    value = json.loads(
        (logs.session_root / "debug-api.jsonl").read_text(encoding="utf-8")
    )
    assert value["details"] == {"step": 1}


def test_debug_logging_can_be_disabled(tmp_path: Path) -> None:
    logs = RuntimeSessionLogs(tmp_path / "logs")
    with pytest.raises(RuntimeError, match="not enabled"):
        logs.append_debug(source="api", message="trace")
