from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

from iii_drone_contracts import EventSource, OperatorEvent
from iii_drone_runtime.api.events import RuntimeEventLog
from iii_drone_runtime.api.session_logs import RuntimeSessionLogs


def event(message: str = "event") -> OperatorEvent:
    return OperatorEvent(
        event_id=f"event-{message}",
        source=EventSource.RUNTIME,
        category="runtime",
        severity="info",
        message=message,
    )


def clock_state(path: Path, boot_id: str) -> None:
    value = {
        "schema": "iii.receiver-clock-state/v1",
        "state_id": "0" * 64,
        "boot_id": boot_id,
        "gate": "OPERATIONAL",
        "synchronized_monotonic_ns": 100,
        "synchronized_utc_ns": 1_000,
        "uncertainty_ns": 25,
    }
    canonical = json.dumps(
        {key: item for key, item in value.items() if key != "state_id"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    value["state_id"] = hashlib.sha256(canonical).hexdigest()
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def test_preclock_events_flush_once_with_uncertainty(tmp_path: Path) -> None:
    boot = tmp_path / "boot-id"
    boot.write_text("boot-one\n", encoding="ascii")
    clock = tmp_path / "clock.json"
    logs = RuntimeSessionLogs(
        tmp_path / "logs",
        boot_id_path=boot,
        clock_state_path=clock,
    )
    logs.append(event("before-clock"))
    session = logs.store.session_root(logs.session_id)
    assert not (session / "logs/preclock.jsonl").exists()

    clock_state(clock, "boot-one")
    logs.append(event("after-clock"))
    logs.append(event("second-after-clock"))

    preclock = [
        json.loads(line)
        for line in (session / "logs/preclock.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    persisted = [
        json.loads(line)
        for line in (session / "logs/runtime-api.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert len(preclock) == 2
    assert preclock[0]["details"]["message"] == "before-clock"
    assert preclock[0]["utc_uncertainty_ns"] == 25
    assert preclock[-1]["kind"] == "preclock-flush"
    assert [row["event"]["message"] for row in persisted] == [
        "after-clock",
        "second-after-clock",
    ]
    assert all(row["utc_uncertainty_ns"] == 25 for row in persisted)
    logs.close()


def test_invalid_clock_stays_memory_bounded_and_has_no_false_utc(
    tmp_path: Path,
) -> None:
    boot = tmp_path / "boot-id"
    boot.write_text("boot-one\n", encoding="ascii")
    clock = tmp_path / "clock.json"
    clock.write_text(
        '{"gate":"OPERATIONAL","boot_id":"another-boot"}', encoding="utf-8"
    )
    logs = RuntimeSessionLogs(
        tmp_path / "logs",
        boot_id_path=boot,
        clock_state_path=clock,
    )
    logs.ring.policy = replace(logs.policy, degraded_max_records=3)

    for number in range(logs.ring.policy.degraded_max_records + 3):
        logs.append(event(str(number)))

    assert logs.ring.dropped_records == 3
    session = logs.store.session_root(logs.session_id)
    assert not (session / "logs").exists()
    clock_state(clock, "boot-one")
    logs.close()
    rows = [
        json.loads(line)
        for line in (session / "logs/preclock.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert rows[-1]["kind"] == "preclock-flush"
    assert rows[-1]["records_flushed"] == logs.ring.policy.degraded_max_records
    assert rows[-1]["dropped_records"] == 3
    metadata = json.loads((session / "session.json").read_text(encoding="utf-8"))
    assert metadata["completed_utc"] is not None


def test_tampered_canonical_clock_state_cannot_create_utc_precision(
    tmp_path: Path,
) -> None:
    boot = tmp_path / "boot-id"
    boot.write_text("boot-one\n", encoding="ascii")
    clock = tmp_path / "clock.json"
    clock_state(clock, "boot-one")
    value = json.loads(clock.read_text(encoding="utf-8"))
    value["synchronized_utc_ns"] += 1
    clock.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    logs = RuntimeSessionLogs(
        tmp_path / "logs",
        boot_id_path=boot,
        clock_state_path=clock,
    )

    logs.append(event("untrusted-clock"))

    session = logs.store.session_root(logs.session_id)
    assert logs.ring._rows[0][0]["message"] == "untrusted-clock"
    assert not (session / "logs").exists()
    logs.close()


def test_process_restart_recovers_previous_current_session(tmp_path: Path) -> None:
    boot = tmp_path / "boot-id"
    boot.write_text("boot-one\n", encoding="ascii")
    first = RuntimeSessionLogs(
        tmp_path / "logs",
        boot_id_path=boot,
        clock_state_path=tmp_path / "missing-clock",
    )
    second = RuntimeSessionLogs(
        tmp_path / "logs",
        boot_id_path=boot,
        clock_state_path=tmp_path / "missing-clock",
    )

    previous = json.loads(
        (first.store.session_root(first.session_id) / "session.json").read_text()
    )
    current = json.loads(
        (second.store.session_root(second.session_id) / "session.json").read_text()
    )
    assert previous["state"] == "completed"
    assert previous["completion_reason"] == "process-restart"
    assert current["state"] == "current"
    assert current["sequence"] == previous["sequence"] + 1
    second.close()


def test_idle_availability_repetition_is_not_persisted() -> None:
    persisted = []
    log = RuntimeEventLog(sink=persisted.append)

    first = log.record_availability_change(label="runtime", available=True)
    duplicate = log.record_availability_change(label="runtime", available=True)
    changed = log.record_availability_change(
        label="runtime", available=False, reason="stopped"
    )

    assert duplicate is first
    assert changed is not first
    assert len(log.recent()) == len(persisted) == 2


def test_debug_logging_is_explicit_and_session_scoped(tmp_path: Path) -> None:
    boot = tmp_path / "boot-id"
    boot.write_text("boot-one\n", encoding="ascii")
    disabled = RuntimeSessionLogs(
        tmp_path / "disabled",
        boot_id_path=boot,
        clock_state_path=tmp_path / "missing-clock",
    )
    import pytest

    with pytest.raises(RuntimeError, match="not enabled"):
        disabled.append_debug(source="runtime", message="details")
    disabled.close()

    enabled = RuntimeSessionLogs(
        tmp_path / "enabled",
        boot_id_path=boot,
        clock_state_path=tmp_path / "missing-clock",
        debug_enabled=True,
    )
    enabled.append_debug(source="runtime", message="details", details={"value": 1})
    path = enabled.store.session_root(enabled.session_id) / "debug/runtime.jsonl"
    row = json.loads(path.read_text(encoding="utf-8"))
    assert row["session_scoped_debug"] is True
    enabled.close()
