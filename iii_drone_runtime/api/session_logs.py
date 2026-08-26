"""Runtime adapter for receiver-governed session logging contracts."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Mapping
from uuid import uuid4

SESSION_ID = re.compile(r"^[a-z0-9][a-z0-9-]{7,63}$")


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _identity(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        _canonical({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _atomic(path: Path, value: Mapping[str, Any], *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    temporary = path.parent / f".{path.name}.partial-{os.getpid()}-{uuid4().hex}"
    try:
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode
        )
        try:
            raw = _canonical(value) + b"\n"
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("runtime session metadata write made no progress")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists() and not temporary.is_symlink():
            temporary.unlink()


@dataclass(frozen=True)
class _Policy:
    degraded_max_records: int = 10_000
    degraded_max_bytes: int = 16 * 1024**2
    debug_session_max_bytes: int = 256 * 1024**2


class _RuntimeSessionStore:
    """Runtime-owned writer for the deployment-governed log-session schema."""

    def __init__(self, root: Path, policy: _Policy) -> None:
        self.root = root
        self.policy = policy
        sessions = root / "sessions"
        if sessions.exists() and not sessions.is_symlink() and sessions.is_dir():
            for partial in sessions.glob("*/.session.json.partial-*"):
                if partial.is_symlink() or not partial.is_file():
                    raise RuntimeError("runtime session partial is unsafe")
                partial.unlink()

    def session_root(self, session_id: str) -> Path:
        if not SESSION_ID.fullmatch(session_id):
            raise RuntimeError("invalid runtime session identity")
        return self.root / "sessions" / session_id

    def _session(self, session_id: str) -> dict[str, Any]:
        path = self.session_root(session_id) / "session.json"
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("runtime session metadata is missing or linked")
        raw = path.read_bytes()
        value = json.loads(raw)
        if not isinstance(value, dict) or raw != _canonical(value) + b"\n":
            raise RuntimeError("runtime session metadata is not canonical JSON")
        if value.get("session_identity") != _identity(value, "session_identity"):
            raise RuntimeError("runtime session metadata identity mismatch")
        return value

    def sessions(self) -> list[dict[str, Any]]:
        root = self.root / "sessions"
        if not root.exists() and not root.is_symlink():
            return []
        if root.is_symlink() or not root.is_dir():
            raise RuntimeError("runtime session root is unsafe")
        values = []
        for path in sorted(root.iterdir(), key=lambda item: item.name):
            if path.is_symlink() or not path.is_dir():
                raise RuntimeError("runtime session inventory is unsafe")
            values.append(self._session(path.name))
        return values

    def begin(
        self,
        *,
        session_id: str,
        boot_id: str,
        started_monotonic_ns: int,
        debug_enabled: bool,
    ) -> None:
        sessions = self.sessions()
        if any(item["state"] == "current" for item in sessions):
            raise RuntimeError("another runtime session is current")
        value: dict[str, Any] = {
            "schema": "iii.log-session/v1",
            "session_identity": "0" * 64,
            "session_id": session_id,
            "sequence": max(
                (int(item.get("sequence", 0)) for item in sessions), default=0
            )
            + 1,
            "boot_id": boot_id,
            "state": "current",
            "started_monotonic_ns": started_monotonic_ns,
            "started_utc": None,
            "completed_utc": None,
            "completion_reason": None,
            "debug_enabled": debug_enabled,
            "last_transitions": {},
        }
        value["session_identity"] = _identity(value, "session_identity")
        _atomic(self.session_root(session_id) / "session.json", value, mode=0o640)

    def complete(
        self, session_id: str, *, completed_utc: str | None, reason: str
    ) -> None:
        value = self._session(session_id)
        if value["state"] != "current":
            raise RuntimeError("only a current runtime session can complete")
        value.update(
            state="completed",
            completed_utc=completed_utc,
            completion_reason=reason,
        )
        value["session_identity"] = _identity(value, "session_identity")
        _atomic(self.session_root(session_id) / "session.json", value, mode=0o440)

    def recover_interrupted(self, *, boot_id: str) -> None:
        for value in self.sessions():
            if value["state"] == "current":
                self.complete(
                    value["session_id"],
                    completed_utc=None,
                    reason=(
                        "process-restart"
                        if value["boot_id"] == boot_id
                        else "boot-interrupted"
                    ),
                )

    def append(
        self,
        session_id: str,
        *,
        source: str,
        record: Mapping[str, Any],
        debug: bool = False,
        transition_key: str | None = None,
        transition_value: str | None = None,
    ) -> bool:
        session = self._session(session_id)
        if session["state"] != "current":
            raise RuntimeError("completed runtime session is immutable")
        if debug and not session["debug_enabled"]:
            raise RuntimeError("debug logging is not enabled for this session")
        if transition_key is not None:
            if session["last_transitions"].get(transition_key) == transition_value:
                return False
            session["last_transitions"][transition_key] = transition_value
            session["session_identity"] = _identity(session, "session_identity")
            _atomic(self.session_root(session_id) / "session.json", session, mode=0o640)
        directory = self.session_root(session_id) / ("debug" if debug else "logs")
        directory.mkdir(parents=True, exist_ok=True, mode=0o750)
        path = directory / f"{source}.jsonl"
        if path.is_symlink():
            raise RuntimeError("runtime session log is linked")
        raw = _canonical(dict(record)) + b"\n"
        if debug:
            used = 0
            for candidate in directory.glob("*.jsonl"):
                if candidate.is_symlink() or not candidate.is_file():
                    raise RuntimeError("runtime debug log inventory is unsafe")
                used += candidate.stat().st_size
            if used + len(raw) > self.policy.debug_session_max_bytes:
                raise RuntimeError("debug session log cap would be exceeded")
        descriptor = os.open(
            path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o640
        )
        try:
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("runtime session append made no progress")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return True


class _PreclockRing:
    def __init__(self, *, boot_id: str, policy: _Policy) -> None:
        self.boot_id = boot_id
        self.policy = policy
        self._rows: deque[tuple[dict[str, Any], int]] = deque()
        self._bytes = 0
        self.dropped_records = 0
        self._flushed = False

    def append(self, **record: Any) -> None:
        if self._flushed:
            raise RuntimeError("preclock ring was already flushed")
        row = {"boot_id": self.boot_id, **record}
        size = len(_canonical(row)) + 1
        self._rows.append((row, size))
        self._bytes += size
        while (
            len(self._rows) > self.policy.degraded_max_records
            or self._bytes > self.policy.degraded_max_bytes
        ):
            _row, removed = self._rows.popleft()
            self._bytes -= removed
            self.dropped_records += 1

    def flush(
        self,
        store: _RuntimeSessionStore,
        session_id: str,
        *,
        synchronized_monotonic_ns: int,
        synchronized_utc_ns: int,
        uncertainty_ns: int,
    ) -> None:
        if self._flushed:
            raise RuntimeError("preclock ring was already flushed")
        written = 0
        for row, _size in self._rows:
            estimate = synchronized_utc_ns + (
                row["monotonic_ns"] - synchronized_monotonic_ns
            )
            store.append(
                session_id,
                source="preclock",
                record={
                    **row,
                    "utc_estimate_ns": estimate,
                    "utc_lower_ns": estimate - uncertainty_ns,
                    "utc_upper_ns": estimate + uncertainty_ns,
                    "utc_reconstructed": True,
                    "utc_uncertainty_ns": uncertainty_ns,
                },
            )
            written += 1
        store.append(
            session_id,
            source="preclock",
            record={
                "boot_id": self.boot_id,
                "kind": "preclock-flush",
                "records_flushed": written,
                "dropped_records": self.dropped_records,
                "utc_reconstructed": True,
                "utc_uncertainty_ns": uncertainty_ns,
            },
        )
        self._rows.clear()
        self._bytes = 0
        self._flushed = True


class RuntimeSessionLogs:
    """Persist runtime events without trusting wall time before receiver sync."""

    def __init__(
        self,
        root: Path,
        *,
        clock_state_path: Path = Path("/var/lib/iii/deployment/clock-state.json"),
        boot_id_path: Path = Path("/proc/sys/kernel/random/boot_id"),
        debug_enabled: bool = False,
    ) -> None:
        self.root = root
        self.clock_state_path = clock_state_path
        self.boot_id_path = boot_id_path
        self.debug_enabled = debug_enabled
        self.policy = _Policy()
        self.store = _RuntimeSessionStore(root, self.policy)
        self.boot_id = self._boot_id()
        self.store.recover_interrupted(boot_id=self.boot_id)
        self.session_id = f"runtime-{uuid4().hex[:24]}"
        self.store.begin(
            session_id=self.session_id,
            boot_id=self.boot_id,
            started_monotonic_ns=time.monotonic_ns(),
            debug_enabled=debug_enabled,
        )
        self.ring = _PreclockRing(boot_id=self.boot_id, policy=self.policy)
        self._clock_was_flushed = False
        self._closed = False
        self._mutex = threading.RLock()

    def _boot_id(self) -> str:
        try:
            value = self.boot_id_path.read_text(encoding="ascii").strip()
        except OSError as exc:
            raise RuntimeError("cannot read runtime boot identity") from exc
        if not value:
            raise RuntimeError("runtime boot identity is empty")
        return value

    def _clock_mapping(self) -> dict[str, int] | None:
        try:
            if (
                self.clock_state_path.is_symlink()
                or not self.clock_state_path.is_file()
            ):
                return None
            raw = self.clock_state_path.read_bytes()
            value = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if (
            not isinstance(value, dict)
            or raw != _canonical(value) + b"\n"
            or value.get("schema") != "iii.receiver-clock-state/v1"
            or value.get("state_id") != _identity(value, "state_id")
            or value.get("boot_id") != self.boot_id
            or value.get("gate") != "OPERATIONAL"
        ):
            return None
        fields = ("synchronized_monotonic_ns", "synchronized_utc_ns", "uncertainty_ns")
        if any(
            not isinstance(value.get(field), int) or isinstance(value.get(field), bool)
            for field in fields
        ) or any(value[field] < 0 for field in fields):
            return None
        return {field: int(value[field]) for field in fields}

    @staticmethod
    def _record(event: Any) -> dict[str, Any]:
        if hasattr(event, "model_dump"):
            value = event.model_dump(mode="json")
        elif isinstance(event, Mapping):
            value = dict(event)
        else:
            raise TypeError("runtime log event is not contract serializable")
        if not isinstance(value, dict):
            raise TypeError("runtime log event must serialize to an object")
        return value

    def append(self, event: Any) -> None:
        with self._mutex:
            self._append(event)

    def _flush_if_synchronized(self, mapping: Mapping[str, int] | None) -> None:
        if mapping is None or self._clock_was_flushed:
            return
        self.ring.flush(
            self.store,
            self.session_id,
            synchronized_monotonic_ns=mapping["synchronized_monotonic_ns"],
            synchronized_utc_ns=mapping["synchronized_utc_ns"],
            uncertainty_ns=mapping["uncertainty_ns"],
        )
        self._clock_was_flushed = True

    def _append(self, event: Any) -> None:
        if self._closed:
            raise RuntimeError("runtime session log is already closed")
        record = self._record(event)
        monotonic_ns = time.monotonic_ns()
        mapping = self._clock_mapping()
        if mapping is None and not self._clock_was_flushed:
            self.ring.append(
                monotonic_ns=monotonic_ns,
                source="runtime-api",
                severity=str(record.get("severity", "info")),
                message=str(record.get("message", record.get("category", "event"))),
                details=record,
            )
            return
        self._flush_if_synchronized(mapping)
        timestamp: dict[str, Any]
        if mapping is None:
            timestamp = {
                "utc_reconstructed": False,
                "clock_trusted": False,
            }
        else:
            estimate = mapping["synchronized_utc_ns"] + (
                monotonic_ns - mapping["synchronized_monotonic_ns"]
            )
            timestamp = {
                "utc_estimate_ns": estimate,
                "utc_lower_ns": estimate - mapping["uncertainty_ns"],
                "utc_upper_ns": estimate + mapping["uncertainty_ns"],
                "utc_reconstructed": True,
                "utc_uncertainty_ns": mapping["uncertainty_ns"],
                "clock_trusted": True,
            }
        transition_key = None
        transition_value = None
        if record.get("category") == "availability":
            details = record.get("details") or {}
            transition_key = f"availability:{details.get('label')}"
            transition_value = json.dumps(
                {
                    "available": details.get("available"),
                    "reason": details.get("reason"),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        self.store.append(
            self.session_id,
            source="runtime-api",
            record={
                "boot_id": self.boot_id,
                "monotonic_ns": monotonic_ns,
                **timestamp,
                "event": record,
            },
            transition_key=transition_key,
            transition_value=transition_value,
        )

    def append_debug(
        self, *, source: str, message: str, details: Mapping[str, Any] | None = None
    ) -> None:
        with self._mutex:
            if not self.debug_enabled:
                raise RuntimeError("runtime session debug mode is not enabled")
            if not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", source):
                raise ValueError("runtime debug source is invalid")
            self.store.append(
                self.session_id,
                source=source,
                debug=True,
                record={
                    "boot_id": self.boot_id,
                    "monotonic_ns": time.monotonic_ns(),
                    "message": message,
                    "details": dict(details or {}),
                    "session_scoped_debug": True,
                },
            )

    def close(self) -> None:
        with self._mutex:
            if self._closed:
                return
            mapping = self._clock_mapping()
            self._flush_if_synchronized(mapping)
            completed_utc = None
            if mapping is not None:
                estimate = mapping["synchronized_utc_ns"] + (
                    time.monotonic_ns() - mapping["synchronized_monotonic_ns"]
                )
                completed_utc = (
                    datetime.fromtimestamp(estimate / 1_000_000_000, tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            self.store.complete(
                self.session_id,
                completed_utc=completed_utc,
                reason="clean-shutdown",
            )
            self._closed = True
