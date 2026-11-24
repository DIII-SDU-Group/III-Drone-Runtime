"""Simple, immediate runtime event logging for developer-operated aircraft."""

from __future__ import annotations

import json
from pathlib import Path
import re
import time
from typing import Any, Mapping
from uuid import uuid4


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


class RuntimeSessionLogs:
    """Append runtime events immediately to a normal writable session directory.

    The prototype Pi has no release receiver or clock authority. Event logging
    must therefore never wait for a signed clock state or durable flush token.
    """

    def __init__(self, root: Path, *, debug_enabled: bool = False) -> None:
        self.root = root
        self.debug_enabled = debug_enabled
        self.session_id = f"runtime-{uuid4().hex[:24]}"
        self.session_root = root / "sessions" / self.session_id
        self.session_root.mkdir(parents=True, exist_ok=True)
        self._closed = False

    def append(self, event: Any) -> None:
        if self._closed:
            raise RuntimeError("runtime session log is already closed")
        self._append(
            "runtime-api",
            {"monotonic_ns": time.monotonic_ns(), "event": _record(event)},
        )

    def append_debug(
        self, *, source: str, message: str, details: Mapping[str, Any] | None = None
    ) -> None:
        if self._closed:
            raise RuntimeError("runtime session log is already closed")
        if not self.debug_enabled:
            raise RuntimeError("runtime session debug mode is not enabled")
        if not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", source):
            raise ValueError("runtime debug source is invalid")
        self._append(
            f"debug-{source}",
            {
                "monotonic_ns": time.monotonic_ns(),
                "message": message,
                "details": dict(details or {}),
                "session_scoped_debug": True,
            },
        )

    def _append(self, source: str, record: Mapping[str, Any]) -> None:
        path = self.session_root / f"{source}.jsonl"
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")

    def close(self) -> None:
        self._closed = True
