"""Runtime API log source model."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from pathlib import Path
from typing import AsyncIterator


@dataclass(frozen=True)
class LogSource:
    source_id: str
    label: str
    kind: str
    path: Path | None = None

    def as_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "label": self.label,
            "kind": self.kind,
            "path": str(self.path) if self.path else None,
        }


class LogSourceProvider:
    def __init__(self, sources: list[LogSource] | None = None):
        self._sources = sources or _discover_default_sources()

    def list_sources(self) -> list[LogSource]:
        ids = {source.source_id for source in self._sources}
        if "all" not in ids:
            return [*self._sources, LogSource("all", "All logs", "aggregate")]
        return list(self._sources)

    def tail(self, source_id: str, *, lines: int = 200) -> list[dict]:
        source = self._get(source_id)
        if source.source_id == "all":
            rows = []
            for item in self._sources:
                if item.source_id == "all":
                    continue
                rows.extend(self.tail(item.source_id, lines=lines))
            return rows[-lines:]
        if source.path is None:
            return [
                self._line_row(
                    source,
                    f"{source.label} has no readable file-backed log configured for this runtime profile.",
                )
            ]
        if not source.path.exists():
            return [self._line_row(source, f"{source.path} is not readable.")]
        text_lines = source.path.read_text(encoding="utf-8").splitlines()
        return [self._line_row(source, line) for line in text_lines[-lines:]]

    def download(self, source_id: str) -> str:
        lines = self.tail(source_id, lines=10_000)
        return "\n".join(f"[{line['source_id']}] {line['line']}" for line in lines)

    def tail_directory(
        self, source_id: str, directory: Path, *, lines: int = 200
    ) -> list[dict]:
        """Tail a daemon-authenticated entity directory not in the static list."""

        if directory.is_symlink() or not directory.is_dir():
            return [
                self._line_row(
                    LogSource(source_id, source_id, "entity"),
                    f"{directory} is not a readable log directory.",
                )
            ]
        current = directory / "current.log"
        candidates = [path for path in directory.rglob("*.log") if path.is_file()]
        selected = current if current.is_file() else (
            max(candidates, key=lambda path: path.stat().st_mtime)
            if candidates
            else None
        )
        source = LogSource(source_id, source_id, "entity", selected)
        if selected is None:
            return [
                self._line_row(
                    source, f"{directory} contains no readable log file."
                )
            ]
        return self.tail_source(source, lines=lines)

    def tail_source(self, source: LogSource, *, lines: int = 200) -> list[dict]:
        if source.path is None or not source.path.is_file():
            return [self._line_row(source, f"{source.path} is not readable.")]
        text_lines = source.path.read_text(encoding="utf-8").splitlines()
        return [self._line_row(source, line) for line in text_lines[-lines:]]

    async def follow(
        self,
        source_id: str,
        *,
        initial_lines: int = 200,
        poll_interval_seconds: float = 0.1,
        max_batch_lines: int = 100,
    ) -> AsyncIterator[dict]:
        sources = self._follow_sources(source_id)
        cursors: dict[str, int] = {}
        for source in sources:
            lines = self._read_lines(source)
            for line in lines[-initial_lines:]:
                yield self._line_row(source, line)
            cursors[source.source_id] = len(lines)

        while True:
            emitted = False
            for source in sources:
                lines = self._read_lines(source)
                cursor = cursors.get(source.source_id, 0)
                if len(lines) < cursor:
                    cursor = 0
                new_lines = lines[cursor : cursor + max_batch_lines]
                cursors[source.source_id] = cursor + len(new_lines)
                for line in new_lines:
                    emitted = True
                    yield self._line_row(source, line)
            if not emitted:
                await asyncio.sleep(poll_interval_seconds)

    def _get(self, source_id: str) -> LogSource:
        for source in self.list_sources():
            if source.source_id == source_id:
                return source
        raise KeyError(f"unknown log source: {source_id}")

    def _follow_sources(self, source_id: str) -> list[LogSource]:
        source = self._get(source_id)
        if source.source_id == "all":
            return [item for item in self._sources if item.source_id != "all"]
        return [source]

    def _read_lines(self, source: LogSource) -> list[str]:
        if source.path is None:
            return [f"{source.label} has no readable file-backed log configured for this runtime profile."]
        if not source.path.exists():
            return [f"{source.path} is not readable."]
        return source.path.read_text(encoding="utf-8").splitlines()

    def _line_row(self, source: LogSource, line: str) -> dict:
        return {
            "source_id": source.source_id,
            "source_label": source.label,
            "kind": source.kind,
            "line": line,
        }


def _discover_default_sources() -> list[LogSource]:
    log_root = Path(os.environ.get("III_LOG_ROOT", "/var/log/iii"))
    candidates = [
        ("runtime_api", "III runtime API", _first_existing([
            Path(os.environ.get("III_RUNTIME_API_LOG", "")),
            log_root / "runtime_api.log",
            log_root / "iii_runtime_api.log",
        ])),
        ("daemon", "III system daemon", _first_existing([
            Path(os.environ.get("III_DAEMON_LOG", "")),
            log_root / "iii_daemon.log",
            log_root / "daemon.log",
        ])),
        ("runtime", "Runtime logs", _latest_log_file(log_root)),
    ]
    sources = [LogSource(source_id, label, "file", path) for source_id, label, path in candidates if path is not None]
    if not sources:
        sources = [
            LogSource("daemon", "III system daemon", "journal"),
            LogSource("runtime_api", "III runtime API", "journal"),
        ]
    sources.append(LogSource("all", "All logs", "aggregate"))
    return sources


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if str(path) and path.exists() and path.is_file():
            return path
    return None


def _latest_log_file(root: Path) -> Path | None:
    if not root.exists():
        return None
    files = [path for path in root.rglob("*.log") if path.is_file()]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)
