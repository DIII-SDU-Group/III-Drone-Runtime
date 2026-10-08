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
        return [self._line_row(source, line) for line in _tail_lines(source.path, lines)]

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
        return [self._line_row(source, line) for line in _tail_lines(source.path, lines)]

    async def follow(
        self,
        source_id: str,
        *,
        initial_lines: int = 200,
        poll_interval_seconds: float = 0.1,
        max_batch_lines: int = 100,
    ) -> AsyncIterator[dict]:
        sources = self._follow_sources(source_id)
        # Each poll reads only what was appended: re-reading whole files made
        # a follower's CPU grow with the size of the logs it followed.
        followed: list[tuple[LogSource, _AppendedLineReader, list[str]]] = []
        for source in sources:
            if source.path is None or not source.path.exists():
                for line in self._read_lines(source)[-initial_lines:]:
                    yield self._line_row(source, line)
            if source.path is None:
                continue
            reader = _AppendedLineReader(source.path)
            for line in reader.read_new()[-initial_lines:]:
                yield self._line_row(source, line)
            followed.append((source, reader, []))

        while True:
            emitted = False
            for source, reader, backlog in followed:
                backlog.extend(reader.read_new())
                batch = backlog[:max_batch_lines]
                del backlog[:max_batch_lines]
                for line in batch:
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


class _AppendedLineReader:
    """Complete lines appended to a log file since the previous read."""

    def __init__(self, path: Path):
        self._path = path
        self._inode: int | None = None
        self._offset = 0
        self._partial = b""

    def read_new(self) -> list[str]:
        try:
            status = self._path.stat()
            if status.st_ino != self._inode or status.st_size < self._offset:
                # Replaced or truncated: start over.
                self._inode = status.st_ino
                self._offset = 0
                self._partial = b""
            if status.st_size == self._offset:
                return []
            with self._path.open("rb") as handle:
                handle.seek(self._offset)
                data = handle.read(status.st_size - self._offset)
        except OSError:
            return []
        self._offset += len(data)
        chunks = (self._partial + data).split(b"\n")
        # A line still being written stays pending until its newline arrives.
        self._partial = chunks.pop()
        return [chunk.rstrip(b"\r").decode("utf-8", errors="replace") for chunk in chunks]


def _tail_lines(path: Path, count: int, block_bytes: int = 65536) -> list[str]:
    """The last count lines, read backwards from the end of the file."""
    with path.open("rb") as handle:
        position = handle.seek(0, os.SEEK_END)
        data = b""
        while position > 0 and data.count(b"\n") <= count:
            step = min(block_bytes, position)
            position -= step
            handle.seek(position)
            data = handle.read(step) + data
    lines = data.splitlines()
    if position > 0:
        lines = lines[1:]  # starts mid-line
    return [line.decode("utf-8", errors="replace") for line in lines[-count:]] if count > 0 else []


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
