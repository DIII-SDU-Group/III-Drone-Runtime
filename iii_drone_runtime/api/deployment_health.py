"""Publish fail-closed local activation observations for the root receiver."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping


RUNTIME_HEALTH_SCHEMA = "iii.runtime-activation-health/v1"
SAFETY_SCHEMA = "iii.activation-safety/v1"


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def content_identity(value: Mapping[str, Any]) -> str:
    import hashlib

    return hashlib.sha256(canonical_json(value)).hexdigest()


def atomic_document(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    temporary = path.parent / f".{path.name}.partial-{os.getpid()}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o640,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(canonical_json(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists() and not temporary.is_symlink():
            temporary.unlink()


class RuntimeActivationHealthPublisher:
    def __init__(
        self,
        *,
        health_path: Path,
        safety_path: Path,
        health_provider: Callable[[], Mapping[str, Any]],
        safety_provider: Callable[[], Mapping[str, Any]],
    ) -> None:
        if (
            health_path == safety_path
            or health_path.is_symlink()
            or safety_path.is_symlink()
        ):
            raise RuntimeError("activation observation paths are unsafe")
        self.health_path = health_path
        self.safety_path = safety_path
        self.health_provider = health_provider
        self.safety_provider = safety_provider

    def publish(
        self,
        *,
        health_document: Mapping[str, Any] | None = None,
        safety_document: Mapping[str, Any] | None = None,
    ) -> tuple[str, str]:
        health = dict(
            self.health_provider() if health_document is None else health_document
        )
        required_health = {
            "schema",
            "snapshot_id",
            "release_id",
            "profile",
            "boot_id",
            "observed_monotonic",
            "daemon",
            "runtime_api",
            "configuration",
            "hardware_roles",
            "services",
            "managed_nodes",
            "px4",
            "operations",
        }
        if set(health) != required_health or health["schema"] != RUNTIME_HEALTH_SCHEMA:
            raise RuntimeError("runtime activation health fields are malformed")
        health["snapshot_id"] = content_identity(
            {key: item for key, item in health.items() if key != "snapshot_id"}
        )
        safety = dict(
            self.safety_provider() if safety_document is None else safety_document
        )
        if safety.get("schema") != SAFETY_SCHEMA or "observation_id" not in safety:
            raise RuntimeError("runtime activation safety fields are malformed")
        safety["observation_id"] = content_identity(
            {key: item for key, item in safety.items() if key != "observation_id"}
        )
        atomic_document(self.health_path, health)
        atomic_document(self.safety_path, safety)
        return health["snapshot_id"], safety["observation_id"]

    def remove(self) -> None:
        self.health_path.unlink(missing_ok=True)
        self.safety_path.unlink(missing_ok=True)


def selected_checkpoint(
    selector: Path = Path("/var/lib/iii/configuration/current"),
    checkpoint_root: Path = Path("/var/lib/iii/configuration/checkpoints"),
) -> dict[str, Any]:
    if not selector.is_symlink():
        raise RuntimeError("configuration selector is unavailable")
    root = checkpoint_root.resolve()
    selected = selector.resolve(strict=True)
    if not selected.is_relative_to(root) or selected.parent != root:
        raise RuntimeError("configuration selector escapes the checkpoint root")
    manifest_path = selected / "checkpoint.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise RuntimeError("configuration checkpoint manifest is unavailable")
    raw = manifest_path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != canonical_json(value) + b"\n":
        raise RuntimeError("configuration checkpoint manifest is not canonical")
    expected = content_identity(
        {key: item for key, item in value.items() if key != "checkpoint_id"}
    )
    if value.get("checkpoint_id") != expected or selected.name != expected:
        raise RuntimeError("configuration checkpoint identity mismatch")
    return value


def hardware_roles(
    path: Path = Path("/run/iii/hardware-health.json"),
) -> dict[str, dict[str, Any]]:
    if not path.exists() and not path.is_symlink():
        return {}
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("hardware-role health path is unsafe")
    raw = path.read_bytes()
    value = json.loads(raw)
    if (
        not isinstance(value, dict)
        or raw != canonical_json(value) + b"\n"
        or set(value) != {"schema", "snapshot_id", "roles"}
        or value["schema"] != "iii.hardware-health/v1"
        or value["snapshot_id"]
        != content_identity(
            {key: item for key, item in value.items() if key != "snapshot_id"}
        )
        or not isinstance(value["roles"], dict)
    ):
        raise RuntimeError("hardware-role health document is malformed")
    return dict(value["roles"])
