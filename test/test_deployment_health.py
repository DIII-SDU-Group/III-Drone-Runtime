from pathlib import Path

import pytest

from iii_drone_runtime.api.deployment_health import (
    RuntimeActivationHealthPublisher,
    canonical_json,
    content_identity,
    hardware_roles,
    selected_checkpoint,
)


def _health():
    return {
        "schema": "iii.runtime-activation-health/v1",
        "snapshot_id": "0" * 64,
        "release_id": "a" * 64,
        "profile": "real",
        "boot_id": "boot-a",
        "observed_monotonic": 12.0,
        "daemon": {},
        "runtime_api": {},
        "configuration": {},
        "hardware_roles": {},
        "services": {},
        "managed_nodes": {},
        "px4": {},
        "operations": {},
    }


def _safety():
    return {
        "schema": "iii.activation-safety/v1",
        "logical_target": "drone",
        "profile": "real",
        "observation_id": "0" * 64,
        "runtime_api_available": True,
        "runtime_identity_matches": True,
        "runtime_fresh": True,
        "px4_available": True,
        "px4_fresh": True,
        "armed": False,
        "in_air": False,
        "nav_state": "hold",
        "failsafe": False,
        "mission_fresh": True,
        "mission_active": False,
        "mission_control_owner": False,
        "operation_fresh": True,
        "custom_operation_active": False,
        "custom_operation_control_owner": False,
        "direct_operation_active": False,
        "reference_owner_active": False,
        "configuration_migration_ready": True,
        "configuration_checkpoint_id": "b" * 64,
        "continuously_safe_for_s": 3.0,
    }


def test_publisher_writes_canonical_identity_bound_health_and_safety(tmp_path: Path):
    health_path = tmp_path / "run/health.json"
    safety_path = tmp_path / "run/safety.json"
    publisher = RuntimeActivationHealthPublisher(
        health_path=health_path,
        safety_path=safety_path,
        health_provider=_health,
        safety_provider=_safety,
    )
    health_id, safety_id = publisher.publish()
    health = __import__("json").loads(health_path.read_bytes())
    safety = __import__("json").loads(safety_path.read_bytes())
    assert health_path.read_bytes() == canonical_json(health) + b"\n"
    assert safety_path.read_bytes() == canonical_json(safety) + b"\n"
    assert health_id == content_identity(
        {key: value for key, value in health.items() if key != "snapshot_id"}
    )
    assert safety_id == content_identity(
        {key: value for key, value in safety.items() if key != "observation_id"}
    )
    publisher.remove()
    assert not health_path.exists()
    assert not safety_path.exists()


def test_publisher_rejects_extra_runtime_fields(tmp_path: Path):
    value = _health()
    value["command"] = "arbitrary"
    publisher = RuntimeActivationHealthPublisher(
        health_path=tmp_path / "health.json",
        safety_path=tmp_path / "safety.json",
        health_provider=lambda: value,
        safety_provider=_safety,
    )
    with pytest.raises(RuntimeError, match="fields are malformed"):
        publisher.publish()


def test_selected_checkpoint_requires_fixed_root_identity_and_canonical_manifest(
    tmp_path: Path,
):
    root = tmp_path / "var/lib/iii/configuration/checkpoints"
    value = {
        "schema": "iii.configuration-checkpoint/v1",
        "checkpoint_id": "0" * 64,
        "schema_version": 1,
        "profile": "real",
        "values_hash": "c" * 64,
    }
    value["checkpoint_id"] = content_identity(
        {key: item for key, item in value.items() if key != "checkpoint_id"}
    )
    checkpoint = root / value["checkpoint_id"]
    checkpoint.mkdir(parents=True)
    (checkpoint / "checkpoint.json").write_bytes(canonical_json(value) + b"\n")
    selector = tmp_path / "current"
    selector.symlink_to(checkpoint)
    assert selected_checkpoint(selector, root) == value
    outside = tmp_path / "outside"
    outside.mkdir()
    selector.unlink()
    selector.symlink_to(outside)
    with pytest.raises(RuntimeError, match="escapes"):
        selected_checkpoint(selector, root)


def test_selected_checkpoint_authenticates_writable_copy_against_sealed_origin(
    tmp_path: Path,
):
    root = tmp_path / "var/lib/iii/configuration/checkpoints"
    working_root = tmp_path / "var/lib/iii/configuration/working"
    value = {
        "schema": "iii.configuration-checkpoint/v1",
        "checkpoint_id": "0" * 64,
        "schema_version": 1,
        "profile": "real",
        "values_hash": "c" * 64,
    }
    value["checkpoint_id"] = content_identity(
        {key: item for key, item in value.items() if key != "checkpoint_id"}
    )
    checkpoint = root / value["checkpoint_id"]
    checkpoint.mkdir(parents=True)
    (checkpoint / "checkpoint.json").write_bytes(canonical_json(value) + b"\n")
    working = working_root / value["checkpoint_id"]
    working.mkdir(parents=True)
    (working / "mutable.yaml").write_text("changed: true\n", encoding="utf-8")
    selector = tmp_path / "current"
    selector.symlink_to(working)

    assert selected_checkpoint(selector, root, working_root) == value

    unbound = working_root / ("f" * 64)
    unbound.mkdir()
    selector.unlink()
    selector.symlink_to(unbound)
    with pytest.raises(RuntimeError, match="manifest is unavailable"):
        selected_checkpoint(selector, root, working_root)


def test_hardware_role_observation_is_identity_bound_and_never_auto_learned(
    tmp_path: Path,
):
    path = tmp_path / "hardware-health.json"
    value = {
        "schema": "iii.hardware-health/v1",
        "snapshot_id": "0" * 64,
        "roles": {"fmu": {"state": "present", "unambiguous": True}},
    }
    value["snapshot_id"] = content_identity(
        {key: item for key, item in value.items() if key != "snapshot_id"}
    )
    path.write_bytes(canonical_json(value) + b"\n")
    assert hardware_roles(path) == value["roles"]
    value["roles"]["learned-device"] = {"state": "present", "unambiguous": True}
    path.write_bytes(canonical_json(value) + b"\n")
    with pytest.raises(RuntimeError, match="malformed"):
        hardware_roles(path)
