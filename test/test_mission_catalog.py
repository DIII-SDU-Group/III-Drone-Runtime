from types import SimpleNamespace

import pytest

from iii_drone_contracts import CommandId, CommandRequest, HandlerPermission, MissionDomainState
from iii_drone_runtime.api.dispatch import DispatchRegistry
from iii_drone_runtime.api.events import RuntimeEventLog
from iii_drone_runtime.api.mission_catalog import (
    MissionCatalogCommandHandlers,
    MissionCatalogSelectionGate,
    _catalog_id,
    _validate_selection_evidence,
)


class FakeCatalogService:
    def __init__(self):
        self.selections = []

    def catalog(self, *, include_incompatible):
        entries = [
            {
                "id": "inspection-production",
                "classification": "production",
                "available": True,
                "dependencies": ["sha256:" + "a" * 64],
            },
            {
                "id": "inspection-test",
                "classification": "test",
                "available": False,
                "unavailable_reason": "incompatible with profile real",
                "dependencies": ["sha256:" + "b" * 64],
            },
        ]
        if not include_incompatible:
            entries = entries[:1]
        return {
            "schema": "iii.mission-catalog/v1",
            "catalog_hash": "sha256:" + "c" * 64,
            "scope": "local",
            "active_profile": "real",
            "entries": entries,
        }

    def select(self, *, catalog_id, use_default):
        self.selections.append((catalog_id, use_default))
        return {
            "success": True,
            "message": "selected",
            "active_catalog_id": "inspection-production" if use_default else catalog_id,
            "active_catalog_hash": "sha256:" + "c" * 64,
            "active_entry_hash": "sha256:" + "d" * 64,
            "active_specification_asset_id": "sha256:" + "e" * 64,
            "active_behavior_tree_asset_ids": ["sha256:" + "f" * 64],
            "temporary_override": not use_default,
            "warning": "EXPERIMENTAL mission" if catalog_id == "inspection-experimental" else None,
        }


def _state(*, mission_active=False, operation_active=False, freshness="fresh"):
    mission = MissionDomainState(
        source_label="mission",
        freshness=freshness,
        source_availability="available",
        latest={"mission_active": mission_active},
    )
    operation = SimpleNamespace(
        freshness=freshness,
        latest={"operation_active": operation_active},
    )
    vehicle = SimpleNamespace(
        freshness=freshness,
        source_availability="available",
        armed=False,
        in_air=False,
        nav_state="hold",
    )
    return mission, operation, vehicle


def _handlers(*, profile="sim", mission_active=False, operation_active=False, vehicle=None):
    mission, operation, default_vehicle = _state(
        mission_active=mission_active,
        operation_active=operation_active,
    )
    service = FakeCatalogService()
    gate = MissionCatalogSelectionGate(
        profile=profile,
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        vehicle_state_provider=lambda: vehicle or default_vehicle,
    )
    event_log = RuntimeEventLog()
    handlers = MissionCatalogCommandHandlers(
        service=service,
        status_provider=lambda: mission,
        selection_gate=gate,
        event_log=event_log,
    )
    return handlers, service, event_log


def _request(command_id, parameters=None):
    return CommandRequest(
        request_id=f"test-{command_id}",
        command_id=command_id,
        parameters=parameters or {},
    )


def test_registry_declares_read_and_runtime_mutation_permissions():
    handlers, _service, _events = _handlers()
    registry = DispatchRegistry.empty()
    handlers.register(registry)
    assert registry.action_permission(CommandId.MISSION_CATALOG_STATUS.value) == HandlerPermission.READ_ONLY
    assert registry.action_permission(CommandId.MISSION_CATALOG_LIST.value) == HandlerPermission.READ_ONLY
    assert registry.action_permission(CommandId.MISSION_CATALOG_SHOW.value) == HandlerPermission.READ_ONLY
    assert registry.action_permission(CommandId.MISSION_CATALOG_SELECT.value) == HandlerPermission.RUNTIME_MUTATION


def test_list_all_and_show_expose_metadata_without_target_paths():
    handlers, _service, _events = _handlers()
    listed = handlers.handle(
        _request(CommandId.MISSION_CATALOG_LIST.value, {"all": True})
    )
    assert listed.accepted is True
    assert len(listed.result["catalog"]["entries"]) == 2
    assert listed.result["catalog"]["entries"][1]["unavailable_reason"]
    shown = handlers.handle(
        _request(
            CommandId.MISSION_CATALOG_SHOW.value,
            {"catalog_id": "inspection-production", "all": True},
        )
    )
    assert shown.accepted is True
    assert shown.result["entry"]["id"] == "inspection-production"
    assert "/home/" not in str(shown.result)


def test_sim_selection_requires_no_active_mission_or_operation():
    handlers, service, events = _handlers(profile="sim")
    selected = handlers.handle(
        _request(CommandId.MISSION_CATALOG_SELECT.value, {"catalog_id": "inspection-experimental"})
    )
    assert selected.accepted is True
    assert service.selections == [("inspection-experimental", False)]
    assert "WARNING: EXPERIMENTAL" in selected.message
    decision = events.recent()[-1]
    assert decision.details["active_catalog_id"] == "inspection-experimental"
    assert decision.details["active_specification_asset_id"] == "sha256:" + "e" * 64
    assert decision.details["active_behavior_tree_asset_ids"] == ["sha256:" + "f" * 64]

    handlers, _, _ = _handlers(profile="sim", mission_active=True)
    assert handlers.handle(
        _request(CommandId.MISSION_CATALOG_SELECT.value, {"catalog_id": "inspection-production"})
    ).accepted is False
    handlers, _, _ = _handlers(profile="sim", operation_active=True)
    assert handlers.handle(
        _request(CommandId.MISSION_CATALOG_SELECT.value, {"catalog_id": "inspection-production"})
    ).accepted is False


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"freshness": "stale"}, "PX4 vehicle state is stale"),
        ({"source_availability": "degraded"}, "PX4 vehicle state is degraded"),
        ({"armed": True}, "not confirmed disarmed"),
        ({"in_air": True}, "not confirmed landed"),
        ({"nav_state": "mission"}, "not maintenance-safe"),
    ],
)
def test_real_selection_enforces_full_maintenance_state(overrides, reason):
    _mission, _operation, vehicle = _state()
    for key, value in overrides.items():
        setattr(vehicle, key, value)
    handlers, service, _events = _handlers(profile="real", vehicle=vehicle)
    response = handlers.handle(
        _request(CommandId.MISSION_CATALOG_SELECT.value, {"catalog_id": "inspection-production"})
    )
    assert response.accepted is False
    assert reason in response.message
    assert service.selections == []


def test_select_default_is_explicit_and_paths_are_rejected():
    handlers, service, _events = _handlers(profile="sim")
    response = handlers.handle(
        _request(CommandId.MISSION_CATALOG_SELECT.value, {"default": True})
    )
    assert response.accepted is True
    assert service.selections == [("", True)]
    with pytest.raises(RuntimeError, match="filesystem paths are forbidden"):
        _catalog_id({"catalog_id": "/tmp/mission.yaml"})


def test_selection_evidence_requires_exact_catalog_specification_and_tree_ids():
    valid = FakeCatalogService().select(catalog_id="inspection-production", use_default=False)
    _validate_selection_evidence(valid)
    for field in (
        "active_catalog_hash",
        "active_entry_hash",
        "active_specification_asset_id",
    ):
        malformed = dict(valid)
        malformed[field] = ""
        with pytest.raises(RuntimeError, match=field):
            _validate_selection_evidence(malformed)
    malformed = dict(valid)
    malformed["active_behavior_tree_asset_ids"] = []
    with pytest.raises(RuntimeError, match="behavior-tree asset identities"):
        _validate_selection_evidence(malformed)
