from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest

from iii_drone_contracts import (
    InspectionPreflight,
    InspectionPreflightItem,
    MissionDomainState,
    TelemetryFieldState,
    VehicleDomainState,
)
from iii_drone_runtime.api.app import (
    RuntimeApiSettings,
    _telemetry_field_ready,
    create_app,
    require_inspection_preflight,
)
from iii_drone_runtime.api.mission_status import MissionStatusCache


def _mode(
    key: str,
    name: str,
    mode_id: int,
    *,
    active: bool = False,
    tree_running: bool = False,
    tree_finished: bool = False,
    tree_success: bool = False,
):
    return SimpleNamespace(
        stamp=SimpleNamespace(sec=1_800_000_000, nanosec=0),
        mode_key=key,
        display_name=name,
        mode_id=mode_id,
        mode_id_valid=True,
        registered=True,
        active=active,
        tree_running=tree_running,
        tree_finished=tree_finished,
        tree_success=tree_success,
        degraded=False,
        degraded_reason="",
    )


def _eligibility(*, eligible=True, reasons=None):
    return SimpleNamespace(
        stamp=SimpleNamespace(sec=1_800_000_000, nanosec=0),
        evaluable=True,
        eligible=eligible,
        side="positive",
        measured_lateral_clearance_m=2.4,
        required_lateral_clearance_m=2.0,
        between_pylons=True,
        distance_from_start_boundary_m=4.0,
        distance_to_end_boundary_m=7.0,
        pylon_span_margin_m=0.5,
        ingress_point_valid=True,
        ingress_point=SimpleNamespace(x=1.0, y=2.0, z=6.0),
        failure_reasons=reasons or [],
    )


def _client(cache: MissionStatusCache) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            mission_status=cache,
        )
    )


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_inspection_preflight_rejects_all_failed_hard_gates_with_details():
    state = MissionDomainState(
        preflight=InspectionPreflight(
            ready=False,
            items=[
                InspectionPreflightItem(
                    key="gps",
                    label="3D GPS fix",
                    passed=False,
                    source="PX4 SensorGps",
                    detail="fix 2",
                ),
                InspectionPreflightItem(
                    key="manual_link",
                    label="RC/manual-control link",
                    passed=False,
                    hard_gate=False,
                    source="PX4 ManualControlSetpoint",
                ),
                InspectionPreflightItem(
                    key="recording",
                    label="Inspection recording",
                    passed=False,
                    source="rosbag recorder",
                    detail="critically low storage",
                ),
            ],
        )
    )

    with pytest.raises(
        RuntimeError,
        match="3D GPS fix: fix 2; Inspection recording: critically low storage",
    ):
        require_inspection_preflight(state)

    assert state.preflight.advisory_acknowledgement_policy == "informational"
    assert state.preflight.items[1].hard_gate is False


def test_latched_home_position_remains_ready_after_its_source_timestamp_ages():
    vehicle = VehicleDomainState(
        telemetry_fields={
            "home_position_valid": TelemetryFieldState(
                value=True,
                source="PX4 ROS/uXRCE:home_position",
                freshness="stale",
                source_availability="available",
            )
        }
    )

    assert _telemetry_field_ready(vehicle, "home_position_valid", require_fresh=False) is True
    assert _telemetry_field_ready(vehicle, "home_position_valid") is False


def test_mission_status_cache_represents_missing_topic_explicitly():
    state = MissionStatusCache().state()

    assert state.source_availability == "unavailable"
    assert state.required_modes_registered is False
    assert "not been received" in state.degraded_reason


def test_mission_status_cache_exposes_activation_preconditions():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/mission.yaml",
            mission_active=False,
            mission_state_label="ready",
            required_modes=["first", "second"],
            registered_modes=["first"],
            owned_mode="executor",
            modes=[_mode("executor", "Executor", 77)],
            control_owner="mission",
            ready=False,
            degraded=True,
            degraded_reasons=["second mode not registered"],
            required_modes_registered=False,
        )
    )

    state = cache.state()

    assert state.active_spec_id == "/missions/mission.yaml"
    assert state.required_modes_registered is False
    assert state.latest["registered_modes"] == ["first"]
    assert state.latest["mode_id"] == 77
    assert cache.mission_mode_id() == 77
    assert state.modes[0].mode_key == "executor"
    assert state.modes[0].mode_id == 77
    assert state.modes[1].mode_key == "first"
    assert state.modes[1].freshness == "unknown"
    assert state.modes[2].mode_key == "second"
    assert state.modes[2].degraded_reason == "typed mission mode status is missing"
    assert state.latest["activation_allowed"] is False
    assert "required mission modes are not registered" in state.latest["activation_rejections"]


def test_runtime_api_exposes_mission_status_domain():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/mission.yaml",
            mission_active=True,
            mission_state_label="active",
            required_modes=["executor"],
            registered_modes=["executor"],
            owned_mode="executor",
            modes=[_mode("executor", "Inspection Demo", 77, active=True, tree_running=True)],
            control_owner="mission",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )
    client = _client(cache)

    response = client.get("/mission/status", headers=_headers(client))

    assert response.status_code == 200
    payload = response.json()
    assert payload["active_spec_id"] == "/missions/mission.yaml"
    assert payload["required_modes_registered"] is True
    assert payload["latest"]["activation_allowed"] is True
    assert payload["modes"] == [
        {
            "mode_key": "executor",
            "display_name": "Inspection Demo",
            "mode_id": 77,
            "registered": True,
            "active": True,
            "tree_running": True,
            "tree_finished": False,
            "tree_success": None,
            "source_timestamp": "2027-01-15T08:00:00Z",
            "freshness": "fresh",
            "degraded_reason": None,
        }
    ]


def test_mission_status_exposes_canonical_specification_identity_hash_and_load_failure():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/development-override.yaml",
            canonical_mission_specification="/missions/mission_specification.yaml",
            active_mission_specification_hash="sha256:deadbeef",
            canonical_mission_specification_loaded=False,
            mission_specification_load_error="active specification is not the canonical inspection specification",
            configuration_profile="real",
            mission_active=False,
            mission_state_label="not_ready",
            required_modes=["inspection_demo"],
            registered_modes=["inspection_demo"],
            owned_mode="inspection_demo",
            modes=[_mode("inspection_demo", "Inspection Demo", 30)],
            control_owner="",
            ready=False,
            degraded=True,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )

    state = cache.state()

    assert state.specification.active_path == "/missions/development-override.yaml"
    assert state.specification.canonical_path == "/missions/mission_specification.yaml"
    assert state.specification.label == "mission_specification.yaml"
    assert state.specification.content_hash == "sha256:deadbeef"
    assert state.specification.canonical_loaded is False
    assert state.specification.configuration_profile == "real"
    assert state.specification.load_error.startswith("active specification")
    assert "canonical inspection specification is not loaded" in state.latest["activation_rejections"]


def test_registry_exposes_all_inspection_modes_with_live_ids_and_tree_state():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    modes = [
        _mode("inspection_demo", "Inspection Demo", 30, active=True, tree_running=True),
        _mode("reach_cable", "Reach Cable", 31),
        _mode("cable_charging", "Cable Charging", 32, tree_finished=True, tree_success=True),
        _mode("leave_cable", "Leave Cable", 33),
    ]
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/inspection.yaml",
            mission_active=True,
            mission_state_label="active",
            required_modes=[mode.mode_key for mode in modes],
            registered_modes=[mode.mode_key for mode in modes],
            owned_mode="inspection_demo",
            modes=modes,
            control_owner="mission",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )

    state = cache.state()

    assert [(mode.mode_key, mode.display_name, mode.mode_id) for mode in state.modes] == [
        ("inspection_demo", "Inspection Demo", 30),
        ("reach_cable", "Reach Cable", 31),
        ("cable_charging", "Cable Charging", 32),
        ("leave_cable", "Leave Cable", 33),
    ]
    assert state.modes[0].active is True
    assert state.modes[0].tree_running is True
    assert state.modes[2].tree_finished is True
    assert state.modes[2].tree_success is True
    assert cache.mode_id("leave_cable") == 33


def test_intent_completion_reconstructs_from_finished_modes_after_runtime_reconnect():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    modes = [
        _mode("inspection_demo", "Inspection Demo", 30, active=True, tree_running=True),
        _mode("reach_cable", "Reach Cable", 31, tree_finished=True, tree_success=True),
        _mode("cable_charging", "Cable Charging", 32, tree_finished=True, tree_success=True),
        _mode("leave_cable", "Leave Cable", 33, tree_finished=True, tree_success=True),
    ]
    intents = [
        SimpleNamespace(intent_key=key, service_name=f"/{key}", flag_name=key, value=True, sequence_id=index, lifecycle="acknowledged_onboard", detail="accepted")
        for index, key in enumerate(("trigger_recharge_now", "stay_on_cable", "interrupt_recharging_now"), 1)
    ]
    cache.handle_message(SimpleNamespace(
        active_mission_specification="/missions/inspection.yaml",
        mission_active=True,
        mission_state_label="active",
        required_modes=[mode.mode_key for mode in modes],
        registered_modes=[mode.mode_key for mode in modes],
        owned_mode="inspection_demo",
        modes=modes,
        intents=intents,
        control_owner="mission",
        ready=True,
        degraded=False,
        degraded_reasons=[],
        required_modes_registered=True,
    ))

    state = cache.state()

    assert {intent.intent_key: intent.lifecycle for intent in state.intents} == {
        "trigger_recharge_now": "completed",
        "stay_on_cable": "completed",
        "interrupt_recharging_now": "completed",
    }


def test_registry_becomes_explicitly_stale_without_updates():
    now = datetime(2026, 8, 12, tzinfo=timezone.utc)
    cache = MissionStatusCache(stale_after=timedelta(seconds=2), clock=lambda: now)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/inspection.yaml",
            mission_active=False,
            mission_state_label="ready",
            required_modes=["inspection_demo"],
            registered_modes=["inspection_demo"],
            owned_mode="inspection_demo",
            modes=[_mode("inspection_demo", "Inspection Demo", 30)],
            control_owner="",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )
    now += timedelta(seconds=3)

    state = cache.state()

    assert state.freshness == "stale"
    assert state.source_availability == "degraded"
    assert state.modes[0].freshness == "stale"
    assert "mission mode registry is stale" in state.latest["activation_rejections"]


def test_mission_status_exposes_authoritative_inspection_start_eligibility():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/inspection.yaml",
            mission_active=False,
            mission_state_label="ready",
            required_modes=["inspection_demo"],
            registered_modes=["inspection_demo"],
            owned_mode="inspection_demo",
            modes=[_mode("inspection_demo", "Inspection Demo", 30)],
            inspection_start_eligibility=_eligibility(),
            control_owner="",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )

    state = cache.state()

    eligibility = state.inspection_start_eligibility
    assert eligibility is not None
    assert eligibility.eligible is True
    assert eligibility.side == "positive"
    assert eligibility.measured_lateral_clearance_m == 2.4
    assert eligibility.required_lateral_clearance_m == 2.0
    assert eligibility.ingress_x == 1.0
    assert eligibility.ingress_y == 2.0
    assert eligibility.ingress_z == 6.0
    assert state.latest["activation_allowed"] is True


def test_ineligible_geometry_is_an_exact_mission_activation_rejection():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/inspection.yaml",
            mission_active=False,
            mission_state_label="ready",
            required_modes=["inspection_demo"],
            registered_modes=["inspection_demo"],
            owned_mode="inspection_demo",
            modes=[_mode("inspection_demo", "Inspection Demo", 30)],
            inspection_start_eligibility=_eligibility(
                eligible=False,
                reasons=["aircraft is inside the corridor or lacks the required outer-conductor clearance"],
            ),
            control_owner="",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )

    state = cache.state()

    assert state.inspection_start_eligibility.eligible is False
    assert state.latest["activation_rejections"] == [
        "aircraft is inside the corridor or lacks the required outer-conductor clearance"
    ]
