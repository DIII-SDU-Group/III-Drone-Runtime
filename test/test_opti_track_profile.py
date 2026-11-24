"""opti_track mission activation: an external-vision preflight and recording set.

Indoors there is no GPS fix, stored overview, powerline, pylon, start geometry
or payload; the activation precondition instead requires local position, the
EKF global origin, PX4 external-vision fusion and a healthy pose relay, and the
mission recording follows the relay and PX4 estimate.
"""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from iii_drone_contracts import (
    CommandId,
    ExternalVisionState,
    MissionDomainState,
    MissionModeRegistryEntry,
    MissionSpecificationIdentity,
    SystemDomainState,
    TelemetryFieldState,
    VehicleDomainState,
)
from iii_drone_runtime.api.app import RuntimeApiSettings, classify_operational_safety, create_app
from iii_drone_runtime.api.clock_sync import ChronyClockMonitor
from iii_drone_runtime.api.flight_commands import DroneAwarenessState
from iii_drone_runtime.api.rosbag import OPTI_TRACK_RECORDING_TOPICS
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_clock_sync import SYNCED
from test_rosbag import _FakeRosbagAdapter
from test_runtime_commands import _FakeDaemonClient, _FakeSystemd


MODE_KEY = "opti_track_flight"
ENTRY_HASH = "sha256:" + "b" * 64

# Installed OptiTrack missions: which modes their specification lets start
# from a disarmed, landed aircraft (the mission then arms it).
OPTI_TRACK_MISSIONS = {
    "opti-track-hover": {"ot_hover": False},
    "opti-track-maneuvers": {"ot_maneuvers": False},
    "opti-track-cycle": {"ot_cycle_takeoff": True, "ot_cycle_shuttle": False, "ot_cycle_land": False},
    "opti-track-lemniscate": {
        "otl_takeoff": True,
        "otl_lower_stop": False,
        "otl_upper_stop": False,
        "otl_lower_blend": False,
        "otl_upper_blend": False,
    },
}

OPTI_TRACK_PREFLIGHT_KEYS = [
    "system",
    "vehicle_state",
    "air_state",
    "local_position",
    "ekf_origin",
    "vision_fusion",
    "pose_relay",
    "arming_checks",
    "manual_link",
    "configuration",
    "mission_modes",
    "battery",
    "storage",
    "control_owner",
    "clock",
    "operator_link",
]


def _ready_vision(**overrides):
    values = dict(
        ready=True,
        freshness="fresh",
        relay_level="ok",
        relay_freshness="fresh",
        relay_stale=False,
        input_rate_hz=120.0,
        ev_pos_fused=True,
        ev_hgt_fused=True,
        ev_yaw_fused=True,
        fusion_freshness="fresh",
        origin_valid=True,
        origin_freshness="fresh",
    )
    values.update(overrides)
    return ExternalVisionState(**values)


def _evidence(value):
    return TelemetryFieldState(value=value, source="test", freshness="fresh", source_availability="available")


class _Vehicle:
    def __init__(self, vision, *, armed=True, in_air=True, arming_checks_passed=True):
        self.vehicle = VehicleDomainState(
            source_label="px4_fusion",
            freshness="fresh",
            source_availability="available",
            armed=armed,
            in_air=in_air,
            nav_state="hold" if in_air else "position",
            failsafe=False,
            arming_checks_passed=arming_checks_passed,
            battery_voltage_v=15.8,
            telemetry_fields={
                "armed": _evidence(armed),
                "in_air": _evidence(in_air),
                "local_position_valid": _evidence(True),
                "arming_checks_passed": _evidence(arming_checks_passed),
                "rc_link_available": _evidence(True),
                "battery_voltage_v": _evidence(15.8),
                # Indoors: no GPS fix, global position or home.
                "gps_fix_type": _evidence(0),
            },
            latest={"command_transport": {"command_available": True}},
            external_vision=vision,
        )

    def state(self):
        return self.vehicle.model_copy(deep=True)

    def dangerous_command_rejection_reason(self):
        return None


class _Mission:
    def __init__(self, catalog_id="opti-track-flight", mode_keys=(MODE_KEY,)):
        self.mission = MissionDomainState(
            source_label="mission_status",
            freshness="fresh",
            source_availability="available",
            active_spec_id=catalog_id,
            mission_state="ready",
            required_modes_registered=True,
            modes=[
                MissionModeRegistryEntry(
                    mode_key=mode_key,
                    display_name=mode_key,
                    mode_id=30 + index,
                    registered=True,
                    freshness="fresh",
                )
                for index, mode_key in enumerate(mode_keys)
            ],
            specification=MissionSpecificationIdentity(
                catalog_id=catalog_id,
                catalog_hash="sha256:" + "a" * 64,
                entry_hash=ENTRY_HASH,
                catalog_ready=True,
                active_profile="opti_track",
            ),
            latest={"owned_mode": mode_keys[0], "activation_rejections": [], "mission_active": False},
        )

    def state(self):
        return self.mission.model_copy(deep=True)

    def set_system_running(self, running):
        del running

    def registered_mode_ids(self):
        return frozenset(mode.mode_id for mode in self.mission.modes)

    def mode_id(self, mode_key):
        return next((mode.mode_id for mode in self.mission.modes if mode.mode_key == mode_key), None)

    def subscribe(self, node):
        del node
        return []


class _Supervision:
    def state(self):
        return SystemDomainState(
            source_label="supervision_health",
            freshness="fresh",
            source_availability="available",
            api_state="up",
            daemon_state="ready",
            booted=True,
            active=True,
        )

    def subscribe(self, node):
        del node

    def subsystem_health(self):
        return []


class _Catalog:
    def __init__(self, missions):
        self.missions = missions
        self.reads = 0

    def catalog(self, *, include_incompatible):
        del include_incompatible
        self.reads += 1
        return {
            "schema": "iii.mission-catalog/v1",
            "entries": [
                {
                    "id": catalog_id,
                    "entry_hash": ENTRY_HASH,
                    "specification": {
                        "executor_owned_mode": next(iter(modes)),
                        "entries": [
                            {"key": key, "mode_name": key, "allow_activate_when_disarmed": allowed}
                            for key, allowed in modes.items()
                        ],
                    },
                }
                for catalog_id, modes in self.missions.items()
            ],
        }

    def select(self, *, catalog_id, use_default):
        raise AssertionError("selection is not part of these tests")


class _ModeAdapter:
    def __init__(self):
        self.requests = []

    def request_mode(self, target, *, mode_key=None, expected_mode_id=None):
        self.requests.append((target, mode_key, expected_mode_id))
        return {"requested": target}


def _client(
    profile="opti_track",
    vision=None,
    rosbag=None,
    mode_adapter=None,
    *,
    vehicle=None,
    mission=None,
    catalog=None,
    awareness=None,
):
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(runtime_id="iii-runtime", profile=profile),
            system_adapter=RuntimeSystemAdapter(daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()),
            supervision_health=_Supervision(),
            mission_status=mission or _Mission(),
            mission_catalog_service=catalog or _Catalog(OPTI_TRACK_MISSIONS),
            drone_awareness=awareness,
            px4_state_provider=vehicle or _Vehicle(_ready_vision() if vision is None else vision),
            configuration_adapter=SimpleNamespace(
                manifest_parameter_values=lambda names: {name: 14.0 for name in names}
            ),
            rosbag_adapter=rosbag or _FakeRosbagAdapter(),
            clock_monitor=ChronyClockMonitor(enabled=True, tracking=lambda: SYNCED),
            control_mode_adapter=mode_adapter or _ModeAdapter(),
        )
    )


def _activate(client, mode_key=MODE_KEY):
    return client.post(
        "/commands/actions/start",
        json={
            "request_id": "activate-1",
            "command_id": CommandId.MISSION_ACTIVATE.value,
            "parameters": {"mode_key": mode_key},
        },
    ).json()


def _mission(catalog_id):
    return _Mission(catalog_id=catalog_id, mode_keys=tuple(OPTI_TRACK_MISSIONS[catalog_id]))


def _on_ground(**kwargs):
    return _Vehicle(_ready_vision(), armed=False, in_air=False, **kwargs)


GROUND_START_REJECTION = (
    "mission activation requires the vehicle to be armed; "
    "mission activation requires the vehicle to be in flight"
)


def test_opti_track_preflight_uses_external_vision_instead_of_gps_and_overviews():
    mission = _client().get("/mission/status").json()

    items = mission["preflight"]["items"]
    assert [item["key"] for item in items] == OPTI_TRACK_PREFLIGHT_KEYS
    assert mission["preflight"]["ready"] is True
    assert all(item["passed"] for item in items if item["hard_gate"])
    # Stored overviews do not exist indoors and must not block activation.
    assert mission["latest"]["overview_rejections"] == []


def test_real_profile_keeps_its_gps_and_overview_preflight():
    mission = _client(profile="real").get("/mission/status").json()

    keys = [item["key"] for item in mission["preflight"]["items"]]
    for key in ("gps", "position", "estimator", "perception", "powerline", "pylons", "start_geometry", "payload"):
        assert key in keys
    assert "pose_relay" not in keys
    assert mission["preflight"]["ready"] is False
    assert mission["latest"]["overview_rejections"]


def test_opti_track_mission_activation_records_the_flight_basics_topic_set():
    rosbag = _FakeRosbagAdapter()
    modes = _ModeAdapter()

    response = _activate(_client(rosbag=rosbag, mode_adapter=modes))

    assert response["accepted"] is True, response
    assert modes.requests == [("mission", MODE_KEY, 30)]
    assert len(rosbag.started) == 1
    # No runtime ROS node here, so no pose-relay input topic was discovered.
    assert rosbag.started[0]["topics"] == list(OPTI_TRACK_RECORDING_TOPICS)
    assert not any(topic.startswith(("/perception", "/payload", "/sensor")) for topic in rosbag.started[0]["topics"])


@pytest.mark.parametrize(
    ("vision", "failed_label"),
    [
        (_ready_vision(ev_hgt_fused=False), "PX4 external-vision fusion: position fused, height not fused, yaw fused"),
        (_ready_vision(relay_level="warn", relay_message="input gap"), "OptiTrack pose relay healthy: level warn"),
        (_ready_vision(relay_freshness="stale"), "OptiTrack pose relay healthy: level ok (stale)"),
        (_ready_vision(origin_valid=False), "EKF global origin set: origin not set"),
    ],
)
def test_opti_track_mission_activation_requires_healthy_external_vision(vision, failed_label):
    rosbag = _FakeRosbagAdapter()
    modes = _ModeAdapter()

    response = _activate(_client(vision=vision, rosbag=rosbag, mode_adapter=modes))

    assert response["accepted"] is False
    assert response["message"].startswith("inspection preflight failed: ")
    assert failed_label in response["message"]
    assert modes.requests == []
    assert rosbag.started == []


def test_perception_loss_is_not_classified_for_a_profile_without_perception():
    active = MissionModeRegistryEntry(mode_key=MODE_KEY, display_name="OptiTrack Flight", active=True)
    arguments = dict(
        vehicle=VehicleDomainState(failsafe=False, in_air=True),
        transition_state=SimpleNamespace(owner="mission", active_setpoint_owner="mission_executor", latest={}),
        failed_mode=None,
        active_mode=active,
        perception_state=SimpleNamespace(source_availability="unavailable"),
        payload_state=SimpleNamespace(source_availability="unavailable"),
        mission_state=MissionDomainState(mission_state="active", modes=[active]),
        recent_context=[],
    )

    assert classify_operational_safety(**arguments).status == "perception_loss"
    assert classify_operational_safety(**arguments, perception_expected=False).status == "normal"


@pytest.mark.parametrize(("catalog_id", "mode_key"), [("opti-track-cycle", "ot_cycle_takeoff"), ("opti-track-lemniscate", "otl_takeoff")])
def test_opti_track_starts_a_mode_that_may_arm_from_a_disarmed_landed_aircraft(catalog_id, mode_key):
    modes = _ModeAdapter()
    catalog = _Catalog(OPTI_TRACK_MISSIONS)
    client = _client(vehicle=_on_ground(), mission=_mission(catalog_id), catalog=catalog, mode_adapter=modes)

    mission = client.get("/mission/status").json()
    response = _activate(client, mode_key)

    items = {item["key"]: item for item in mission["preflight"]["items"]}
    assert "air_state" not in items
    assert items["ready_to_arm"]["label"] == "Aircraft ready to arm"
    assert items["ready_to_arm"]["passed"] is True
    assert items["ready_to_arm"]["detail"] == "disarmed and landed; PX4 arming checks passed"
    assert mission["preflight"]["ready"] is True
    assert response["accepted"] is True, response
    assert modes.requests == [("mission", mode_key, 30)]
    # The installed specification was read once for the selected entry.
    assert catalog.reads == 1


@pytest.mark.parametrize(("catalog_id", "mode_key"), [("opti-track-maneuvers", "ot_maneuvers"), ("opti-track-hover", "ot_hover")])
def test_opti_track_still_starts_other_modes_airborne_only(catalog_id, mode_key):
    modes = _ModeAdapter()
    client = _client(vehicle=_on_ground(), mission=_mission(catalog_id), mode_adapter=modes)

    items = {item["key"]: item for item in client.get("/mission/status").json()["preflight"]["items"]}
    response = _activate(client, mode_key)

    assert items["air_state"]["passed"] is False
    assert "ready_to_arm" not in items
    assert response["accepted"] is False
    assert response["message"] == GROUND_START_REJECTION
    assert modes.requests == []


def test_ground_start_with_failing_px4_arming_checks_is_refused_by_the_preflight():
    modes = _ModeAdapter()
    client = _client(
        vehicle=_on_ground(arming_checks_passed=False),
        mission=_mission("opti-track-cycle"),
        mode_adapter=modes,
    )

    response = _activate(client, "ot_cycle_takeoff")

    assert response["accepted"] is False
    assert response["message"].startswith("inspection preflight failed: ")
    assert "Aircraft ready to arm: PX4 arming checks have not passed" in response["message"]
    assert modes.requests == []


def test_ground_start_is_still_refused_on_a_cable():
    on_cable = SimpleNamespace(
        state=lambda: DroneAwarenessState(
            freshness="fresh", source_availability="available", degraded_reason=None, drone_location="on_cable", on_cable=True
        ),
        subscribe=lambda node: None,
    )
    client = _client(vehicle=_on_ground(), mission=_mission("opti-track-cycle"), awareness=on_cable)

    response = _activate(client, "ot_cycle_takeoff")

    assert response["accepted"] is False
    assert "mission activation is disabled while the vehicle is on cable" in response["message"]


def test_an_airborne_aircraft_starts_a_mode_that_may_arm_as_before():
    modes = _ModeAdapter()
    client = _client(mission=_mission("opti-track-cycle"), mode_adapter=modes)

    items = {item["key"]: item for item in client.get("/mission/status").json()["preflight"]["items"]}
    response = _activate(client, "ot_cycle_takeoff")

    assert items["air_state"]["passed"] is True
    assert "ready_to_arm" not in items
    assert response["accepted"] is True, response


@pytest.mark.parametrize("profile", ["real", "sim", "hil"])
def test_field_and_simulation_profiles_start_missions_airborne_only(profile):
    # Canonical cable modes carry the flag too; outside opti_track it is ignored.
    catalog = _Catalog({"inspection-production": {"cable_charging": True}})
    modes = _ModeAdapter()
    client = _client(
        profile=profile,
        vehicle=_on_ground(),
        mission=_Mission(catalog_id="inspection-production", mode_keys=("cable_charging",)),
        catalog=catalog,
        mode_adapter=modes,
    )

    response = _activate(client, "cable_charging")

    assert response["accepted"] is False
    assert response["message"].startswith(GROUND_START_REJECTION)
    assert modes.requests == []
    assert catalog.reads == 0
