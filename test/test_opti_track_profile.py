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
from iii_drone_runtime.api.rosbag import OPTI_TRACK_RECORDING_TOPICS
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_clock_sync import SYNCED
from test_rosbag import _FakeRosbagAdapter
from test_runtime_commands import _FakeDaemonClient, _FakeSystemd


MODE_KEY = "opti_track_flight"

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
    def __init__(self, vision):
        self.vehicle = VehicleDomainState(
            source_label="px4_fusion",
            freshness="fresh",
            source_availability="available",
            armed=True,
            in_air=True,
            nav_state="hold",
            failsafe=False,
            arming_checks_passed=True,
            battery_voltage_v=15.8,
            telemetry_fields={
                "armed": _evidence(True),
                "in_air": _evidence(True),
                "local_position_valid": _evidence(True),
                "arming_checks_passed": _evidence(True),
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
    def __init__(self):
        self.mission = MissionDomainState(
            source_label="mission_status",
            freshness="fresh",
            source_availability="available",
            active_spec_id="opti-track-flight",
            mission_state="ready",
            required_modes_registered=True,
            modes=[
                MissionModeRegistryEntry(
                    mode_key=MODE_KEY,
                    display_name="OptiTrack Flight",
                    mode_id=30,
                    registered=True,
                    freshness="fresh",
                )
            ],
            specification=MissionSpecificationIdentity(
                catalog_id="opti-track-flight",
                catalog_hash="sha256:" + "a" * 64,
                entry_hash="sha256:" + "b" * 64,
                catalog_ready=True,
                active_profile="opti_track",
            ),
            latest={"owned_mode": MODE_KEY, "activation_rejections": [], "mission_active": False},
        )

    def state(self):
        return self.mission.model_copy(deep=True)

    def set_system_running(self, running):
        del running

    def registered_mode_ids(self):
        return frozenset({30})

    def mode_id(self, mode_key):
        return 30 if mode_key == MODE_KEY else None

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


class _ModeAdapter:
    def __init__(self):
        self.requests = []

    def request_mode(self, target, *, mode_key=None, expected_mode_id=None):
        self.requests.append((target, mode_key, expected_mode_id))
        return {"requested": target}


def _client(profile="opti_track", vision=None, rosbag=None, mode_adapter=None):
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(runtime_id="iii-runtime", profile=profile),
            system_adapter=RuntimeSystemAdapter(daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()),
            supervision_health=_Supervision(),
            mission_status=_Mission(),
            px4_state_provider=_Vehicle(_ready_vision() if vision is None else vision),
            configuration_adapter=SimpleNamespace(
                manifest_parameter_values=lambda names: {name: 14.0 for name in names}
            ),
            rosbag_adapter=rosbag or _FakeRosbagAdapter(),
            clock_monitor=ChronyClockMonitor(enabled=True, tracking=lambda: SYNCED),
            control_mode_adapter=mode_adapter or _ModeAdapter(),
        )
    )


def _activate(client):
    return client.post(
        "/commands/actions/start",
        json={
            "request_id": "activate-1",
            "command_id": CommandId.MISSION_ACTIVATE.value,
            "parameters": {"mode_key": MODE_KEY},
        },
    ).json()


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
