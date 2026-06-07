from datetime import datetime, timedelta, timezone
import sys
from types import SimpleNamespace
import types

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache
from iii_drone_runtime.api.px4_adapter import PersistentPx4CommandAdapter, Px4CommandTransportStatus
from iii_drone_runtime.api.px4_state import FusedPx4StateProvider, RosPx4StateCache
from test_px4_adapter import _FakeSystem


class _FakeCommandAdapter:
    def __init__(self, status: Px4CommandTransportStatus):
        self._status = status

    def status(self):
        return self._status


def _command_status(**overrides):
    values = {
        "enabled": True,
        "endpoint": "udp://test",
        "connected": True,
        "source_availability": "available",
        "degraded_reason": None,
        "last_heartbeat_at": datetime.now(timezone.utc),
        "last_update_at": datetime.now(timezone.utc),
        "armed": True,
        "flight_mode": "HOLD",
        "nav_state": "hold",
        "in_air": True,
        "reconnect_attempts": 1,
        "last_error": None,
    }
    values.update(overrides)
    return Px4CommandTransportStatus(**values)


def _ros_cache(*, armed=True, in_air=True, nav_state=4, failsafe=False):
    cache = RosPx4StateCache()
    cache.handle_vehicle_status_message(
        SimpleNamespace(
            ARMING_STATE_ARMED=2,
            NAVIGATION_STATE_POSCTL=2,
            NAVIGATION_STATE_AUTO_MISSION=3,
            NAVIGATION_STATE_AUTO_LOITER=4,
            NAVIGATION_STATE_OFFBOARD=14,
            NAVIGATION_STATE_AUTO_TAKEOFF=17,
            NAVIGATION_STATE_AUTO_LAND=18,
            NAVIGATION_STATE_EXTERNAL1=23,
            NAVIGATION_STATE_EXTERNAL5=27,
            arming_state=2 if armed else 1,
            nav_state=nav_state,
            failsafe=failsafe,
        )
    )
    cache.handle_vehicle_land_detected_message(SimpleNamespace(landed=not in_air))
    return cache


def test_ros_px4_state_cache_subscribes_to_px4_vehicle_topics(monkeypatch):
    package = types.ModuleType("px4_msgs")
    msg_module = types.ModuleType("px4_msgs.msg")
    rclpy_module = types.ModuleType("rclpy")
    qos_module = types.ModuleType("rclpy.qos")
    msg_module.VehicleStatus = object
    msg_module.VehicleLandDetected = str
    qos_module.qos_profile_sensor_data = "sensor-data-qos"
    monkeypatch.setitem(sys.modules, "px4_msgs", package)
    monkeypatch.setitem(sys.modules, "px4_msgs.msg", msg_module)
    monkeypatch.setitem(sys.modules, "rclpy", rclpy_module)
    monkeypatch.setitem(sys.modules, "rclpy.qos", qos_module)
    calls = []

    class _Node:
        def create_subscription(self, message_type, topic, callback, queue_size):
            calls.append((message_type, topic, callback, queue_size))
            return topic

    subscriptions = RosPx4StateCache().subscribe(_Node())

    assert subscriptions == ["/fmu/out/vehicle_status_v1", "/fmu/out/vehicle_land_detected"]
    assert calls[0][0] is object
    assert calls[0][2].__name__ == "handle_vehicle_status_message"
    assert calls[1][0] is str
    assert calls[1][2].__name__ == "handle_vehicle_land_detected_message"
    assert calls[0][3] == "sensor-data-qos"
    assert calls[1][3] == "sensor-data-qos"


def test_fused_px4_state_prefers_ros_fields_and_exposes_source_diagnostics():
    provider = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(_command_status(flight_mode="HOLD", nav_state="hold")),
        ros_state=_ros_cache(nav_state=4),
    )

    state = provider.state()

    assert state.source_availability == "available"
    assert state.armed is True
    assert state.in_air is True
    assert state.nav_state == "hold"
    assert state.failsafe is False
    assert state.latest["command_transport"]["connected"] is True
    assert state.latest["ros_uxrce"]["available"] is True
    assert state.latest["disagreements"] == []


def test_disagreement_marks_fused_state_degraded_and_blocks_dangerous_commands():
    provider = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(_command_status(armed=True, in_air=True, nav_state="hold")),
        ros_state=_ros_cache(armed=False, in_air=True, nav_state=4),
    )

    state = provider.state()

    assert state.source_availability == "degraded"
    assert state.latest["dangerous_commands_allowed"] is False
    assert state.latest["disagreements"] == [{"field": "armed", "mavsdk": True, "ros_uxrce": False}]
    assert "disagree" in provider.dangerous_command_rejection_reason()


def test_missing_ros_source_does_not_degrade_when_mavsdk_state_is_complete():
    missing = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(_command_status()),
        ros_state=RosPx4StateCache(),
    )
    state = missing.state()

    assert state.freshness == "fresh"
    assert state.source_availability == "available"
    assert state.latest["ros_uxrce"]["available"] is False
    assert missing.dangerous_command_rejection_reason() is None


def test_missing_or_stale_ros_source_blocks_when_mavsdk_state_is_incomplete():
    missing = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(_command_status(armed=None)),
        ros_state=RosPx4StateCache(),
    )
    assert "has not been received" in missing.dangerous_command_rejection_reason()

    stale_cache = _ros_cache()
    stale_cache._last_vehicle_status_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    stale = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(_command_status(in_air=None)),
        ros_state=stale_cache,
    )
    assert "stale" in stale.dangerous_command_rejection_reason()


def test_registered_external_nav_state_uses_mode_label_before_fusion():
    provider = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(_command_status(flight_mode="UNKNOWN", nav_state="unknown")),
        ros_state=_ros_cache(nav_state=27),
        mode_label_provider=lambda nav_state_id: "custom_operation" if nav_state_id == 27 else None,
    )

    state = provider.state()

    assert state.freshness == "fresh"
    assert state.nav_state == "custom_operation"
    assert state.latest["ros_uxrce"]["raw"]["registered_nav_state_label"] == "custom_operation"
    assert state.latest["disagreements"] == []


def test_unregistered_external_nav_state_keeps_raw_nav_state_label():
    provider = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(_command_status(flight_mode="UNKNOWN", nav_state="unknown")),
        ros_state=_ros_cache(nav_state=27),
    )

    state = provider.state()

    assert state.nav_state == "nav_state_27"
    assert state.latest["disagreements"] == []


def test_runtime_api_labels_registered_custom_operation_external_mode():
    system = _FakeSystem()
    adapter = PersistentPx4CommandAdapter(
        endpoint="udp://test",
        system_factory=lambda endpoint: system,
        reconnect_backoff_seconds=0.01,
    )
    ros_state = _ros_cache(nav_state=27)
    operation_status = CustomOperationStatusCache()
    operation_status.handle_message(
        SimpleNamespace(
            operation_active=False,
            active_operation="",
            operation_state_label="ready",
            custom_operation_modes_registered=True,
            required_modes=["custom_operation"],
            registered_modes=["custom_operation"],
            owned_mode="CustomOperation",
            mode_id=27,
            control_owner="custom_operation",
            cancel_available=False,
            degraded=False,
            degraded_reasons=[],
        )
    )
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        px4_adapter=adapter,
        px4_ros_state=ros_state,
        operation_status=operation_status,
    )

    with TestClient(app) as client:
        token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
        vehicle = client.get("/vehicle/status", headers={"Authorization": f"Bearer {token}"})

    assert vehicle.status_code == 200
    assert vehicle.json()["nav_state"] == "custom_operation"
    assert vehicle.json()["latest"]["ros_uxrce"]["nav_state"] == "custom_operation"


def test_runtime_api_exposes_fused_vehicle_status_and_rejects_dangerous_px4_command_on_disagreement():
    system = _FakeSystem()
    adapter = PersistentPx4CommandAdapter(
        endpoint="udp://test",
        system_factory=lambda endpoint: system,
        reconnect_backoff_seconds=0.01,
    )
    ros_state = _ros_cache(armed=False, in_air=False, nav_state=4)
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        px4_adapter=adapter,
        px4_ros_state=ros_state,
    )

    with TestClient(app) as client:
        token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
        headers = {"Authorization": f"Bearer {token}"}
        vehicle = client.get("/vehicle/status", headers=headers)
        rejected = client.post(
            "/commands/actions/start",
            headers=headers,
            json={"request_id": "px4-fused-1", "command_id": CommandId.PX4_ARM.value},
        )
        hold = client.post(
            "/commands/actions/start",
            headers=headers,
            json={"request_id": "px4-fused-2", "command_id": CommandId.PX4_HOLD.value},
        )

    assert vehicle.status_code == 200
    assert vehicle.json()["latest"]["ros_uxrce"]["available"] is True
    assert vehicle.json()["latest"]["command_transport"]["connected"] is True
    assert rejected.json()["accepted"] is False
    assert rejected.json()["rejection"]["code"] == "degraded_state"
    assert hold.json()["accepted"] is True
