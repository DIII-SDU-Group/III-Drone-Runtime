from datetime import datetime, timedelta, timezone
import sys
import types
from types import SimpleNamespace

from iii_drone_contracts import ExternalVisionState, TelemetryFieldState, VehicleDomainState
from iii_drone_runtime.api.external_vision import (
    ESTIMATOR_STATUS_FLAGS_TOPIC,
    POSE_RELAY_HEALTH_TOPIC,
    ExternalVisionMonitor,
    external_vision_preflight_items,
)
from iii_drone_runtime.api.px4_state import FusedPx4StateProvider
from iii_drone_runtime.ros_sampling import register_sampler
from test_px4_state import _FakeCommandAdapter, _command_status, _ros_cache


def _relay_health(*, level=b"\x00", message="", **values):
    defaults = {
        "input_rate_hz": "120.0",
        "output_rate_hz": "60.0",
        "last_input_age_ms": "6.5",
        "max_input_gap_ms": "18.0",
        "lab_stamp_age_ms": "9.0",
        "stale": "false",
        "origin_sent": "true",
        "rigid_body_id": "1",
    }
    defaults.update(values)
    return SimpleNamespace(
        level=level,
        name="opti_track_pose_relay",
        message=message,
        values=[SimpleNamespace(key=key, value=value) for key, value in defaults.items()],
    )


def _fusion(*, pos=True, hgt=True, yaw=True):
    return SimpleNamespace(cs_ev_pos=pos, cs_ev_hgt=hgt, cs_ev_yaw=yaw, cs_gps=False)


def _monitor(relay=None, fusion=None):
    monitor = ExternalVisionMonitor(enabled=True)
    if relay is not None:
        monitor.handle_relay_health_message(relay)
    if fusion is not None:
        monitor.handle_estimator_status_flags_message(fusion)
    return monitor


def _now():
    return datetime.now(timezone.utc)


def test_healthy_relay_and_fused_vision_with_an_origin_is_ready():
    state = _monitor(_relay_health(), _fusion()).state(origin_valid=True, origin_timestamp=_now())

    assert state.ready is True
    assert state.degraded_reason is None
    assert state.freshness == "fresh"
    assert state.relay_level == "ok"
    assert state.relay_stale is False
    assert state.input_rate_hz == 120.0
    assert state.output_rate_hz == 60.0
    assert state.last_input_age_ms == 6.5
    assert state.max_input_gap_ms == 18.0
    assert state.lab_stamp_age_ms == 9.0
    assert state.origin_sent is True
    assert state.rigid_body_id == "1"
    assert (state.ev_pos_fused, state.ev_hgt_fused, state.ev_yaw_fused) == (True, True, True)
    assert state.origin_valid is True
    assert state.origin_freshness == "fresh"


def test_nothing_received_is_unknown_and_not_ready():
    state = ExternalVisionMonitor(enabled=True).state(origin_valid=None, origin_timestamp=None)

    assert state.ready is False
    assert state.freshness == "unknown"
    assert state.relay_level == "unknown"
    assert state.degraded_reason == (
        "pose relay health has not been received; "
        "PX4 estimator status flags have not been received; "
        "EKF global origin is not set"
    )


def test_relay_warnings_stale_input_and_missing_fusion_are_reported():
    state = _monitor(
        _relay_health(level=b"\x01", message="input gap 250 ms", stale="true"),
        _fusion(yaw=False),
    ).state(origin_valid=False, origin_timestamp=_now())

    assert state.ready is False
    assert state.relay_level == "warn"
    assert state.relay_message == "input gap 250 ms"
    assert state.relay_stale is True
    assert state.degraded_reason == (
        "pose relay reports warn: input gap 250 ms; pose relay input is stale; "
        "PX4 is not fusing external-vision yaw; EKF global origin is not set"
    )


def test_old_reports_turn_stale():
    monitor = _monitor(_relay_health(), _fusion())
    later = _now() + timedelta(seconds=5)

    state = monitor.state(origin_valid=True, origin_timestamp=later - timedelta(seconds=10), now=later)

    assert state.ready is False
    assert state.relay_freshness == "stale"
    assert state.fusion_freshness == "stale"
    assert state.origin_freshness == "stale"
    assert state.freshness == "stale"
    assert "pose relay health is stale" in state.degraded_reason
    assert "PX4 estimator status flags are stale" in state.degraded_reason
    assert "EKF global origin state is stale" in state.degraded_reason


def test_relay_level_and_values_tolerate_integer_levels_and_malformed_values():
    state = _monitor(
        _relay_health(level=2, input_rate_hz="nan", stale="maybe", rigid_body_id=""),
        _fusion(),
    ).state(origin_valid=True, origin_timestamp=_now())

    assert state.relay_level == "error"
    assert state.input_rate_hz is None
    assert state.relay_stale is None
    assert state.rigid_body_id is None
    assert "pose relay input is stale" in state.degraded_reason


def test_monitor_subscribes_at_its_low_rate_only_when_enabled(monkeypatch):
    diagnostic_msgs = types.ModuleType("diagnostic_msgs")
    diagnostic_msg = types.ModuleType("diagnostic_msgs.msg")
    diagnostic_msg.DiagnosticStatus = type("DiagnosticStatus", (), {})
    px4_msgs = types.ModuleType("px4_msgs")
    px4_msg = types.ModuleType("px4_msgs.msg")
    px4_msg.EstimatorStatusFlags = type("EstimatorStatusFlags", (), {})
    rclpy = types.ModuleType("rclpy")
    rclpy_qos = types.ModuleType("rclpy.qos")
    rclpy_qos.qos_profile_sensor_data = "sensor-data-qos"
    for name, module in (
        ("diagnostic_msgs", diagnostic_msgs),
        ("diagnostic_msgs.msg", diagnostic_msg),
        ("px4_msgs", px4_msgs),
        ("px4_msgs.msg", px4_msg),
        ("rclpy", rclpy),
        ("rclpy.qos", rclpy_qos),
    ):
        monkeypatch.setitem(sys.modules, name, module)

    class _Sampler:
        def __init__(self):
            self.created = []

        def create_subscription(self, msg_type, topic, callback, qos, *, rate_hz=None):
            self.created.append((msg_type.__name__, topic, rate_hz))
            return topic

    class _Node:
        pass

    node = _Node()
    sampler = _Sampler()
    register_sampler(node, sampler)

    assert ExternalVisionMonitor(enabled=False).subscribe(node) == []
    assert ExternalVisionMonitor(enabled=True).subscribe(node) == [POSE_RELAY_HEALTH_TOPIC, ESTIMATOR_STATUS_FLAGS_TOPIC]
    assert sampler.created == [
        ("DiagnosticStatus", POSE_RELAY_HEALTH_TOPIC, 2.0),
        ("EstimatorStatusFlags", ESTIMATOR_STATUS_FLAGS_TOPIC, 2.0),
    ]


def test_local_position_records_the_ekf_global_origin():
    cache = _ros_cache()
    cache.handle_local_position_message(SimpleNamespace(xy_valid=True, z_valid=True, xy_global=True, z_global=True))
    assert cache.status().telemetry["global_origin_valid"] is True

    cache.handle_local_position_message(SimpleNamespace(xy_valid=True, z_valid=True, xy_global=True, z_global=False))
    telemetry = cache.status().telemetry

    assert telemetry["local_position_valid"] is True
    assert telemetry["global_origin_valid"] is False
    cache.handle_local_position_message(SimpleNamespace(xy_valid=True, z_valid=True))
    assert cache.status().telemetry["global_origin_valid"] is None


def test_fused_vehicle_state_carries_external_vision_only_for_its_profiles():
    ros = _ros_cache()
    ros.handle_local_position_message(SimpleNamespace(xy_valid=True, z_valid=True, xy_global=True, z_global=True))
    monitor = _monitor(_relay_health(), _fusion())
    adapter = _FakeCommandAdapter(_command_status())

    with_vision = FusedPx4StateProvider(command_adapter=adapter, ros_state=ros, external_vision=monitor).state()
    disabled = FusedPx4StateProvider(
        command_adapter=adapter, ros_state=ros, external_vision=ExternalVisionMonitor(enabled=False)
    ).state()
    without = FusedPx4StateProvider(command_adapter=adapter, ros_state=ros).state()

    assert with_vision.external_vision.ready is True
    assert with_vision.external_vision.origin_valid is True
    assert with_vision.external_vision.origin_freshness == "fresh"
    assert disabled.external_vision is None
    assert without.external_vision is None


def _evidence(value, freshness="fresh"):
    return TelemetryFieldState(value=value, source="test", freshness=freshness, source_availability="available")


def test_preflight_items_replace_gps_evidence_with_external_vision():
    ready = ExternalVisionState(
        ready=True,
        relay_level="ok",
        relay_freshness="fresh",
        relay_stale=False,
        input_rate_hz=120.0,
        last_input_age_ms=7.0,
        ev_pos_fused=True,
        ev_hgt_fused=True,
        ev_yaw_fused=True,
        fusion_freshness="fresh",
        origin_valid=True,
        origin_freshness="fresh",
    )
    vehicle = VehicleDomainState(
        telemetry_fields={"local_position_valid": _evidence(True)},
        external_vision=ready,
    )

    items = {item.key: item for item in external_vision_preflight_items(vehicle)}

    assert list(items) == ["local_position", "ekf_origin", "vision_fusion", "pose_relay"]
    assert all(item.passed and item.hard_gate for item in items.values())
    assert items["pose_relay"].detail == "level ok (fresh), input 120 Hz, last input 7 ms ago"

    unhealthy = VehicleDomainState(
        telemetry_fields={"local_position_valid": _evidence(True, freshness="stale")},
        external_vision=ready.model_copy(update={"ev_hgt_fused": False, "origin_valid": False, "relay_stale": True}),
    )
    failed = {item.key: item for item in external_vision_preflight_items(unhealthy)}

    assert not any(item.passed for item in failed.values())
    assert failed["vision_fusion"].detail == "position fused, height not fused, yaw fused (fresh)"
    assert failed["ekf_origin"].detail == "origin not set (fresh)"
    # Without any external-vision report every item fails closed.
    assert not any(item.passed for item in external_vision_preflight_items(VehicleDomainState()))
