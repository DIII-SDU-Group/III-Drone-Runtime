"""Named fault-acceptance matrix for the field inspection operator path."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from iii_drone_runtime.api.app import classify_operational_safety
from iii_drone_runtime.api.flight_commands import (
    ControlTransitionTracker,
    DroneAwarenessState,
    FlightCommandGate,
)
from iii_drone_runtime.api.mission_status import MissionStatusCache


def _vehicle(*, fresh=True, gps=True, armed=True, in_air=True, nav_state="hold"):
    state = SimpleNamespace(
        freshness="fresh" if fresh else "stale",
        armed=armed,
        in_air=in_air,
        nav_state=nav_state,
        flight_mode=nav_state,
        latest={"ros_uxrce": {"nav_state_id": 23, "gps_valid": gps}},
    )
    return SimpleNamespace(
        state=lambda: state,
        dangerous_command_rejection_reason=lambda: None if fresh else "vehicle state stale",
    )


def _mission(*, mode_id=23, fresh=True, activation_rejections=()):
    mode = SimpleNamespace(
        mode_key="inspection_demo",
        mode_id=mode_id,
        registered=True,
        active=False,
        freshness="fresh" if fresh else "stale",
    )
    return SimpleNamespace(
        active_spec_id="inspection-spec",
        freshness="fresh" if fresh else "stale",
        modes=[mode],
        required_modes_registered=True,
        ready=not activation_rejections,
        degraded=bool(activation_rejections),
        degraded_reasons=list(activation_rejections),
        mission_state="idle",
        latest={
            "mission_active": False,
            "owned_mode": "inspection_demo",
            "activation_rejections": list(activation_rejections),
            "overview_rejections": [],
        },
    )


def _operation():
    return SimpleNamespace(latest={"operation_active": False}, active_operation_id=None)


def _system():
    return SimpleNamespace(booted=True, active=True, freshness="fresh")


@pytest.mark.parametrize(
    ("fault", "vehicle", "mission", "awareness", "expected"),
    [
        ("stale_pose", _vehicle(fresh=False), _mission(), DroneAwarenessState(freshness="fresh", source_availability="available", drone_location="outside_corridor"), "stale"),
        ("bad_gps", _vehicle(gps=False), _mission(activation_rejections=("GPS position is invalid",)), DroneAwarenessState(freshness="fresh", source_availability="available", drone_location="outside_corridor"), "GPS"),
        ("perception_loss", _vehicle(), _mission(activation_rejections=("powerline perception unavailable",)), DroneAwarenessState(freshness="fresh", source_availability="available", drone_location="outside_corridor"), "perception"),
        ("ineligible_geometry", _vehicle(), _mission(activation_rejections=("inspection start position is not eligible",)), DroneAwarenessState(freshness="fresh", source_availability="available", drone_location="inside_corridor"), "eligible"),
    ],
)
def test_inspection_activation_faults_fail_closed(fault, vehicle, mission, awareness, expected):
    gate = FlightCommandGate(
        vehicle_state_provider=vehicle,
        system_state_provider=_system,
        mission_state_provider=lambda: mission,
        operation_state_provider=_operation,
        transition_tracker=ControlTransitionTracker(),
        awareness_state_provider=lambda: awareness,
    )
    reasons = gate.disabled_reasons("mission.activate", mode_key="inspection_demo")
    assert reasons, fault
    assert expected.lower() in "; ".join(reasons).lower()


def test_activation_timeout_never_claims_mission_ownership():
    tracker = ControlTransitionTracker(timeout_seconds=0.01)
    transition = tracker.start(
        command_id="mission.activate",
        request_id="timeout",
        target="mission",
        expected_mode_key="inspection_demo",
        expected_mode_id=23,
    )
    tracker._transition = transition.__class__(
        **{**transition.__dict__, "started_at": datetime.now(timezone.utc) - timedelta(seconds=1)}
    )
    state = tracker.control_state()
    assert state.owner == "degraded_conflict"
    assert state.latest["transition"]["timed_out"] is True


def test_runtime_reconnect_reconstructs_finished_phase_without_reacquiring_control():
    cache = MissionStatusCache()
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/mission/mission_specification.yaml",
            mission_active=False,
            mission_state_label="idle",
            required_modes=["inspection_demo", "reach_cable", "cable_charging", "leave_cable"],
            registered_modes=["inspection_demo", "reach_cable", "cable_charging", "leave_cable"],
            modes=[],
            owned_mode="",
            control_owner="px4_hold",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
            intents=[],
        )
    )
    state = cache.state()
    assert state.mission_state == "idle"
    assert state.latest["mission_active"] is False


def test_charger_failure_is_a_stop_required_fault_with_operator_context():
    safety = classify_operational_safety(
        vehicle=SimpleNamespace(failsafe=False, in_air=True),
        transition_state=SimpleNamespace(owner="mission", degraded_reason=None),
        failed_mode=None,
        active_mode=SimpleNamespace(mode_key="cable_charging"),
        perception_state=SimpleNamespace(source_availability="available"),
        payload_state=SimpleNamespace(source_availability="unavailable"),
        mission_state=SimpleNamespace(mission_state="active", modes=[]),
        recent_context=[{"category": "payload", "message": "charger topic stale"}],
    )

    assert safety.status == "charging_failure"
    assert safety.stop_required is True
    assert safety.source == "charger/gripper topics"
    assert safety.recent_context[0]["message"] == "charger topic stale"


def test_external_takeover_never_reacquires_finished_mission_control():
    finished_mode = SimpleNamespace(tree_finished=True, display_name="Inspection Demo")
    safety = classify_operational_safety(
        vehicle=SimpleNamespace(failsafe=False, in_air=True),
        transition_state=SimpleNamespace(owner="px4_hold", degraded_reason=None),
        failed_mode=None,
        active_mode=None,
        perception_state=SimpleNamespace(source_availability="available"),
        payload_state=SimpleNamespace(source_availability="available"),
        mission_state=SimpleNamespace(mission_state="idle", modes=[finished_mode]),
        recent_context=[{"category": "control_owner_terminated", "message": "Hold confirmed"}],
    )

    assert safety.status == "safe_recovery"
    assert safety.stop_required is True
    assert "Hold" in safety.operator_action


def test_recharge_phase_is_not_reported_as_ended_mission_ownership():
    finished_inspection = SimpleNamespace(tree_finished=True, display_name="Inspection Demo")
    active_reach_cable = SimpleNamespace(
        mode_key="reach_cable",
        display_name="Reach Cable",
        tree_finished=False,
    )
    safety = classify_operational_safety(
        vehicle=SimpleNamespace(failsafe=False, in_air=True),
        transition_state=SimpleNamespace(owner="mission", degraded_reason=None),
        failed_mode=None,
        active_mode=active_reach_cable,
        perception_state=SimpleNamespace(source_availability="available"),
        payload_state=SimpleNamespace(source_availability="available"),
        mission_state=SimpleNamespace(
            mission_state="idle",
            modes=[finished_inspection, active_reach_cable],
        ),
        recent_context=[],
    )

    assert safety.status == "normal"
    assert safety.stop_required is False


def test_mission_owned_phase_handoff_is_not_reported_as_ended_ownership():
    finished_inspection = SimpleNamespace(tree_finished=True, display_name="Inspection Demo")
    safety = classify_operational_safety(
        vehicle=SimpleNamespace(failsafe=False, in_air=True),
        transition_state=SimpleNamespace(
            owner="mission",
            active_setpoint_owner="mission_executor",
            degraded_reason=None,
        ),
        failed_mode=None,
        active_mode=None,
        perception_state=SimpleNamespace(source_availability="available"),
        payload_state=SimpleNamespace(source_availability="available"),
        mission_state=SimpleNamespace(mission_state="idle", modes=[finished_inspection]),
        recent_context=[],
    )

    assert safety.status == "normal"
    assert safety.stop_required is False


@pytest.mark.parametrize(
    ("in_air", "expected_status"),
    [(True, "safe_recovery"), (False, "normal")],
)
def test_explicit_hold_termination_is_not_reported_as_mission_failure(in_air, expected_status):
    failed_mode = SimpleNamespace(
        tree_finished=True,
        tree_success=False,
        display_name="Inspection Demo",
    )
    transition = {
        # A later landing command may replace the latest transition. The
        # completed Hold interruption remains the reason this tree stopped.
        "command_id": "px4.land",
        "request_id": "land-request",
        "target": "px4_land",
        "status": "active",
    }
    safety = classify_operational_safety(
        vehicle=SimpleNamespace(failsafe=False, in_air=in_air),
        transition_state=SimpleNamespace(
            owner="unknown",
            degraded_reason=None,
            latest={
                "transition": transition,
                "mission_hold_termination": {
                    "command_id": "px4.hold",
                    "request_id": "hold-request",
                    "interrupted_owners": ["mission"],
                    "completed": True,
                },
            },
        ),
        failed_mode=failed_mode,
        active_mode=None,
        perception_state=SimpleNamespace(source_availability="available"),
        payload_state=SimpleNamespace(source_availability="available"),
        mission_state=SimpleNamespace(mission_state="idle", modes=[failed_mode]),
        recent_context=[{"category": "control_owner_terminated", "message": "Hold confirmed"}],
    )

    assert safety.status == expected_status


def test_absent_completed_mission_hold_does_not_hide_mission_failure():
    failed_mode = SimpleNamespace(
        tree_finished=True,
        tree_success=False,
        display_name="Inspection Demo",
    )
    safety = classify_operational_safety(
        vehicle=SimpleNamespace(failsafe=False, in_air=False),
        transition_state=SimpleNamespace(
            owner="unknown",
            degraded_reason=None,
            latest={
                "transition": {
                    "command_id": "px4.hold",
                    "request_id": "newer-hold",
                    "target": "px4_hold",
                    "status": "terminated",
                },
                "hold_interruption": {
                    "command_id": "px4.hold",
                    "request_id": "older-hold",
                    "interrupted_owners": ["mission"],
                    "completed": True,
                },
                "mission_hold_termination": None,
            },
        ),
        failed_mode=failed_mode,
        active_mode=None,
        perception_state=SimpleNamespace(source_availability="available"),
        payload_state=SimpleNamespace(source_availability="available"),
        mission_state=SimpleNamespace(mission_state="idle", modes=[failed_mode]),
        recent_context=[],
    )

    assert safety.status == "mission_error"
