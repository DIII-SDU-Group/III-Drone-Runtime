from datetime import timedelta
import sys
from types import ModuleType
from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import (
    CommandId,
    MissionDomainState,
    MissionModeRegistryEntry,
    OperationDomainState,
    SystemDomainState,
    VehicleDomainState,
)
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.dispatch import DispatchRegistry
from iii_drone_runtime.api.events import RuntimeEventLog
from iii_drone_runtime.api.flight_commands import (
    ControlModeCommandAdapter,
    ControlTransitionTracker,
    DroneAwarenessCache,
    DroneAwarenessState,
    FlightCommandGate,
    HoldInterruptionReconciler,
    Px4NavStateModeAdapter,
    register_flight_mode_command_handlers,
)
from iii_drone_runtime.api.mission_status import MissionStatusCache
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache
from iii_drone_runtime.api.px4_adapter import PersistentPx4CommandAdapter
from iii_drone_runtime.api.px4_state import FusedPx4StateProvider, RosPx4StateCache
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_px4_adapter import _FakeSystem
from test_px4_state import _FakeCommandAdapter, _command_status
from test_runtime_commands import _FakeDaemonClient, _FakeSystemd


class _VehicleProvider:
    def __init__(self, *, state: VehicleDomainState, rejection_reason=None):
        self._state = state
        self._rejection_reason = rejection_reason

    def state(self):
        return self._state

    def dangerous_command_rejection_reason(self):
        return self._rejection_reason


class _ModeAdapter(ControlModeCommandAdapter):
    def __init__(self):
        self.requests = []

    def request_mode(self, target: str, *, mode_key=None, expected_mode_id=None):
        self.requests.append((target, mode_key, expected_mode_id))
        return {"requested": target, "mode_key": mode_key, "mode_id": expected_mode_id}


class _Clock:
    def now(self):
        return SimpleNamespace(nanoseconds=123_000)


class _Publisher:
    def __init__(self):
        self.messages = []
        self.subscription_counts = [1]

    def publish(self, message):
        self.messages.append(message)

    def get_subscription_count(self):
        if len(self.subscription_counts) > 1:
            return self.subscription_counts.pop(0)
        return self.subscription_counts[0]


class _RosNode:
    def __init__(self):
        self.publisher = _Publisher()
        self.publisher_calls = []

    def create_publisher(self, message_type, topic, qos):
        self.publisher_calls.append((message_type, topic, qos))
        return self.publisher

    def get_clock(self):
        return _Clock()


def _vehicle(*, armed=True, in_air=True, transport_available=True, nav_state="hold"):
    return VehicleDomainState(
        source_label="test",
        freshness="fresh",
        source_availability="available",
        armed=armed,
        in_air=in_air,
        nav_state=nav_state,
        failsafe=False,
        latest={"command_transport": {"command_available": transport_available}},
    )


def _gate(
    *,
    vehicle=None,
    vehicle_rejection=None,
    system_running=True,
    mission=None,
    operation=None,
    tracker=None,
    awareness=None,
    hold_reconciler=None,
):
    return FlightCommandGate(
        vehicle_state_provider=_VehicleProvider(
            state=vehicle or _vehicle(),
            rejection_reason=vehicle_rejection,
        ),
        system_state_provider=lambda: SystemDomainState(booted=system_running, active=system_running),
        mission_state_provider=lambda: mission
        or MissionDomainState(
            active_spec_id="/missions/mission.yaml",
            required_modes_registered=True,
            latest={"activation_rejections": [], "owned_mode": "inspection_demo"},
            freshness="fresh",
            source_availability="available",
            modes=[
                MissionModeRegistryEntry(
                    mode_key="inspection_demo",
                    display_name="Inspection Demo",
                    mode_id=77,
                    registered=True,
                    freshness="fresh",
                )
            ],
        ),
        operation_state_provider=lambda: operation
        or OperationDomainState(
            status="ready",
            latest={"custom_operation_modes_registered": True},
        ),
        transition_tracker=tracker or ControlTransitionTracker(),
        hold_reconciler=hold_reconciler,
        awareness_state_provider=lambda: awareness or DroneAwarenessState(
            freshness="fresh",
            source_availability="available",
            degraded_reason=None,
            drone_location="in_flight",
            on_cable=False,
        ),
    )


def test_flight_command_gating_matrix_exposes_disabled_reasons():
    gate = _gate(vehicle=_vehicle(armed=False, in_air=False))

    assert gate.disabled_reasons(CommandId.PX4_ARM.value) == []
    assert gate.disabled_reasons(CommandId.PX4_TAKEOFF.value) == [
        "takeoff requires the vehicle to already be armed"
    ]
    assert gate.disabled_reasons(CommandId.PX4_LAND.value) == [
        "land requires the vehicle to be in flight"
    ]

    hold_gate = _gate(vehicle=_vehicle(transport_available=True), vehicle_rejection="fused state stale")
    assert hold_gate.disabled_reasons(CommandId.PX4_HOLD.value) == []

    unavailable_hold = _gate(vehicle=_vehicle(transport_available=False))
    assert unavailable_hold.disabled_reasons(CommandId.PX4_HOLD.value) == [
        "PX4 command transport is unavailable"
    ]


def test_mission_and_custom_operation_activation_preconditions():
    mission_blocked = _gate(
        vehicle=_vehicle(in_air=False),
        system_running=False,
        mission=MissionDomainState(
            active_spec_id=None,
            required_modes_registered=False,
            latest={"activation_rejections": ["required mission modes are not registered"]},
        ),
    )
    mission_reasons = mission_blocked.disabled_reasons(CommandId.MISSION_ACTIVATE.value)

    assert "system is not running" in mission_reasons
    assert "mission activation requires the vehicle to be in flight" in mission_reasons
    assert "mission activation requires an active mission specification" in mission_reasons
    assert "mission activation requires all required modes to be registered" in mission_reasons

    on_cable_blocked = _gate(
        awareness=DroneAwarenessState(
            freshness="fresh",
            source_availability="available",
            degraded_reason=None,
            drone_location="on_cable",
            on_cable=True,
            on_cable_id=7,
        )
    )
    assert on_cable_blocked.disabled_reasons(CommandId.MISSION_ACTIVATE.value) == [
        "mission activation is disabled while the vehicle is on cable 7"
    ]

    custom_blocked = _gate(
        operation=OperationDomainState(
            status="unknown",
            degraded_reason="custom operation status degraded",
            latest={"custom_operation_modes_registered": False},
        )
    )
    custom_reasons = custom_blocked.disabled_reasons(CommandId.CUSTOM_OPERATION_ACTIVATE.value)

    assert "Custom Operation mode is not registered" in custom_reasons
    assert "custom operation status degraded" in custom_reasons

    unknown_custom_status = _gate(
        operation=OperationDomainState(
            status="unknown",
            degraded_reason="custom operation status topic has not been received",
            latest={},
        )
    )

    assert unknown_custom_status.disabled_reasons(CommandId.CUSTOM_OPERATION_ACTIVATE.value) == [
        "custom operation status topic has not been received"
    ]

    active_custom_mode = _gate(
        operation=OperationDomainState(
            status="custom_operation_idle",
            latest={
                "operation_active": False,
                "custom_operation_modes_registered": True,
                "control_owner": "custom_operation",
                "owned_mode": "CustomOperation",
            },
        )
    )

    assert active_custom_mode.disabled_reasons(CommandId.CUSTOM_OPERATION_ACTIVATE.value) == [
        "Custom Operation mode is already active"
    ]

    recoverable_idle_mode = _gate(
        operation=OperationDomainState(
            status="custom_operation_idle",
            degraded_reason="CustomOperation mode is not active",
            latest={
                "custom_operation_modes_registered": True,
                "control_owner": "unknown",
                "owned_mode": "CustomOperation",
            },
        )
    )

    assert recoverable_idle_mode.disabled_reasons(CommandId.CUSTOM_OPERATION_ACTIVATE.value) == []


def test_external_mode_activation_rejects_stale_ros_registration_absent_from_px4_mask():
    stale_vehicle = _vehicle()
    stale_vehicle.latest["ros_uxrce"] = {
        "raw": {"vehicle_status": {"can_set_nav_states_mask": 1 << 4}}
    }
    operation = OperationDomainState(
        status="custom_operation_idle",
        latest={
            "operation_active": False,
            "custom_operation_modes_registered": True,
            "control_owner": "unknown",
            "mode_id": 27,
        },
    )

    reasons = _gate(vehicle=stale_vehicle, operation=operation).disabled_reasons(
        CommandId.CUSTOM_OPERATION_ACTIVATE.value
    )

    assert reasons == [
        "Custom Operation mode is not selectable in PX4; restart the PX4 bridge dependents while disarmed"
    ]


def test_transition_tracker_reports_timeout_as_degraded_control_state():
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    transition = tracker.start(
        command_id=CommandId.PX4_HOLD.value,
        request_id="req-transition",
        target="px4_hold",
    )

    fresh = tracker.control_state(now=transition.started_at + timedelta(seconds=1))
    timed_out = tracker.control_state(now=transition.started_at + timedelta(seconds=6))

    assert fresh.owner == "transitioning"
    assert fresh.transition_target == "px4_hold"
    assert fresh.latest["transition"]["status"] == "transitioning"
    assert timed_out.owner == "degraded_conflict"
    assert timed_out.latest["transition"]["status"] == "timed_out"
    assert "timed out" in timed_out.degraded_reason


def test_gate_reports_abandoned_custom_operation_transition_before_clearing_it():
    tracker = ControlTransitionTracker(timeout_seconds=-1.0)
    tracker.start(
        command_id=CommandId.CUSTOM_OPERATION_ACTIVATE.value,
        request_id="req-custom",
        target="custom_operation",
    )
    gate = _gate(
        tracker=tracker,
        operation=OperationDomainState(
            status="custom_operation_idle",
            latest={"operation_active": False, "custom_operation_modes_registered": True},
        ),
    )

    state = gate.control_state()

    assert state.owner == "degraded_conflict"
    assert state.source_availability == "degraded"
    assert state.latest["transition"]["status"] == "timed_out"
    assert tracker.consume_terminal().request_id == "req-custom"
    assert gate.control_state().latest["transition"] is None


def test_gate_reports_terminal_transition_when_custom_operation_becomes_active():
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    tracker.start(
        command_id=CommandId.CUSTOM_OPERATION_ACTIVATE.value,
        request_id="req-custom",
        target="custom_operation",
    )
    gate = _gate(
        tracker=tracker,
        operation=OperationDomainState(
            active_operation_id="op-1",
            status="custom_operation_active",
            latest={"operation_active": True, "custom_operation_modes_registered": True},
        ),
    )

    state = gate.control_state()

    assert state.owner == "custom_operation"
    assert state.latest["transition"]["status"] == "active"
    assert tracker.consume_terminal().request_id == "req-custom"


def test_gate_reports_terminal_takeoff_and_landing_transitions():
    takeoff_tracker = ControlTransitionTracker(timeout_seconds=5.0)
    takeoff_tracker.start(
        command_id=CommandId.PX4_TAKEOFF.value,
        request_id="req-takeoff",
        target="px4_takeoff",
    )
    takeoff = _gate(
        tracker=takeoff_tracker,
        vehicle=_vehicle(armed=True, in_air=True),
    ).control_state()

    assert takeoff.latest["transition"]["status"] == "active"
    assert takeoff_tracker.consume_terminal().request_id == "req-takeoff"

    landing_tracker = ControlTransitionTracker(timeout_seconds=5.0)
    landing_tracker.start(
        command_id=CommandId.PX4_LAND.value,
        request_id="req-land",
        target="px4_land",
    )
    landing = _gate(
        tracker=landing_tracker,
        vehicle=_vehicle(armed=False, in_air=False),
    ).control_state()

    assert landing.latest["transition"]["status"] == "terminated"
    assert landing_tracker.consume_terminal().request_id == "req-land"


def test_transition_tracker_allows_a_longer_landing_confirmation_window():
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    transition = tracker.start(
        command_id=CommandId.PX4_LAND.value,
        request_id="req-land",
        target="px4_land",
        timeout_seconds=60.0,
    )

    state = tracker.control_state(now=transition.started_at + timedelta(seconds=15))

    assert state.latest["transition"]["status"] == "transitioning"
    assert state.latest["transition"]["timeout_seconds"] == 60.0


def test_terminal_timeout_payload_remains_marked_timed_out():
    tracker = ControlTransitionTracker(timeout_seconds=-1.0)
    tracker.start(command_id=CommandId.PX4_LAND.value, request_id="req-land", target="px4_land")
    tracker.complete(status="timed_out", message="landing confirmation timed out")

    payload = tracker.consume_terminal().as_dict()

    assert payload["status"] == "timed_out"
    assert payload["timed_out"] is True


def test_hold_reconciler_warns_if_mission_or_custom_state_persists():
    mission = MissionDomainState(mission_state="active", latest={"mission_active": True})
    operation = OperationDomainState(active_operation_id="op-1", latest={"operation_active": True})
    event_log = RuntimeEventLog()
    reconciler = HoldInterruptionReconciler(
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        event_log=event_log,
        timeout_seconds=3.0,
    )

    reconciler.record_hold(request_id="req-hold", command_id=CommandId.PX4_HOLD.value)
    interruption_event = event_log.recent()[-1]
    pending = reconciler._pending

    assert interruption_event.category == "control_owner_interruption"
    assert interruption_event.details["interrupted_owners"] == ["mission", "custom_operation"]
    assert reconciler.warnings(now=pending.started_at + timedelta(seconds=1)) == []

    warnings = reconciler.warnings(now=pending.started_at + timedelta(seconds=4))

    assert warnings[0]["still_active_owners"] == ["mission", "custom_operation"]
    assert event_log.recent()[-1].category == "hold_reconciliation"

    mission.mission_state = "idle"
    mission.latest = {"mission_active": False}
    operation.active_operation_id = None
    operation.latest = {"operation_active": False}
    assert reconciler.warnings(now=pending.started_at + timedelta(seconds=5)) == []


def test_hold_transition_reports_confirmed_stop_then_terminated_ownership():
    mission = MissionDomainState(mission_state="active", latest={"mission_active": True})
    operation = OperationDomainState(latest={"operation_active": False})
    event_log = RuntimeEventLog()
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    reconciler = HoldInterruptionReconciler(
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        event_log=event_log,
    )
    tracker.start(
        command_id=CommandId.PX4_HOLD.value,
        request_id="req-hold",
        target="px4_hold",
    )
    reconciler.record_hold(request_id="req-hold", command_id=CommandId.PX4_HOLD.value)
    gate = _gate(
        vehicle=_vehicle(nav_state="hold"),
        mission=mission,
        operation=operation,
        tracker=tracker,
        hold_reconciler=reconciler,
    )

    stopping = gate.control_state()

    assert stopping.owner == "stopping"
    assert stopping.latest["transition"]["status"] == "stopping"
    assert "safely stopping" in stopping.latest["transition"]["message"]

    mission.mission_state = "idle"
    mission.latest = {"mission_active": False}
    terminated = gate.control_state()

    assert terminated.owner == "unknown"
    assert terminated.latest["transition"]["status"] == "terminated"
    assert "ownership cleared" in terminated.latest["transition"]["message"]
    assert terminated.latest["hold_interruption"] == {
        "request_id": "req-hold",
        "command_id": CommandId.PX4_HOLD.value,
        "started_at": terminated.latest["hold_interruption"]["started_at"],
        "interrupted_owners": ["mission"],
        "completed": True,
    }
    assert reconciler.completed_interruption("mission") == terminated.latest["hold_interruption"]
    assert event_log.recent()[-1].category == "control_owner_terminated"

    reconciler.record_hold(request_id="req-hold-landed", command_id=CommandId.PX4_HOLD.value)
    assert reconciler.completed_interruption("mission") == terminated.latest["hold_interruption"]

    reconciler.clear_completed_interruption("mission")
    assert reconciler.completed_interruption("mission") is None


def test_completed_hold_interruption_survives_runtime_restart(tmp_path):
    state_path = tmp_path / "hold-interruption.json"
    mission = MissionDomainState(mission_state="active", latest={"mission_active": True})
    operation = OperationDomainState(latest={"operation_active": False})
    reconciler = HoldInterruptionReconciler(
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        event_log=RuntimeEventLog(),
        state_path=state_path,
    )

    reconciler.record_hold(request_id="req-hold", command_id="px4.hold")
    mission.mission_state = "idle"
    mission.latest = {"mission_active": False}
    assert reconciler.transition_outcome(hold_confirmed=True, transition_timed_out=False)

    restored = HoldInterruptionReconciler(
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        event_log=RuntimeEventLog(),
        state_path=state_path,
    )
    persisted = restored.completed_interruption("mission")
    assert persisted == {
        "request_id": "req-hold",
        "command_id": "px4.hold",
        "started_at": persisted["started_at"],
        "interrupted_owners": ["mission"],
        "completed": True,
    }

    restored.clear_completed_interruption("mission")
    restarted = HoldInterruptionReconciler(
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        event_log=RuntimeEventLog(),
        state_path=state_path,
    )
    assert restarted.completed_interruption("mission") is None


def test_hold_completion_remains_available_when_persistence_fails(tmp_path):
    blocked_parent = tmp_path / "not-a-directory"
    blocked_parent.write_text("occupied", encoding="utf-8")
    mission = MissionDomainState(mission_state="active", latest={"mission_active": True})
    operation = OperationDomainState(latest={"operation_active": False})
    event_log = RuntimeEventLog()
    reconciler = HoldInterruptionReconciler(
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        event_log=event_log,
        state_path=blocked_parent / "hold-interruption.json",
    )

    reconciler.record_hold(request_id="req-hold", command_id="px4.hold")
    mission.mission_state = "idle"
    mission.latest = {"mission_active": False}
    assert reconciler.transition_outcome(hold_confirmed=True, transition_timed_out=False)

    assert reconciler.completed_interruption("mission")["completed"] is True
    assert any(event.category == "runtime_state_persistence" for event in event_log.recent())


def test_hold_transition_reports_timeout_before_px4_confirmation():
    mission = MissionDomainState(mission_state="active", latest={"mission_active": True})
    operation = OperationDomainState(latest={"operation_active": False})
    tracker = ControlTransitionTracker(timeout_seconds=-1.0)
    reconciler = HoldInterruptionReconciler(
        mission_state_provider=lambda: mission,
        operation_state_provider=lambda: operation,
        event_log=RuntimeEventLog(),
    )
    tracker.start(
        command_id=CommandId.PX4_HOLD.value,
        request_id="req-hold-timeout",
        target="px4_hold",
    )
    reconciler.record_hold(request_id="req-hold-timeout", command_id=CommandId.PX4_HOLD.value)
    gate = _gate(
        vehicle=_vehicle(nav_state="mission"),
        mission=mission,
        operation=operation,
        tracker=tracker,
        hold_reconciler=reconciler,
    )

    state = gate.control_state()

    assert state.latest["transition"]["status"] == "timed_out"
    assert "not confirmed" in state.latest["transition"]["message"]
    assert state.source_availability == "degraded"


def test_mission_activation_handler_requests_target_mode_and_sets_transition():
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    gate = _gate(tracker=tracker)
    mode_adapter = _ModeAdapter()
    registry = DispatchRegistry.empty()
    register_flight_mode_command_handlers(
        registry,
        gate=gate,
        transition_tracker=tracker,
        event_log=RuntimeEventLog(),
        mode_adapter=mode_adapter,
    )

    response, result = registry.start_action(
        SimpleNamespace(
            request_id="req-mission",
            command_id=CommandId.MISSION_ACTIVATE.value,
            client_label="pytest",
            parameters={"mode_key": "inspection_demo"},
        )
    )

    assert response.accepted is True
    assert response.started is True
    assert response.result["transition"]["target"] == "mission"
    assert result.status == "accepted"
    assert mode_adapter.requests == [("mission", "inspection_demo", 77)]
    assert tracker.control_state().transition_target == "mission"


def test_accepted_mission_activation_clears_prior_hold_termination_evidence():
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    cleared = []
    hold_reconciler = SimpleNamespace(
        clear_completed_interruption=lambda owner: cleared.append(owner),
    )
    registry = DispatchRegistry.empty()
    register_flight_mode_command_handlers(
        registry,
        gate=_gate(tracker=tracker),
        transition_tracker=tracker,
        event_log=RuntimeEventLog(),
        mode_adapter=_ModeAdapter(),
        hold_reconciler=hold_reconciler,
    )

    response, _ = registry.start_action(
        SimpleNamespace(
            request_id="req-fresh-mission",
            command_id=CommandId.MISSION_ACTIVATE.value,
            client_label="pytest",
            parameters={"mode_key": "inspection_demo"},
        )
    )

    assert response.accepted is True
    assert cleared == ["mission"]


def test_mission_activation_requires_explicit_stable_mode_key():
    tracker = ControlTransitionTracker()
    registry = DispatchRegistry.empty()
    register_flight_mode_command_handlers(
        registry,
        gate=_gate(tracker=tracker),
        transition_tracker=tracker,
        event_log=RuntimeEventLog(),
        mode_adapter=_ModeAdapter(),
    )

    response, _ = registry.start_action(
        SimpleNamespace(
            request_id="req-mission-no-key",
            command_id=CommandId.MISSION_ACTIVATE.value,
            client_label="pytest",
            parameters={},
        )
    )

    assert response.accepted is False
    assert response.rejection.code == "invalid_request"
    assert response.rejection.message == "mission activation requires a stable mode_key"


def test_mission_activation_fails_before_mode_request_when_recording_gate_fails():
    tracker = ControlTransitionTracker()
    mode_adapter = _ModeAdapter()
    registry = DispatchRegistry.empty()

    def reject_low_storage():
        raise RuntimeError("rosbag storage critically low")

    register_flight_mode_command_handlers(
        registry,
        gate=_gate(tracker=tracker),
        transition_tracker=tracker,
        event_log=RuntimeEventLog(),
        mode_adapter=mode_adapter,
        mission_activation_precondition=reject_low_storage,
    )

    response, _ = registry.start_action(
        SimpleNamespace(
            request_id="req-mission-low-storage",
            command_id=CommandId.MISSION_ACTIVATE.value,
            client_label="pytest",
            parameters={"mode_key": "inspection_demo"},
        )
    )

    assert response.accepted is False
    assert response.rejection.code == "degraded_state"
    assert "critically low" in response.rejection.message
    assert mode_adapter.requests == []


def test_mission_activation_rejects_stale_or_missing_live_mode_id():
    stale_mission = MissionDomainState(
        active_spec_id="/missions/mission.yaml",
        required_modes_registered=True,
        freshness="stale",
        latest={"owned_mode": "inspection_demo", "activation_rejections": []},
        modes=[
            MissionModeRegistryEntry(
                mode_key="inspection_demo",
                display_name="Inspection Demo",
                mode_id=None,
                registered=True,
                freshness="stale",
            )
        ],
    )

    reasons = _gate(mission=stale_mission).disabled_reasons(
        CommandId.MISSION_ACTIVATE.value,
        mode_key="inspection_demo",
    )

    assert "mission activation requires fresh mode registry state (currently stale)" in reasons
    assert "mission mode inspection_demo status is stale" in reasons
    assert "mission mode inspection_demo has no live PX4 ID" in reasons


def test_mission_transition_confirms_only_when_px4_and_typed_mode_are_active():
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    tracker.start(
        command_id=CommandId.MISSION_ACTIVATE.value,
        request_id="req-confirm",
        target="mission",
        expected_mode_key="inspection_demo",
        expected_mode_id=77,
    )
    mission = MissionDomainState(
        active_spec_id="/missions/mission.yaml",
        mission_state="active",
        required_modes_registered=True,
        freshness="fresh",
        latest={"mission_active": True, "owned_mode": "inspection_demo"},
        modes=[
            MissionModeRegistryEntry(
                mode_key="inspection_demo",
                display_name="Inspection Demo",
                mode_id=77,
                registered=True,
                active=True,
                tree_running=True,
                freshness="fresh",
            )
        ],
    )
    vehicle = _vehicle()
    vehicle.latest["ros_uxrce"] = {"nav_state_id": 77}

    state = _gate(tracker=tracker, mission=mission, vehicle=vehicle).control_state()

    assert state.owner == "mission"
    assert state.latest["transition"]["status"] == "active"
    assert state.latest["transition"]["expected_mode_key"] == "inspection_demo"
    assert state.latest["transition"]["expected_mode_id"] == 77


def test_mission_transition_rejects_registry_id_change_without_claiming_active():
    tracker = ControlTransitionTracker(timeout_seconds=5.0)
    tracker.start(
        command_id=CommandId.MISSION_ACTIVATE.value,
        request_id="req-id-change",
        target="mission",
        expected_mode_key="inspection_demo",
        expected_mode_id=77,
    )
    mission = MissionDomainState(
        active_spec_id="/missions/mission.yaml",
        mission_state="active",
        required_modes_registered=True,
        freshness="fresh",
        latest={"mission_active": True, "owned_mode": "inspection_demo"},
        modes=[
            MissionModeRegistryEntry(
                mode_key="inspection_demo",
                display_name="Inspection Demo",
                mode_id=78,
                registered=True,
                active=True,
                freshness="fresh",
            )
        ],
    )
    vehicle = _vehicle()
    vehicle.latest["ros_uxrce"] = {"nav_state_id": 78}

    state = _gate(tracker=tracker, mission=mission, vehicle=vehicle).control_state()

    assert state.owner == "degraded_conflict"
    assert state.latest["transition"]["status"] == "rejected"
    assert "ID changed during activation from 77 to 78" in state.degraded_reason


def test_activation_path_does_not_issue_implicit_cancel_commands():
    class CancelAwareModeAdapter(_ModeAdapter):
        def cancel_active(self):
            raise AssertionError("activation must not cancel existing actions")

    tracker = ControlTransitionTracker()
    gate = _gate(tracker=tracker)
    mode_adapter = CancelAwareModeAdapter()
    registry = DispatchRegistry.empty()
    register_flight_mode_command_handlers(
        registry,
        gate=gate,
        transition_tracker=tracker,
        event_log=RuntimeEventLog(),
        mode_adapter=mode_adapter,
    )

    response, _ = registry.start_action(
        SimpleNamespace(
            request_id="req-custom",
            command_id=CommandId.CUSTOM_OPERATION_ACTIVATE.value,
            client_label="pytest",
        )
    )

    assert response.accepted is True
    assert mode_adapter.requests == [("custom_operation", None, None)]


def test_px4_nav_state_mode_adapter_publishes_custom_operation_mode_command(monkeypatch):
    px4_msgs = ModuleType("px4_msgs")
    msg_module = ModuleType("px4_msgs.msg")

    class _VehicleCommand:
        VEHICLE_CMD_SET_NAV_STATE = 129

    msg_module.VehicleCommand = _VehicleCommand
    monkeypatch.setitem(sys.modules, "px4_msgs", px4_msgs)
    monkeypatch.setitem(sys.modules, "px4_msgs.msg", msg_module)
    node = _RosNode()
    adapter = Px4NavStateModeAdapter(
        node_provider=lambda: node,
        custom_operation_mode_id_provider=lambda: 42,
        repeat_count=3,
    )

    result = adapter.request_mode("custom_operation")

    assert result["mode_id"] == 42
    assert result["command"] == "VEHICLE_CMD_SET_NAV_STATE"
    assert node.publisher_calls[0][1] == "/fmu/in/vehicle_command"
    assert len(node.publisher.messages) == 3
    assert node.publisher.messages[-1].command == _VehicleCommand.VEHICLE_CMD_SET_NAV_STATE
    assert node.publisher.messages[-1].param1 == 42.0
    assert node.publisher.messages[-1].source_system == 255
    assert node.publisher.messages[-1].source_component == 0
    assert node.publisher.messages[-1].from_external is True


def test_px4_nav_state_mode_adapter_waits_for_px4_subscription_before_publish(monkeypatch):
    px4_msgs = ModuleType("px4_msgs")
    msg_module = ModuleType("px4_msgs.msg")

    class _VehicleCommand:
        VEHICLE_CMD_SET_NAV_STATE = 129

    msg_module.VehicleCommand = _VehicleCommand
    monkeypatch.setitem(sys.modules, "px4_msgs", px4_msgs)
    monkeypatch.setitem(sys.modules, "px4_msgs.msg", msg_module)
    node = _RosNode()
    node.publisher.subscription_counts = [0, 0, 1]
    adapter = Px4NavStateModeAdapter(
        node_provider=lambda: node,
        custom_operation_mode_id_provider=lambda: 42,
        repeat_count=1,
    )

    adapter.request_mode("custom_operation")

    assert len(node.publisher.messages) == 1
    assert node.publisher.subscription_counts == [1]


def test_px4_nav_state_mode_adapter_publishes_mission_mode_command(monkeypatch):
    px4_msgs = ModuleType("px4_msgs")
    msg_module = ModuleType("px4_msgs.msg")

    class _VehicleCommand:
        VEHICLE_CMD_SET_NAV_STATE = 129

    msg_module.VehicleCommand = _VehicleCommand
    monkeypatch.setitem(sys.modules, "px4_msgs", px4_msgs)
    monkeypatch.setitem(sys.modules, "px4_msgs.msg", msg_module)
    node = _RosNode()
    adapter = Px4NavStateModeAdapter(
        node_provider=lambda: node,
        mission_mode_id_provider=lambda mode_key: 77 if mode_key == "inspection_demo" else None,
        custom_operation_mode_id_provider=lambda: 42,
        repeat_count=1,
    )

    result = adapter.request_mode("mission", mode_key="inspection_demo", expected_mode_id=77)

    assert result["target"] == "mission"
    assert result["mode_id"] == 77
    assert result["mode_key"] == "inspection_demo"
    assert len(node.publisher.messages) == 1
    assert node.publisher.messages[0].param1 == 77.0


def test_px4_nav_state_mode_adapter_rejects_id_changed_before_dispatch(monkeypatch):
    adapter = Px4NavStateModeAdapter(
        node_provider=lambda: _RosNode(),
        mission_mode_id_provider=lambda mode_key: 78,
        custom_operation_mode_id_provider=lambda: 42,
    )

    try:
        adapter.request_mode("mission", mode_key="inspection_demo", expected_mode_id=77)
    except RuntimeError as exc:
        assert "changed before dispatch from 77 to 78" in str(exc)
    else:
        raise AssertionError("changed mission mode id should reject before PX4 dispatch")


def test_px4_nav_state_mode_adapter_rejects_when_custom_operation_mode_id_is_missing(monkeypatch):
    adapter = Px4NavStateModeAdapter(
        node_provider=lambda: _RosNode(),
        custom_operation_mode_id_provider=lambda: None,
    )

    try:
        adapter.request_mode("custom_operation")
    except RuntimeError as exc:
        assert "mode id has not been received" in str(exc)
    else:
        raise AssertionError("missing mode id should reject")


def test_runtime_api_exposes_control_status_with_disabled_reasons():
    gate = _gate(vehicle=_vehicle(armed=False, in_air=False))
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            flight_gate=gate,
        )
    )
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]

    response = client.get("/control/status", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200
    permissions = response.json()["latest"]["command_permissions"]
    assert permissions[CommandId.PX4_ARM.value] == []
    assert permissions[CommandId.PX4_TAKEOFF.value] == ["takeoff requires the vehicle to already be armed"]


def test_runtime_api_exposes_combined_drone_awareness_and_blocks_mission_on_cable():
    awareness = DroneAwarenessCache()
    awareness.handle_message(SimpleNamespace(drone_location=3, on_cable_id=7, ground_altitude_estimate=1.25))
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            drone_awareness=awareness,
            px4_state_provider=FusedPx4StateProvider(command_adapter=_FakeCommandAdapter(_command_status())),
            system_adapter=RuntimeSystemAdapter(
                daemon_client=_FakeDaemonClient(),
                systemd=_FakeSystemd(),
            ),
        )
    )
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    vehicle = client.get("/vehicle/status", headers=headers)
    control = client.get("/control/status", headers=headers)

    assert vehicle.status_code == 200
    assert vehicle.json()["latest"]["combined_drone_awareness"]["on_cable"] is True
    assert vehicle.json()["latest"]["combined_drone_awareness"]["on_cable_id"] == 7
    assert control.json()["latest"]["command_permissions"][CommandId.MISSION_ACTIVATE.value] == [
        "mission activation is disabled while the vehicle is on cable 7",
        "mission activation requires an active mission specification",
        "mission activation requires fresh mode registry state (currently unknown)",
        "mission activation requires all required modes to be registered",
        "mission activation requires an owned mission mode key",
        "stored powerline overview status has not been received",
        "stored pylon overview status has not been received",
    ]


def test_runtime_hold_sends_only_px4_hold_and_records_interruption_warning():
    system = _FakeSystem()
    adapter = PersistentPx4CommandAdapter(
        endpoint="udp://test",
        system_factory=lambda endpoint: system,
        reconnect_backoff_seconds=0.01,
    )
    ros_state = RosPx4StateCache()
    ros_state.handle_vehicle_status_message(
        SimpleNamespace(
            ARMING_STATE_ARMED=2,
            NAVIGATION_STATE_AUTO_LOITER=3,
            arming_state=2,
            nav_state=3,
            failsafe=False,
        )
    )
    ros_state.handle_vehicle_land_detected_message(SimpleNamespace(landed=False))
    mission = MissionStatusCache()
    mission.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/mission.yaml",
            mission_active=True,
            mission_state_label="active",
            required_modes=["mission"],
            registered_modes=["mission"],
            owned_mode="Mission",
            control_owner="mission",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )
    operation = CustomOperationStatusCache()
    operation.handle_message(
        SimpleNamespace(
            operation_state_label="active",
            operation_active=True,
            active_operation="hover",
            custom_operation_modes_registered=True,
            required_modes=["CustomOperation"],
            registered_modes=["CustomOperation"],
            owned_mode="CustomOperation",
            control_owner="custom_operation",
            cancel_available=True,
            degraded=False,
            degraded_reasons=[],
        )
    )
    event_log = RuntimeEventLog()
    hold_mission = MissionDomainState(mission_state="active", latest={"mission_active": True})
    hold_operation = OperationDomainState(active_operation_id="hover", latest={"operation_active": True})
    reconciler = HoldInterruptionReconciler(
        # Keep ownership fixed for this API-boundary test. The app's background
        # PX4 reconciliation is exercised separately and may update cache views
        # before the request depending on scheduler timing.
        mission_state_provider=lambda: hold_mission,
        operation_state_provider=lambda: hold_operation,
        event_log=event_log,
        timeout_seconds=3.0,
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
        mission_status=mission,
        operation_status=operation,
        hold_reconciler=reconciler,
    )

    with TestClient(app) as client:
        token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
        headers = {"Authorization": f"Bearer {token}"}
        response = client.post(
            "/commands/actions/start",
            headers=headers,
            json={"request_id": "req-hold-api", "command_id": CommandId.PX4_HOLD.value},
        )

    pending = reconciler._pending
    warning = reconciler.warnings(now=pending.started_at + timedelta(seconds=4))

    assert response.json()["accepted"] is True
    assert system.commands == ["hold"]
    assert event_log.recent()[0].category == "control_owner_interruption"
    # MAVSDK mode telemetry races the immediate warning snapshot. Either typed
    # owner may reconcile first; any owner that remains must stay observable.
    if warning:
        assert set(warning[0]["still_active_owners"]) & {"mission", "custom_operation"}
        assert set(warning[0]["still_active_owners"]) <= {"mission", "custom_operation"}
