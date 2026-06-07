from datetime import timedelta
import sys
from types import ModuleType
from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId, MissionDomainState, OperationDomainState, SystemDomainState, VehicleDomainState
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
from test_px4_adapter import _FakeSystem
from test_px4_state import _FakeCommandAdapter, _command_status


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

    def request_mode(self, target: str):
        self.requests.append(target)
        return {"requested": target}


class _Clock:
    def now(self):
        return SimpleNamespace(nanoseconds=123_000)


class _Publisher:
    def __init__(self):
        self.messages = []

    def publish(self, message):
        self.messages.append(message)


class _RosNode:
    def __init__(self):
        self.publisher = _Publisher()
        self.publisher_calls = []

    def create_publisher(self, message_type, topic, qos):
        self.publisher_calls.append((message_type, topic, qos))
        return self.publisher

    def get_clock(self):
        return _Clock()


def _vehicle(*, armed=True, in_air=True, transport_available=True):
    return VehicleDomainState(
        source_label="test",
        freshness="fresh",
        source_availability="available",
        armed=armed,
        in_air=in_air,
        nav_state="hold",
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
            latest={"activation_rejections": []},
        ),
        operation_state_provider=lambda: operation
        or OperationDomainState(
            status="ready",
            latest={"custom_operation_modes_registered": True},
        ),
        transition_tracker=tracker or ControlTransitionTracker(),
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


def test_gate_clears_abandoned_custom_operation_transition_without_degraded_control():
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

    assert state.owner == "unknown"
    assert state.source_availability == "available"
    assert state.degraded_reason is None
    assert state.latest["transition"] is None


def test_gate_clears_transition_when_custom_operation_becomes_active():
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

    assert state.owner == "unknown"
    assert state.latest["transition"] is None


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
        )
    )

    assert response.accepted is True
    assert response.started is True
    assert response.result["transition"]["target"] == "mission"
    assert result.status == "accepted"
    assert mode_adapter.requests == ["mission"]
    assert tracker.control_state().transition_target == "mission"


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
    assert mode_adapter.requests == ["custom_operation"]


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
    assert node.publisher.messages[-1].from_external is True


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
        mission_mode_id_provider=lambda: 77,
        custom_operation_mode_id_provider=lambda: 42,
        repeat_count=1,
    )

    result = adapter.request_mode("mission")

    assert result["target"] == "mission"
    assert result["mode_id"] == 77
    assert len(node.publisher.messages) == 1
    assert node.publisher.messages[0].param1 == 77.0


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
        "mission activation requires all required modes to be registered",
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
    reconciler = HoldInterruptionReconciler(
        mission_state_provider=mission.state,
        operation_state_provider=operation.state,
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
    assert warning[0]["still_active_owners"] == ["mission", "custom_operation"]
