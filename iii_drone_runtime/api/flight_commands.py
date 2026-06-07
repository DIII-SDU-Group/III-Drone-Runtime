"""Flight command gating and control-owner transition tracking."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
import uuid

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRejection,
    CommandRequest,
    ControlDomainState,
    ErrorCode,
    EventSource,
    HandlerPermission,
    OperatorEvent,
)

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


PX4_TRANSITION_TARGETS = {
    CommandId.PX4_TAKEOFF.value: "px4_takeoff",
    CommandId.PX4_LAND.value: "px4_land",
    CommandId.PX4_HOLD.value: "px4_hold",
}


MODE_TRANSITION_TARGETS = {
    CommandId.MISSION_ACTIVATE.value: "mission",
    CommandId.CUSTOM_OPERATION_ACTIVATE.value: "custom_operation",
}


@dataclass(frozen=True)
class DroneAwarenessState:
    source_label: str = "combined_drone_awareness"
    freshness: str = "unknown"
    source_availability: str = "unavailable"
    degraded_reason: str | None = "combined drone awareness topic has not been received"
    source_timestamp: datetime | None = None
    drone_location: str = "unknown"
    on_cable: bool | None = None
    on_cable_id: int | None = None
    ground_altitude_estimate: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_label": self.source_label,
            "freshness": self.freshness,
            "source_availability": self.source_availability,
            "degraded_reason": self.degraded_reason,
            "source_timestamp": self.source_timestamp.isoformat() if self.source_timestamp else None,
            "drone_location": self.drone_location,
            "on_cable": self.on_cable,
            "on_cable_id": self.on_cable_id,
            "ground_altitude_estimate": self.ground_altitude_estimate,
        }


class DroneAwarenessCache:
    def __init__(
        self,
        *,
        topic: str = "/control/maneuver_controller/combined_drone_awareness",
        stale_after_seconds: float = 2.0,
    ):
        self.topic = topic
        self.stale_after = timedelta(seconds=stale_after_seconds)
        self._latest_message: Any | None = None
        self._last_update_at: datetime | None = None
        self._unavailable_reason = "combined drone awareness topic has not been received"

    def subscribe(self, node: Any):
        try:
            from iii_drone_interfaces.msg import CombinedDroneAwareness
        except Exception as exc:
            self._unavailable_reason = f"CombinedDroneAwareness message unavailable: {exc}"
            return None
        return node.create_subscription(CombinedDroneAwareness, self.topic, self.handle_message, 10)

    def handle_message(self, message: Any, *, now: datetime | None = None) -> None:
        self._latest_message = message
        self._last_update_at = now or _utc_now()
        self._unavailable_reason = None

    def state(self, *, now: datetime | None = None) -> DroneAwarenessState:
        message = self._latest_message
        updated_at = self._last_update_at
        if message is None or updated_at is None:
            return DroneAwarenessState(degraded_reason=self._unavailable_reason)
        current = now or _utc_now()
        stale = current - updated_at > self.stale_after
        location = _drone_location_label(getattr(message, "drone_location", None))
        on_cable = location == "on_cable"
        on_cable_id = _optional_int(getattr(message, "on_cable_id", None))
        if on_cable_id is not None and on_cable_id < 0:
            on_cable_id = None
        return DroneAwarenessState(
            freshness="stale" if stale else "fresh",
            source_availability="degraded" if stale else "available",
            degraded_reason="combined drone awareness topic is stale" if stale else None,
            source_timestamp=updated_at,
            drone_location=location,
            on_cable=on_cable,
            on_cable_id=on_cable_id,
            ground_altitude_estimate=_optional_float(getattr(message, "ground_altitude_estimate", None)),
        )


@dataclass(frozen=True)
class ControlTransition:
    command_id: str
    request_id: str
    target: str
    started_at: datetime
    timeout_seconds: float
    status: str = "transitioning"

    def timed_out(self, *, now: datetime | None = None) -> bool:
        current = now or _utc_now()
        return current >= self.started_at + timedelta(seconds=self.timeout_seconds)

    def as_dict(self, *, now: datetime | None = None) -> dict[str, Any]:
        timed_out = self.timed_out(now=now)
        return {
            "command_id": self.command_id,
            "request_id": self.request_id,
            "target": self.target,
            "started_at": self.started_at.isoformat(),
            "timeout_seconds": self.timeout_seconds,
            "status": "timed_out" if timed_out else self.status,
            "timed_out": timed_out,
        }


class ControlTransitionTracker:
    def __init__(self, *, timeout_seconds: float = 5.0):
        self.timeout_seconds = timeout_seconds
        self._transition: ControlTransition | None = None

    def start(self, *, command_id: str, request_id: str, target: str) -> ControlTransition:
        self._transition = ControlTransition(
            command_id=command_id,
            request_id=request_id,
            target=target,
            started_at=_utc_now(),
            timeout_seconds=self.timeout_seconds,
        )
        return self._transition

    def active(self, *, now: datetime | None = None) -> ControlTransition | None:
        transition = self._transition
        if transition is None:
            return None
        return transition

    def clear(self) -> None:
        self._transition = None

    def control_state(self, *, command_permissions: dict[str, list[str]] | None = None, now: datetime | None = None) -> ControlDomainState:
        transition = self.active(now=now)
        latest = {
            "command_permissions": command_permissions or {},
            "transition": transition.as_dict(now=now) if transition else None,
        }
        degraded_reason = None
        owner = "transitioning" if transition else "unknown"
        source_availability = "available"
        if transition and transition.timed_out(now=now):
            owner = "degraded_conflict"
            source_availability = "degraded"
            degraded_reason = f"control transition to {transition.target} timed out"
        return ControlDomainState(
            source_label="runtime_control_gate",
            freshness="fresh",
            source_availability=source_availability,
            degraded_reason=degraded_reason,
            latest=latest,
            owner=owner,
            transition_target=transition.target if transition else None,
        )


class FlightCommandGate:
    def __init__(
        self,
        *,
        vehicle_state_provider: Any,
        system_state_provider: Callable[[], Any],
        mission_state_provider: Callable[[], Any],
        operation_state_provider: Callable[[], Any],
        transition_tracker: ControlTransitionTracker,
        hold_reconciler: "HoldInterruptionReconciler | None" = None,
        awareness_state_provider: Callable[[], DroneAwarenessState] | None = None,
    ):
        self.vehicle_state_provider = vehicle_state_provider
        self.system_state_provider = system_state_provider
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider
        self.transition_tracker = transition_tracker
        self.hold_reconciler = hold_reconciler
        self.awareness_state_provider = awareness_state_provider

    def disabled_reasons(self, command_id: str) -> list[str]:
        if command_id == CommandId.PX4_HOLD.value:
            return self._hold_reasons()
        if command_id == CommandId.PX4_ARM.value:
            return self._base_flight_reasons()
        if command_id == CommandId.PX4_TAKEOFF.value:
            return self._base_flight_reasons() + self._takeoff_reasons()
        if command_id == CommandId.PX4_LAND.value:
            return self._base_flight_reasons() + self._land_reasons()
        if command_id == CommandId.MISSION_ACTIVATE.value:
            return self._base_flight_reasons() + self._mission_activation_reasons()
        if command_id == CommandId.CUSTOM_OPERATION_ACTIVATE.value:
            return self._base_flight_reasons() + self._custom_operation_activation_reasons()
        return [f"unsupported flight command: {command_id}"]

    def rejection_reason(self, command_id: str) -> str | None:
        reasons = self.disabled_reasons(command_id)
        if reasons:
            return "; ".join(reasons)
        return None

    def command_permissions(self) -> dict[str, list[str]]:
        command_ids = [
            CommandId.PX4_ARM.value,
            CommandId.PX4_TAKEOFF.value,
            CommandId.PX4_LAND.value,
            CommandId.PX4_HOLD.value,
            CommandId.MISSION_ACTIVATE.value,
            CommandId.CUSTOM_OPERATION_ACTIVATE.value,
        ]
        return {command_id: self.disabled_reasons(command_id) for command_id in command_ids}

    def control_state(self) -> ControlDomainState:
        transition = self.transition_tracker.active()
        if transition is not None and self._transition_reconciled(transition):
            self.transition_tracker.clear()
        state = self.transition_tracker.control_state(command_permissions=self.command_permissions())
        if self.hold_reconciler is not None:
            state.latest["hold_interruption_warnings"] = self.hold_reconciler.warnings()
        return state

    def _transition_reconciled(self, transition: ControlTransition) -> bool:
        if transition.target == "custom_operation":
            operation = self.operation_state_provider()
            operation_active = operation.latest.get("operation_active") is True or bool(operation.active_operation_id)
            return operation_active or transition.timed_out()
        if transition.target == "mission":
            mission = self.mission_state_provider()
            mission_active = mission.latest.get("mission_active") is True or mission.mission_state == "active"
            return mission_active or transition.timed_out()
        if transition.target == "px4_hold":
            vehicle = self.vehicle_state_provider.state()
            return _mode_label(vehicle.nav_state or vehicle.flight_mode) == "hold" or transition.timed_out()
        return transition.timed_out()

    def _base_flight_reasons(self) -> list[str]:
        reason = self.vehicle_state_provider.dangerous_command_rejection_reason()
        return [] if reason is None else [reason]

    def _hold_reasons(self) -> list[str]:
        vehicle = self.vehicle_state_provider.state()
        transport = vehicle.latest.get("command_transport", {})
        if transport.get("command_available") is True:
            return []
        return [transport.get("degraded_reason") or "PX4 command transport is unavailable"]

    def _takeoff_reasons(self) -> list[str]:
        vehicle = self.vehicle_state_provider.state()
        if vehicle.armed is not True:
            return ["takeoff requires the vehicle to already be armed"]
        return []

    def _land_reasons(self) -> list[str]:
        vehicle = self.vehicle_state_provider.state()
        if vehicle.in_air is not True:
            return ["land requires the vehicle to be in flight"]
        return []

    def _mission_activation_reasons(self) -> list[str]:
        reasons = self._system_running_reasons()
        vehicle = self.vehicle_state_provider.state()
        mission = self.mission_state_provider()
        if vehicle.in_air is not True:
            reasons.append("mission activation requires the vehicle to be in flight")
        awareness = self.awareness_state_provider() if self.awareness_state_provider else None
        if awareness is not None and awareness.on_cable is True:
            if awareness.on_cable_id is None:
                reasons.append("mission activation is disabled while the vehicle is on cable")
            else:
                reasons.append(f"mission activation is disabled while the vehicle is on cable {awareness.on_cable_id}")
        if not mission.active_spec_id:
            reasons.append("mission activation requires an active mission specification")
        if mission.required_modes_registered is not True:
            reasons.append("mission activation requires all required modes to be registered")
        reasons.extend(mission.latest.get("activation_rejections", []))
        return _deduplicate(reasons)

    def _custom_operation_activation_reasons(self) -> list[str]:
        reasons = self._system_running_reasons()
        vehicle = self.vehicle_state_provider.state()
        operation = self.operation_state_provider()
        if vehicle.in_air is not True:
            reasons.append("Custom Operation activation requires the vehicle to be in flight")
        if _custom_operation_mode_active(operation):
            reasons.append("Custom Operation mode is already active")
        if operation.latest.get("custom_operation_modes_registered") is False:
            reasons.append("Custom Operation mode is not registered")
        if operation.degraded_reason:
            reasons.append(operation.degraded_reason)
        return _deduplicate(reasons)

    def _system_running_reasons(self) -> list[str]:
        system = self.system_state_provider()
        if system.booted is True and system.active is True:
            return []
        return ["system is not running"]


class ControlModeCommandAdapter:
    """Runtime-side hook for requesting mission/custom control modes.

    Concrete ROS integration can provide this adapter; the default rejects with
    an explicit reason instead of pretending that a mode request was sent.
    """

    def request_mode(self, target: str) -> dict[str, Any]:
        raise RuntimeError(f"control mode request adapter unavailable for {target}")


class Px4NavStateModeAdapter(ControlModeCommandAdapter):
    def __init__(
        self,
        *,
        node_provider: Callable[[], Any | None],
        custom_operation_mode_id_provider: Callable[[], int | None],
        mission_mode_id_provider: Callable[[], int | None] | None = None,
        repeat_count: int = 5,
    ):
        self.node_provider = node_provider
        self.custom_operation_mode_id_provider = custom_operation_mode_id_provider
        self.mission_mode_id_provider = mission_mode_id_provider
        self.repeat_count = repeat_count
        self._publisher = None

    def request_mode(self, target: str) -> dict[str, Any]:
        if target not in {"custom_operation", "mission"}:
            raise RuntimeError(f"control mode request adapter unavailable for {target}")
        mode_id = self._mode_id(target)
        if mode_id is None:
            raise RuntimeError(f"{target.replace('_', ' ')} mode id has not been received")
        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is not available for PX4 nav-state request")
        try:
            from px4_msgs.msg import VehicleCommand
        except ImportError as exc:
            raise RuntimeError("px4_msgs is required for PX4 nav-state requests") from exc

        publisher = self._publisher
        if publisher is None:
            publisher = node.create_publisher(VehicleCommand, "/fmu/in/vehicle_command", _px4_input_qos())
            self._publisher = publisher
        for _ in range(max(1, self.repeat_count)):
            message = VehicleCommand()
            message.timestamp = int(node.get_clock().now().nanoseconds / 1000)
            message.command = VehicleCommand.VEHICLE_CMD_SET_NAV_STATE
            message.param1 = float(mode_id)
            message.target_system = 1
            message.target_component = 1
            message.source_system = 1
            message.source_component = 1
            message.from_external = True
            publisher.publish(message)
        return {
            "transport": "px4_vehicle_command",
            "command": "VEHICLE_CMD_SET_NAV_STATE",
            "target": target,
            "mode_id": mode_id,
            "repeat_count": max(1, self.repeat_count),
        }

    def _mode_id(self, target: str) -> int | None:
        if target == "custom_operation":
            return self.custom_operation_mode_id_provider()
        if target == "mission" and self.mission_mode_id_provider is not None:
            return self.mission_mode_id_provider()
        return None


@dataclass(frozen=True)
class HoldInterruption:
    request_id: str
    command_id: str
    started_at: datetime
    interrupted_owners: tuple[str, ...]
    warned: bool = False


class HoldInterruptionReconciler:
    def __init__(
        self,
        *,
        mission_state_provider: Callable[[], Any],
        operation_state_provider: Callable[[], Any],
        event_log: RuntimeEventLog,
        timeout_seconds: float = 3.0,
    ):
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider
        self.event_log = event_log
        self.timeout_seconds = timeout_seconds
        self._pending: HoldInterruption | None = None

    def record_hold(self, *, request_id: str, command_id: str) -> None:
        owners = tuple(self._active_owners())
        if not owners:
            self._pending = None
            return
        self._pending = HoldInterruption(
            request_id=request_id,
            command_id=command_id,
            started_at=_utc_now(),
            interrupted_owners=owners,
        )
        self.event_log.append(
            OperatorEvent(
                event_id=str(uuid.uuid4()),
                source=EventSource.RUNTIME,
                category="control_owner_interruption",
                severity="warning",
                message=f"PX4 Hold interrupted active control owner(s): {', '.join(owners)}",
                request_id=request_id,
                command_id=command_id,
                details={"interrupted_owners": list(owners)},
            )
        )

    def warnings(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        pending = self._pending
        if pending is None:
            return []
        active_owners = self._active_owners()
        still_active = [owner for owner in pending.interrupted_owners if owner in active_owners]
        if not still_active:
            self._pending = None
            return []
        current = now or _utc_now()
        elapsed = (current - pending.started_at).total_seconds()
        if elapsed < self.timeout_seconds:
            return []
        warning = {
            "request_id": pending.request_id,
            "command_id": pending.command_id,
            "interrupted_owners": list(pending.interrupted_owners),
            "still_active_owners": still_active,
            "started_at": pending.started_at.isoformat(),
            "elapsed_seconds": elapsed,
            "message": "PX4 Hold succeeded but active mission/custom-operation state has not reconciled",
        }
        if not pending.warned:
            self.event_log.append(
                OperatorEvent(
                    event_id=str(uuid.uuid4()),
                    source=EventSource.RUNTIME,
                    category="hold_reconciliation",
                    severity="warning",
                    message=warning["message"],
                    request_id=pending.request_id,
                    command_id=pending.command_id,
                    details=warning,
                )
            )
            self._pending = HoldInterruption(
                request_id=pending.request_id,
                command_id=pending.command_id,
                started_at=pending.started_at,
                interrupted_owners=pending.interrupted_owners,
                warned=True,
            )
        return [warning]

    def _active_owners(self) -> list[str]:
        owners: list[str] = []
        mission = self.mission_state_provider()
        operation = self.operation_state_provider()
        if mission.latest.get("mission_active") is True or mission.mission_state == "active":
            owners.append("mission")
        if operation.latest.get("operation_active") is True or operation.active_operation_id:
            owners.append("custom_operation")
        return owners


class FlightModeCommandHandlers:
    def __init__(
        self,
        *,
        gate: FlightCommandGate,
        transition_tracker: ControlTransitionTracker,
        event_log: RuntimeEventLog,
        mode_adapter: ControlModeCommandAdapter | None = None,
    ):
        self.gate = gate
        self.transition_tracker = transition_tracker
        self.event_log = event_log
        self.mode_adapter = mode_adapter or ControlModeCommandAdapter()

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in MODE_TRANSITION_TARGETS:
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.FLIGHT_CRITICAL,
                transport="control_mode_adapter",
                summary=f"Control mode transition {command_id}",
            )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        self.event_log.record_command_request(
            command_id=request.command_id,
            request_id=request.request_id,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=True,
        )
        target = MODE_TRANSITION_TARGETS.get(request.command_id)
        if target is None:
            return _reject(request, "unsupported mode command", ErrorCode.HANDLER_UNAVAILABLE)

        rejection_reason = self.gate.rejection_reason(request.command_id)
        if rejection_reason is not None:
            return _reject(request, rejection_reason, ErrorCode.DEGRADED_STATE)

        try:
            mode_result = self.mode_adapter.request_mode(target)
        except Exception as exc:
            return _reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE)

        transition = self.transition_tracker.start(
            command_id=request.command_id,
            request_id=request.request_id,
            target=target,
        )
        self.event_log.record_command_decision(
            command_id=request.command_id,
            request_id=request.request_id,
            accepted=True,
            reason=None,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=True,
        )
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=True,
            result={
                "mode_request": mode_result,
                "transition": transition.as_dict(),
                "command_permissions": self.gate.command_permissions(),
            },
        )


def _reject(request: CommandRequest, reason: str, code: ErrorCode) -> ActionStartResponse:
    return ActionStartResponse(
        request_id=request.request_id,
        command_id=request.command_id,
        accepted=False,
        started=False,
        message=reason,
        rejection=CommandRejection(
            code=code,
            message=reason,
            request_id=request.request_id,
            command_id=request.command_id,
            retryable=True,
            degraded_reason=reason if code == ErrorCode.DEGRADED_STATE else None,
        ),
    )


def _deduplicate(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _mode_label(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if "hold" in normalized or "loiter" in normalized:
        return "hold"
    if "mission" in normalized:
        return "mission"
    if "position" in normalized or "posctl" in normalized:
        return "position"
    return normalized or None


def _custom_operation_mode_active(operation: Any) -> bool:
    latest = getattr(operation, "latest", {}) or {}
    status = getattr(operation, "status", None)
    owned_mode = str(latest.get("owned_mode", "")).lower()
    control_owner = str(latest.get("control_owner", "")).lower()
    state_label = str(latest.get("operation_state_label", "")).lower()
    return (
        status in {"custom_operation_idle", "custom_operation_active"}
        or control_owner == "custom_operation"
        or "customoperation" in owned_mode.replace(" ", "")
        or state_label.startswith("custom_operation")
    )


def _drone_location_label(value: Any) -> str:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return "unknown"
    return {
        0: "unknown",
        1: "on_ground",
        2: "in_flight",
        3: "on_cable",
    }.get(numeric, "unknown")


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _px4_input_qos():
    try:
        from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
    except ImportError:
        return 10
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=10,
        reliability=ReliabilityPolicy.BEST_EFFORT,
        durability=DurabilityPolicy.VOLATILE,
    )


def register_flight_mode_command_handlers(
    registry: DispatchRegistry,
    *,
    gate: FlightCommandGate,
    transition_tracker: ControlTransitionTracker,
    event_log: RuntimeEventLog,
    mode_adapter: ControlModeCommandAdapter | None = None,
) -> FlightModeCommandHandlers:
    handlers = FlightModeCommandHandlers(
        gate=gate,
        transition_tracker=transition_tracker,
        event_log=event_log,
        mode_adapter=mode_adapter,
    )
    handlers.register(registry)
    return handlers
