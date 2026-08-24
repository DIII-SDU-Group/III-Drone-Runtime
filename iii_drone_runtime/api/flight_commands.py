"""Flight command gating and control-owner transition tracking."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Callable
from threading import Lock
import time
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
    expected_mode_key: str | None = None
    expected_mode_id: int | None = None
    message: str | None = None

    def timed_out(self, *, now: datetime | None = None) -> bool:
        if self.status not in {"transitioning", "stopping"}:
            return False
        current = now or _utc_now()
        return current >= self.started_at + timedelta(seconds=self.timeout_seconds)

    def as_dict(self, *, now: datetime | None = None) -> dict[str, Any]:
        timed_out = self.status == "timed_out" or self.timed_out(now=now)
        return {
            "command_id": self.command_id,
            "request_id": self.request_id,
            "target": self.target,
            "started_at": self.started_at.isoformat(),
            "timeout_seconds": self.timeout_seconds,
            "status": "timed_out" if timed_out else self.status,
            "timed_out": timed_out,
            "expected_mode_key": self.expected_mode_key,
            "expected_mode_id": self.expected_mode_id,
            "message": self.message,
        }


class ControlTransitionTracker:
    def __init__(self, *, timeout_seconds: float = 5.0):
        self.timeout_seconds = timeout_seconds
        self._transition: ControlTransition | None = None
        self._lock = Lock()

    def start(
        self,
        *,
        command_id: str,
        request_id: str,
        target: str,
        timeout_seconds: float | None = None,
        expected_mode_key: str | None = None,
        expected_mode_id: int | None = None,
    ) -> ControlTransition:
        with self._lock:
            self._transition = ControlTransition(
                command_id=command_id,
                request_id=request_id,
                target=target,
                started_at=_utc_now(),
                timeout_seconds=self.timeout_seconds if timeout_seconds is None else timeout_seconds,
                expected_mode_key=expected_mode_key,
                expected_mode_id=expected_mode_id,
            )
            return self._transition

    def active(self, *, now: datetime | None = None) -> ControlTransition | None:
        with self._lock:
            transition = self._transition
        if transition is None:
            return None
        return transition

    def clear(self) -> None:
        with self._lock:
            self._transition = None

    def consume_terminal(self) -> ControlTransition | None:
        with self._lock:
            transition = self._transition
            if transition is None or transition.status in {"transitioning", "stopping"}:
                return None
            self._transition = None
            return transition

    def complete(self, *, status: str, message: str) -> ControlTransition | None:
        with self._lock:
            if self._transition is None:
                return None
            self._transition = replace(self._transition, status=status, message=message)
            return self._transition

    def control_state(self, *, command_permissions: dict[str, list[str]] | None = None, now: datetime | None = None) -> ControlDomainState:
        transition = self.active(now=now)
        latest = {
            "command_permissions": command_permissions or {},
            "transition": transition.as_dict(now=now) if transition else None,
        }
        transition_payload = transition.as_dict(now=now) if transition else None
        transition_status = transition_payload["status"] if transition_payload else None
        degraded_reason = None
        owner = "transitioning" if transition_status == "transitioning" else "unknown"
        if transition_status == "stopping":
            owner = "stopping"
        source_availability = "available"
        if transition_status == "active" and transition is not None:
            owner = transition.target
        if transition_status in {"timed_out", "rejected"} and transition is not None:
            owner = "degraded_conflict"
            source_availability = "degraded"
            status_label = "timed out" if transition_status == "timed_out" else transition_status
            degraded_reason = transition.message or f"control transition to {transition.target} {status_label}"
        return ControlDomainState(
            source_label="runtime_control_gate",
            freshness="fresh",
            source_availability=source_availability,
            degraded_reason=degraded_reason,
            latest={**latest, "transition": transition_payload},
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

    def disabled_reasons(self, command_id: str, *, mode_key: str | None = None) -> list[str]:
        if command_id == CommandId.PX4_HOLD.value:
            return self._hold_reasons()
        if command_id == CommandId.PX4_ARM.value:
            return self._base_flight_reasons()
        if command_id == CommandId.PX4_TAKEOFF.value:
            return self._base_flight_reasons() + self._takeoff_reasons()
        if command_id == CommandId.PX4_LAND.value:
            return self._base_flight_reasons() + self._land_reasons()
        if command_id == CommandId.MISSION_ACTIVATE.value:
            return self._base_flight_reasons() + self._mission_activation_reasons(mode_key=mode_key)
        if command_id == CommandId.CUSTOM_OPERATION_ACTIVATE.value:
            return self._base_flight_reasons() + self._custom_operation_activation_reasons()
        return [f"unsupported flight command: {command_id}"]

    def rejection_reason(self, command_id: str, *, mode_key: str | None = None) -> str | None:
        reasons = self.disabled_reasons(command_id, mode_key=mode_key)
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
        if transition is not None and transition.status in {"transitioning", "stopping"}:
            outcome = self._transition_outcome(transition)
            if outcome is not None:
                status, message = outcome
                self.transition_tracker.complete(status=status, message=message)
        state = self.transition_tracker.control_state(command_permissions=self.command_permissions())
        if self.hold_reconciler is not None:
            state.latest["hold_interruption_warnings"] = self.hold_reconciler.warnings()
            state.latest["hold_interruption"] = self.hold_reconciler.state()
            state.latest["mission_hold_termination"] = self.hold_reconciler.completed_interruption("mission")
        return state

    def _transition_outcome(self, transition: ControlTransition) -> tuple[str, str] | None:
        if transition.target == "custom_operation":
            operation = self.operation_state_provider()
            operation_active = operation.latest.get("operation_active") is True or bool(operation.active_operation_id)
            if operation_active:
                return "active", "Custom Operation mode confirmed"
            if transition.timed_out():
                return "timed_out", "Custom Operation mode was not confirmed before the transition timeout"
            return None
        if transition.target == "mission":
            return self._mission_transition_outcome(transition)
        if transition.target == "px4_takeoff":
            vehicle = self.vehicle_state_provider.state()
            if vehicle.in_air is True:
                return "active", "PX4 takeoff confirmed"
            if transition.timed_out():
                return "timed_out", "PX4 takeoff was not confirmed before the transition timeout"
            return None
        if transition.target == "px4_land":
            vehicle = self.vehicle_state_provider.state()
            if vehicle.in_air is False and vehicle.armed is False:
                return "terminated", "PX4 landing and disarm confirmed"
            if transition.timed_out():
                return "timed_out", "PX4 landing was not confirmed before the transition timeout"
            return None
        if transition.target == "px4_hold":
            vehicle = self.vehicle_state_provider.state()
            hold_confirmed = _mode_label(vehicle.nav_state or vehicle.flight_mode) == "hold"
            if self.hold_reconciler is not None:
                return self.hold_reconciler.transition_outcome(
                    hold_confirmed=hold_confirmed,
                    transition_timed_out=transition.timed_out(),
                )
            if hold_confirmed:
                return "terminated", "PX4 Hold confirmed"
            if transition.timed_out():
                return "timed_out", "PX4 Hold was not confirmed before the transition timeout"
            return None
        return ("timed_out", "control transition timed out") if transition.timed_out() else None

    def _mission_transition_outcome(self, transition: ControlTransition) -> tuple[str, str] | None:
        mission = self.mission_state_provider()
        expected_key = transition.expected_mode_key
        expected_id = transition.expected_mode_id
        mode = _mission_mode(mission, expected_key)
        if mode is None:
            return "rejected", f"mission mode registry no longer contains {expected_key}"
        if mode.freshness != "fresh":
            return "rejected", f"mission mode {expected_key} status became {mode.freshness}"
        if not mode.registered or mode.mode_id is None:
            return "rejected", f"mission mode {expected_key} is no longer registered"
        if mode.mode_id != expected_id:
            return "rejected", (
                f"mission mode {expected_key} ID changed during activation "
                f"from {expected_id} to {mode.mode_id}"
            )

        vehicle = self.vehicle_state_provider.state()
        ros_state = vehicle.latest.get("ros_uxrce", {})
        px4_mode_id = _optional_int(ros_state.get("nav_state_id")) if isinstance(ros_state, dict) else None
        mission_active = mission.latest.get("mission_active") is True or mission.mission_state == "active"
        if px4_mode_id == expected_id and mode.active and mission_active:
            return "active", f"mission mode {expected_key} active on PX4 ID {expected_id}"
        return None

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

    def _mission_activation_reasons(self, *, mode_key: str | None = None) -> list[str]:
        reasons = self._system_running_reasons()
        vehicle = self.vehicle_state_provider.state()
        mission = self.mission_state_provider()
        if vehicle.armed is not True:
            reasons.append("mission activation requires the vehicle to be armed")
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
        if mission.freshness != "fresh":
            reasons.append(f"mission activation requires fresh mode registry state (currently {mission.freshness})")
        if mission.required_modes_registered is not True:
            reasons.append("mission activation requires all required modes to be registered")
        expected_key = mode_key or mission.latest.get("owned_mode")
        if not expected_key:
            reasons.append("mission activation requires an owned mission mode key")
        else:
            owned_key = mission.latest.get("owned_mode")
            if owned_key and expected_key != owned_key:
                reasons.append(f"mission mode {expected_key} is not the owned activation mode {owned_key}")
            mode = _mission_mode(mission, expected_key)
            if mode is None:
                reasons.append(f"mission mode registry does not contain {expected_key}")
            else:
                if mode.freshness != "fresh":
                    reasons.append(f"mission mode {expected_key} status is {mode.freshness}")
                if not mode.registered:
                    reasons.append(f"mission mode {expected_key} is not registered")
                if mode.mode_id is None:
                    reasons.append(f"mission mode {expected_key} has no live PX4 ID")
                else:
                    selectability_reason = _external_mode_selectability_reason(
                        vehicle,
                        mode_id=mode.mode_id,
                        label=f"mission mode {expected_key}",
                    )
                    if selectability_reason:
                        reasons.append(selectability_reason)
        reasons.extend(mission.latest.get("activation_rejections", []))
        reasons.extend(mission.latest.get("overview_rejections", []))
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
        mode_id = _optional_int(operation.latest.get("mode_id"))
        if mode_id is not None:
            selectability_reason = _external_mode_selectability_reason(
                vehicle,
                mode_id=mode_id,
                label="Custom Operation mode",
            )
            if selectability_reason:
                reasons.append(selectability_reason)
        if operation.degraded_reason:
            degraded_reasons = [part.strip() for part in operation.degraded_reason.split(";") if part.strip()]
            reasons.extend(reason for reason in degraded_reasons if reason != "CustomOperation mode is not active")
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

    def request_mode(
        self,
        target: str,
        *,
        mode_key: str | None = None,
        expected_mode_id: int | None = None,
    ) -> dict[str, Any]:
        raise RuntimeError(f"control mode request adapter unavailable for {target}")


class Px4NavStateModeAdapter(ControlModeCommandAdapter):
    def __init__(
        self,
        *,
        node_provider: Callable[[], Any | None],
        custom_operation_mode_id_provider: Callable[[], int | None],
        mission_mode_id_provider: Callable[[str], int | None] | None = None,
        repeat_count: int = 5,
    ):
        self.node_provider = node_provider
        self.custom_operation_mode_id_provider = custom_operation_mode_id_provider
        self.mission_mode_id_provider = mission_mode_id_provider
        self.repeat_count = repeat_count
        self._publisher = None

    def request_mode(
        self,
        target: str,
        *,
        mode_key: str | None = None,
        expected_mode_id: int | None = None,
    ) -> dict[str, Any]:
        if target not in {"custom_operation", "mission"}:
            raise RuntimeError(f"control mode request adapter unavailable for {target}")
        mode_id = self._mode_id(target, mode_key=mode_key)
        if mode_id is None:
            raise RuntimeError(f"{target.replace('_', ' ')} mode id has not been received")
        if expected_mode_id is not None and mode_id != expected_mode_id:
            raise RuntimeError(
                f"{target.replace('_', ' ')} mode id changed before dispatch "
                f"from {expected_mode_id} to {mode_id}"
            )
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

        discovery_deadline = time.monotonic() + 2.0
        while publisher.get_subscription_count() == 0 and time.monotonic() < discovery_deadline:
            time.sleep(0.05)
        if publisher.get_subscription_count() == 0:
            raise RuntimeError("PX4 vehicle-command subscription was not discovered")

        for _ in range(max(1, self.repeat_count)):
            message = VehicleCommand()
            message.timestamp = int(node.get_clock().now().nanoseconds / 1000)
            message.command = VehicleCommand.VEHICLE_CMD_SET_NAV_STATE
            message.param1 = float(mode_id)
            message.target_system = 1
            message.target_component = 1
            message.source_system = 255
            message.source_component = 0
            message.from_external = True
            publisher.publish(message)
            time.sleep(0.05)
        return {
            "transport": "px4_vehicle_command",
            "command": "VEHICLE_CMD_SET_NAV_STATE",
            "target": target,
            "mode_key": mode_key,
            "mode_id": mode_id,
            "repeat_count": max(1, self.repeat_count),
        }

    def _mode_id(self, target: str, *, mode_key: str | None = None) -> int | None:
        if target == "custom_operation":
            return self.custom_operation_mode_id_provider()
        if target == "mission" and self.mission_mode_id_provider is not None and mode_key:
            return self.mission_mode_id_provider(mode_key)
        return None


@dataclass(frozen=True)
class HoldInterruption:
    request_id: str
    command_id: str
    started_at: datetime
    interrupted_owners: tuple[str, ...]
    warned: bool = False
    completed: bool = False


class HoldInterruptionReconciler:
    def __init__(
        self,
        *,
        mission_state_provider: Callable[[], Any],
        operation_state_provider: Callable[[], Any],
        event_log: RuntimeEventLog,
        timeout_seconds: float = 3.0,
        state_path: Path | None = None,
    ):
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider
        self.event_log = event_log
        self.timeout_seconds = timeout_seconds
        self.state_path = state_path
        self._state_lock = Lock()
        self._pending: HoldInterruption | None = None
        self._completed_by_owner: dict[str, HoldInterruption] = {}
        self._load_completed()

    def record_hold(self, *, request_id: str, command_id: str) -> None:
        owners = tuple(self._active_owners())
        self._pending = HoldInterruption(
            request_id=request_id,
            command_id=command_id,
            started_at=_utc_now(),
            interrupted_owners=owners,
        )
        if not owners:
            return
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

    def transition_outcome(
        self,
        *,
        hold_confirmed: bool,
        transition_timed_out: bool,
    ) -> tuple[str, str] | None:
        pending = self._pending
        if pending is None:
            if hold_confirmed:
                return "terminated", "PX4 Hold confirmed; no autonomous owner was active"
            if transition_timed_out:
                return "timed_out", "PX4 Hold was not confirmed before the transition timeout"
            return None

        still_active = [owner for owner in pending.interrupted_owners if owner in self._active_owners()]
        if hold_confirmed and not still_active:
            if not pending.completed:
                self._pending = replace(pending, completed=True)
                for owner in pending.interrupted_owners:
                    self._completed_by_owner[owner] = self._pending
                self._persist_completed()
                if pending.interrupted_owners:
                    self.event_log.append(
                        OperatorEvent(
                            event_id=str(uuid.uuid4()),
                            source=EventSource.RUNTIME,
                            category="control_owner_terminated",
                            severity="info",
                            message="PX4 Hold confirmed and autonomous control ownership cleared",
                            request_id=pending.request_id,
                            command_id=pending.command_id,
                            details={"interrupted_owners": list(pending.interrupted_owners)},
                        )
                    )
            return "terminated", "PX4 Hold confirmed; autonomous action stopped and mission ownership cleared"
        if transition_timed_out:
            if not hold_confirmed:
                return "timed_out", "PX4 Hold was not confirmed before the transition timeout"
            owners = ", ".join(still_active)
            return "timed_out", f"PX4 Hold confirmed, but control ownership did not clear: {owners}"
        if hold_confirmed:
            owners = ", ".join(still_active)
            return "stopping", f"PX4 Hold confirmed; safely stopping active owner(s): {owners}"
        return None

    def state(self) -> dict[str, Any] | None:
        """Return durable, typed evidence for the latest explicit Hold request."""
        pending = self._pending
        if pending is None:
            return None
        return {
            "request_id": pending.request_id,
            "command_id": pending.command_id,
            "started_at": pending.started_at.isoformat(),
            "interrupted_owners": list(pending.interrupted_owners),
            "completed": pending.completed,
        }

    def completed_interruption(self, owner: str) -> dict[str, Any] | None:
        """Return durable evidence that Hold terminated one control owner."""
        completed = self._completed_by_owner.get(owner)
        if completed is None:
            return None
        return {
            "request_id": completed.request_id,
            "command_id": completed.command_id,
            "started_at": completed.started_at.isoformat(),
            "interrupted_owners": list(completed.interrupted_owners),
            "completed": True,
        }

    def clear_completed_interruption(self, owner: str) -> None:
        """Start a fresh owner lifecycle after a new activation is accepted."""
        if self._completed_by_owner.pop(owner, None) is not None:
            self._persist_completed()

    def _load_completed(self) -> None:
        if self.state_path is None:
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return
        owners = payload.get("owners") if isinstance(payload, dict) else None
        if not isinstance(owners, dict):
            return
        for owner, raw in owners.items():
            if not isinstance(owner, str) or not isinstance(raw, dict):
                continue
            try:
                interruption = HoldInterruption(
                    request_id=str(raw["request_id"]),
                    command_id=str(raw["command_id"]),
                    started_at=datetime.fromisoformat(str(raw["started_at"])),
                    interrupted_owners=tuple(str(item) for item in raw["interrupted_owners"]),
                    completed=True,
                )
            except (KeyError, TypeError, ValueError):
                continue
            self._completed_by_owner[owner] = interruption

    def _persist_completed(self) -> None:
        if self.state_path is None:
            return
        payload = {
            "owners": {
                owner: {
                    "request_id": interruption.request_id,
                    "command_id": interruption.command_id,
                    "started_at": interruption.started_at.isoformat(),
                    "interrupted_owners": list(interruption.interrupted_owners),
                }
                for owner, interruption in self._completed_by_owner.items()
            }
        }
        with self._state_lock:
            try:
                self.state_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = self.state_path.with_suffix(f"{self.state_path.suffix}.tmp")
                temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
                temporary.replace(self.state_path)
            except OSError as exc:
                self.event_log.append(
                    OperatorEvent(
                        event_id=str(uuid.uuid4()),
                        source=EventSource.RUNTIME,
                        category="runtime_state_persistence",
                        severity="warning",
                        message="Could not persist completed Hold interruption evidence",
                        details={"path": str(self.state_path), "reason": str(exc)},
                    )
                )

    def warnings(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        pending = self._pending
        if pending is None or pending.completed:
            return []
        active_owners = self._active_owners()
        still_active = [owner for owner in pending.interrupted_owners if owner in active_owners]
        if not still_active:
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
                completed=pending.completed,
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
        mission_activation_precondition: Callable[[], dict[str, Any]] | None = None,
        hold_reconciler: HoldInterruptionReconciler | None = None,
    ):
        self.gate = gate
        self.transition_tracker = transition_tracker
        self.event_log = event_log
        self.mode_adapter = mode_adapter or ControlModeCommandAdapter()
        self.mission_activation_precondition = mission_activation_precondition
        self.hold_reconciler = hold_reconciler

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

        mode_key = None
        expected_mode_id = None
        if target == "mission":
            parameters = getattr(request, "parameters", {}) or {}
            raw_mode_key = parameters.get("mode_key")
            if not isinstance(raw_mode_key, str) or not raw_mode_key.strip():
                return _reject(
                    request,
                    "mission activation requires a stable mode_key",
                    ErrorCode.INVALID_REQUEST,
                )
            mode_key = raw_mode_key.strip()

        rejection_reason = self.gate.rejection_reason(request.command_id, mode_key=mode_key)
        if rejection_reason is not None:
            return _reject(request, rejection_reason, ErrorCode.DEGRADED_STATE)

        if target == "mission" and self.mission_activation_precondition is not None:
            try:
                self.mission_activation_precondition()
            except Exception as exc:
                return _reject(request, str(exc), ErrorCode.DEGRADED_STATE)

        if mode_key is not None:
            mission_mode = _mission_mode(self.gate.mission_state_provider(), mode_key)
            expected_mode_id = mission_mode.mode_id if mission_mode is not None else None
            if expected_mode_id is None:
                return _reject(
                    request,
                    f"mission mode {mode_key} has no live PX4 ID",
                    ErrorCode.STALE_STATE,
                )

        try:
            if target == "mission":
                mode_result = self.mode_adapter.request_mode(
                    target,
                    mode_key=mode_key,
                    expected_mode_id=expected_mode_id,
                )
            else:
                mode_result = self.mode_adapter.request_mode(target)
        except Exception as exc:
            return _reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE)

        if target == "mission" and self.hold_reconciler is not None:
            self.hold_reconciler.clear_completed_interruption("mission")
        transition = self.transition_tracker.start(
            command_id=request.command_id,
            request_id=request.request_id,
            target=target,
            expected_mode_key=mode_key,
            expected_mode_id=expected_mode_id,
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
    control_owner = str(latest.get("control_owner", "")).lower()
    return control_owner == "custom_operation"


def _mission_mode(mission: Any, mode_key: str | None) -> Any | None:
    if not mode_key:
        return None
    for mode in getattr(mission, "modes", []):
        if getattr(mode, "mode_key", None) == mode_key:
            return mode
    return None


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


def _external_mode_selectability_reason(vehicle: Any, *, mode_id: int, label: str) -> str | None:
    vehicle_status = (
        getattr(vehicle, "latest", {})
        .get("ros_uxrce", {})
        .get("raw", {})
        .get("vehicle_status", {})
    )
    selectable_mask = _optional_int(vehicle_status.get("can_set_nav_states_mask"))
    if selectable_mask is None:
        return None
    if mode_id < 0 or mode_id >= 32 or not selectable_mask & (1 << mode_id):
        return f"{label} is not selectable in PX4; restart the PX4 bridge dependents while disarmed"
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
    mission_activation_precondition: Callable[[], dict[str, Any]] | None = None,
    hold_reconciler: HoldInterruptionReconciler | None = None,
) -> FlightModeCommandHandlers:
    handlers = FlightModeCommandHandlers(
        gate=gate,
        transition_tracker=transition_tracker,
        event_log=event_log,
        mode_adapter=mode_adapter,
        mission_activation_precondition=mission_activation_precondition,
        hold_reconciler=hold_reconciler,
    )
    handlers.register(registry)
    return handlers
