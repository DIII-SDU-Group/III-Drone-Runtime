"""Nonblocking CustomOperation facade for runtime API handlers."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import threading
import uuid
from typing import Any, Protocol

from ..ros_services import runtime_reentrant_callback_group


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


SUPPORTED_OPERATIONS = {
    "fly_to_position",
    "cable_aware_fly_to_position",
    "fly_to_object",
    "cable_landing",
    "cable_takeoff",
    "hover",
    "hover_by_object",
    "hover_on_cable",
}


@dataclass(frozen=True)
class OperationReadinessContext:
    custom_operation_mode_registered: bool = False
    custom_operation_mode_active: bool = False
    mission_active: bool = False
    active_operation_id: str | None = None
    available_frames: set[str] | None = None
    available_target_ids: set[int] | None = None
    available_cable_ids: set[int] | None = None


@dataclass(frozen=True)
class OperationValidationResult:
    ok: bool
    operation: str
    arguments: dict[str, Any] = field(default_factory=dict)
    rejection_reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OperationEvent:
    event_type: str
    operation_id: str
    operation: str
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_utc_now)

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "operation_id": self.operation_id,
            "operation": self.operation,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class OperationRecord:
    operation_id: str
    operation: str
    arguments: dict[str, Any]
    request_id: str
    status: str
    accepted: bool = False
    rejection_reasons: list[str] = field(default_factory=list)
    result: dict[str, Any] | None = None
    feedback_count: int = 0
    last_feedback: dict[str, Any] | None = None
    started_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)
    completed_at: datetime | None = None
    transport_goal: Any = None

    @property
    def terminal(self) -> bool:
        return self.status in {"succeeded", "failed", "rejected", "cancelled"}

    def as_dict(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "operation": self.operation,
            "arguments": self.arguments,
            "request_id": self.request_id,
            "status": self.status,
            "accepted": self.accepted,
            "rejection_reasons": self.rejection_reasons,
            "result": self.result,
            "feedback_count": self.feedback_count,
            "last_feedback": self.last_feedback,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class OperationGoalHandle(Protocol):
    accepted: bool

    def cancel(self) -> bool:
        ...


class CustomOperationTransport(Protocol):
    def start(
        self,
        *,
        operation: str,
        arguments: dict[str, Any],
        request_id: str,
        feedback_callback: Callable[[dict[str, Any]], None],
        result_callback: Callable[[dict[str, Any]], None],
    ) -> OperationGoalHandle:
        ...


class UnavailableCustomOperationTransport:
    def __init__(self, reason: str = "CustomOperation action transport is unavailable"):
        self.reason = reason

    def start(
        self,
        *,
        operation: str,
        arguments: dict[str, Any],
        request_id: str,
        feedback_callback: Callable[[dict[str, Any]], None],
        result_callback: Callable[[dict[str, Any]], None],
    ) -> OperationGoalHandle:
        del operation, arguments, request_id, feedback_callback, result_callback
        return _RejectedGoal(self.reason)


class RosCustomOperationTransport:
    """Thin ROS action transport; imports ROS dependencies only when used."""

    def __init__(
        self,
        node: Any | None = None,
        namespace: str = "/mission/custom_operation",
        *,
        node_provider: Callable[[], Any | None] | None = None,
        goal_response_timeout_s: float = 3.0,
    ):
        self.node_provider = node_provider or (lambda: node)
        self.namespace = namespace.rstrip("/")
        self.goal_response_timeout_s = goal_response_timeout_s
        self._action_client = None
        self._action_client_node = None

    def start(
        self,
        *,
        operation: str,
        arguments: dict[str, Any],
        request_id: str,
        feedback_callback: Callable[[dict[str, Any]], None],
        result_callback: Callable[[dict[str, Any]], None],
    ) -> OperationGoalHandle:
        node = self.node_provider()
        if node is None:
            return _RejectedGoal("runtime ROS node is unavailable for CustomOperation")
        from action_msgs.msg import GoalStatus
        from rclpy.action import ActionClient
        from iii_drone_interfaces.action import CustomOperation

        if self._action_client is None or self._action_client_node is not node:
            self._action_client = ActionClient(
                node,
                CustomOperation,
                f"{self.namespace}/run_operation",
                callback_group=runtime_reentrant_callback_group(node),
            )
            self._action_client_node = node
        if not self._action_client.wait_for_server(timeout_sec=0.0):
            return _RejectedGoal("CustomOperation action server unavailable")

        goal = CustomOperation.Goal()
        goal.operation = operation
        goal.arguments_json = json.dumps(arguments, sort_keys=True)
        goal.request_id = request_id

        def on_feedback(feedback_msg: Any) -> None:
            feedback = getattr(feedback_msg, "feedback", feedback_msg)
            try:
                payload = _message_to_dict(feedback)
            except Exception as exc:
                # User-facing feedback is optional; never let serialization or
                # event-sink failures terminate the shared ROS executor.
                payload = {"serialization_error": str(exc)}
            try:
                feedback_callback(payload)
            except Exception:
                return

        send_future = self._action_client.send_goal_async(goal, feedback_callback=on_feedback)
        response_ready = threading.Event()
        send_future.add_done_callback(lambda _future: response_ready.set())
        if not response_ready.wait(timeout=self.goal_response_timeout_s):
            # A goal response arriving after the HTTP request timed out must not
            # leave an untracked aircraft operation in control.
            send_future.add_done_callback(_cancel_late_accepted_goal)
            return _RejectedGoal(f"timed out waiting for {operation} goal response")

        try:
            goal_handle = send_future.result()
        except Exception as exc:
            return _RejectedGoal(f"failed to send {operation} goal: {exc}")
        if not goal_handle or not goal_handle.accepted:
            return _RejectedGoal(f"{operation} goal rejected")

        result_future = goal_handle.get_result_async()

        def on_result_done(done_future: Any) -> None:
            try:
                wrapped = done_future.result()
                result = getattr(wrapped, "result", None)
                status = int(getattr(wrapped, "status", 0))
                cancelled = status == GoalStatus.STATUS_CANCELED
                success = status == GoalStatus.STATUS_SUCCEEDED and bool(getattr(result, "success", True))
                payload = {
                    "success": success,
                    "cancelled": cancelled,
                    "status": "cancelled" if cancelled else status,
                    "error": str(getattr(result, "error", "")),
                }
            except Exception as exc:
                payload = {"success": False, "error": f"failed to receive {operation} result: {exc}"}
            try:
                result_callback(payload)
            except Exception:
                return

        result_future.add_done_callback(on_result_done)
        # Keep both objects alive for the complete action lifetime. rclpy tracks
        # pending result requests internally, but retaining the future here is
        # the explicit ownership contract for this nonblocking facade and
        # prevents executor/version-specific weak-reference behavior.
        return _AsyncRosGoal(goal_handle, result_future)


class _RejectedGoal:
    accepted = False

    def __init__(self, reason: str):
        self.reason = reason

    def cancel(self) -> bool:
        return False


class _AsyncRosGoal:
    accepted = True

    def __init__(self, goal_handle: Any, result_future: Any):
        self._goal_handle = goal_handle
        self._result_future = result_future

    def cancel(self) -> bool:
        self._goal_handle.cancel_goal_async()
        return True


def _cancel_late_accepted_goal(future: Any) -> None:
    try:
        goal_handle = future.result()
        if goal_handle and goal_handle.accepted:
            goal_handle.cancel_goal_async()
    except Exception:
        # The original caller already received a typed rejection. There is no
        # further recovery possible when even the late response is unreadable.
        return


class NonblockingCustomOperationClient:
    def __init__(
        self,
        *,
        transport: CustomOperationTransport,
        readiness_provider: Callable[[], OperationReadinessContext] | None = None,
        event_sink: Callable[[OperationEvent], None] | None = None,
        max_events: int = 100,
    ):
        self.transport = transport
        self.readiness_provider = readiness_provider or (lambda: OperationReadinessContext())
        self.event_sink = event_sink
        self._lock = threading.RLock()
        self._records: dict[str, OperationRecord] = {}
        self._active_operation_id: str | None = None
        self._events: deque[OperationEvent] = deque(maxlen=max_events)

    def validate(self, operation: str, arguments: dict[str, Any], *, context: OperationReadinessContext | None = None) -> OperationValidationResult:
        normalized, reasons = validate_operation_request(
            operation,
            arguments,
            context=context or self.readiness_provider(),
        )
        with self._lock:
            active = self._active_record_locked()
            if active is not None:
                reasons.append(f"operation already active: {active.operation_id}")
        return OperationValidationResult(
            ok=not reasons,
            operation=operation,
            arguments=normalized,
            rejection_reasons=reasons,
        )

    def start(self, operation: str, arguments: dict[str, Any], *, request_id: str = "") -> OperationRecord:
        validation = self.validate(operation, arguments)
        operation_id = str(uuid.uuid4())
        if not validation.ok:
            record = OperationRecord(
                operation_id=operation_id,
                operation=operation,
                arguments=validation.arguments,
                request_id=request_id,
                status="rejected",
                accepted=False,
                rejection_reasons=validation.rejection_reasons,
                completed_at=_utc_now(),
            )
            self._store_record(record)
            self._emit(record, "rejected", {"rejection_reasons": validation.rejection_reasons})
            return record

        record = OperationRecord(
            operation_id=operation_id,
            operation=operation,
            arguments=validation.arguments,
            request_id=request_id,
            status="starting",
        )
        self._store_record(record)

        def feedback_callback(feedback: dict[str, Any]) -> None:
            self.record_feedback(operation_id, feedback)

        def result_callback(result: dict[str, Any]) -> None:
            self.record_result(operation_id, result)

        try:
            goal = self.transport.start(
                operation=operation,
                arguments=validation.arguments,
                request_id=request_id,
                feedback_callback=feedback_callback,
                result_callback=result_callback,
            )
        except Exception as exc:
            self.record_result(operation_id, {"success": False, "error": str(exc)})
            return self.status(operation_id)

        with self._lock:
            stored = self._records[operation_id]
            stored.transport_goal = goal
            if not getattr(goal, "accepted", False):
                stored.accepted = False
                stored.status = "rejected"
                stored.rejection_reasons = [getattr(goal, "reason", "operation goal rejected")]
                stored.completed_at = _utc_now()
                stored.updated_at = stored.completed_at
                self._active_operation_id = None
                event_payload = {"rejection_reasons": stored.rejection_reasons}
                event_type = "rejected"
            elif not stored.terminal:
                stored.accepted = True
                stored.status = "running"
                stored.updated_at = _utc_now()
                self._active_operation_id = operation_id
                event_payload = stored.as_dict()
                event_type = "started"
            else:
                # A very short action may deliver its terminal result while
                # transport.start() is still returning. Preserve that result
                # instead of reviving the record as an active operation.
                event_payload = stored.as_dict()
                event_type = "result"
        self._emit(self.status(operation_id), event_type, event_payload)
        return self.status(operation_id)

    def status(self, operation_id: str | None = None) -> OperationRecord:
        with self._lock:
            target_id = operation_id or self._active_operation_id
            if target_id is None or target_id not in self._records:
                raise KeyError("operation is not tracked")
            return self._copy_record(self._records[target_id])

    def result(self, operation_id: str) -> OperationRecord:
        record = self.status(operation_id)
        if not record.terminal:
            raise RuntimeError("operation result is not available yet")
        return record

    def latest(self) -> OperationRecord:
        with self._lock:
            if not self._records:
                raise KeyError("operation is not tracked")
            record = max(self._records.values(), key=lambda item: item.updated_at)
            return self._copy_record(record)

    def cancel_active(self) -> OperationRecord | None:
        with self._lock:
            active = self._active_record_locked()
            if active is None:
                return None
            goal = active.transport_goal
            operation_id = active.operation_id
        cancelled = bool(goal.cancel()) if goal is not None and hasattr(goal, "cancel") else False
        self.record_result(operation_id, {"success": False, "cancelled": cancelled, "status": "cancelled"})
        return self.status(operation_id)

    def record_feedback(self, operation_id: str, feedback: dict[str, Any]) -> None:
        with self._lock:
            record = self._records[operation_id]
            record.feedback_count += 1
            record.last_feedback = feedback
            record.updated_at = _utc_now()
            snapshot = self._copy_record(record)
        self._emit(snapshot, "feedback", {"feedback": feedback})

    def record_result(self, operation_id: str, result: dict[str, Any]) -> None:
        with self._lock:
            record = self._records[operation_id]
            success = bool(result.get("success", False))
            cancelled = bool(result.get("cancelled", False)) or result.get("status") == "cancelled"
            record.result = result
            record.status = "cancelled" if cancelled else "succeeded" if success else "failed"
            record.updated_at = _utc_now()
            record.completed_at = record.updated_at
            if self._active_operation_id == operation_id:
                self._active_operation_id = None
            snapshot = self._copy_record(record)
        self._emit(snapshot, "result", {"result": result})

    def events(self) -> list[OperationEvent]:
        with self._lock:
            return list(self._events)

    def _store_record(self, record: OperationRecord) -> None:
        with self._lock:
            self._records[record.operation_id] = record

    def _active_record_locked(self) -> OperationRecord | None:
        if self._active_operation_id is None:
            return None
        record = self._records.get(self._active_operation_id)
        if record is None or record.terminal:
            self._active_operation_id = None
            return None
        return record

    def _emit(self, record: OperationRecord, event_type: str, payload: dict[str, Any]) -> None:
        correlated_payload = dict(payload)
        correlated_payload.setdefault("request_id", record.request_id or record.operation_id)
        event = OperationEvent(
            event_type=event_type,
            operation_id=record.operation_id,
            operation=record.operation,
            payload=correlated_payload,
        )
        with self._lock:
            self._events.append(event)
        if self.event_sink is not None:
            self.event_sink(event)

    def _copy_record(self, record: OperationRecord) -> OperationRecord:
        return OperationRecord(
            operation_id=record.operation_id,
            operation=record.operation,
            arguments=dict(record.arguments),
            request_id=record.request_id,
            status=record.status,
            accepted=record.accepted,
            rejection_reasons=list(record.rejection_reasons),
            result=dict(record.result) if record.result is not None else None,
            feedback_count=record.feedback_count,
            last_feedback=dict(record.last_feedback) if record.last_feedback is not None else None,
            started_at=record.started_at,
            updated_at=record.updated_at,
            completed_at=record.completed_at,
            transport_goal=record.transport_goal,
        )


def validate_operation_request(
    operation: str,
    arguments: dict[str, Any],
    *,
    context: OperationReadinessContext,
) -> tuple[dict[str, Any], list[str]]:
    if operation not in SUPPORTED_OPERATIONS:
        return dict(arguments), [f"unsupported custom operation: {operation}"]

    normalized = dict(arguments)
    reasons = _context_rejections(context)
    validators = {
        "fly_to_position": _validate_position_operation,
        "cable_aware_fly_to_position": _validate_position_operation,
        "fly_to_object": _validate_target_operation,
        "cable_landing": _validate_cable_id_operation,
        "cable_takeoff": _validate_cable_takeoff,
        "hover": _validate_hover,
        "hover_by_object": _validate_hover_by_object,
        "hover_on_cable": _validate_hover_on_cable,
    }
    reasons.extend(validators[operation](normalized, context))
    return normalized, _deduplicate(reasons)


def _context_rejections(context: OperationReadinessContext) -> list[str]:
    reasons: list[str] = []
    if not context.custom_operation_mode_registered:
        reasons.append("CustomOperation mode is not registered")
    if not context.custom_operation_mode_active:
        reasons.append("CustomOperation mode is not active")
    if context.mission_active:
        reasons.append("custom operation starts are disabled in Mission mode")
    if context.active_operation_id:
        reasons.append(f"operation already active: {context.active_operation_id}")
    return reasons


def _validate_position_operation(arguments: dict[str, Any], context: OperationReadinessContext) -> list[str]:
    reasons = []
    frame_id = str(arguments.get("frame_id", "")).strip()
    if not frame_id:
        reasons.append("frame_id is required")
    elif context.available_frames is not None and frame_id not in context.available_frames:
        reasons.append(f"frame is unavailable: {frame_id}")
    for field_name in ("x", "y", "z", "yaw"):
        _require_float(arguments, field_name, reasons)
    return reasons


def _validate_target_operation(arguments: dict[str, Any], context: OperationReadinessContext) -> list[str]:
    reasons = []
    target_id = _target_id(arguments)
    if target_id is None:
        reasons.append("target_id is required")
    elif context.available_target_ids is not None and target_id not in context.available_target_ids:
        reasons.append(f"target is unavailable: {target_id}")
    return reasons


def _validate_cable_id_operation(arguments: dict[str, Any], context: OperationReadinessContext) -> list[str]:
    reasons = []
    cable_id = _optional_int(arguments, "target_cable_id")
    if cable_id is None:
        reasons.append("target_cable_id is required")
    elif cable_id < 0:
        reasons.append("target_cable_id must be non-negative")
    elif context.available_cable_ids is not None and cable_id not in context.available_cable_ids:
        reasons.append(f"target cable is unavailable: {cable_id}")
    return reasons


def _validate_cable_takeoff(arguments: dict[str, Any], context: OperationReadinessContext) -> list[str]:
    reasons = _validate_cable_id_operation(arguments, context)
    distance = _require_float(arguments, "target_cable_distance", reasons)
    if distance is not None and distance < 0.0:
        reasons.append("target_cable_distance must be non-negative")
    return reasons


def _validate_hover(arguments: dict[str, Any], context: OperationReadinessContext) -> list[str]:
    del context
    reasons = []
    duration = _require_float(arguments, "duration_s", reasons)
    if duration is not None and duration <= 0.0:
        reasons.append("duration_s must be greater than zero")
    sustain = _require_float(arguments, "sustain_duration_s", reasons, default=0.0)
    if sustain is not None and sustain < 0.0:
        reasons.append("sustain_duration_s must be non-negative")
    return reasons


def _validate_hover_by_object(arguments: dict[str, Any], context: OperationReadinessContext) -> list[str]:
    reasons = _validate_target_operation(arguments, context)
    duration = _require_float(arguments, "duration_s", reasons)
    if duration is not None and duration <= 0.0:
        reasons.append("duration_s must be greater than zero")
    return reasons


def _validate_hover_on_cable(arguments: dict[str, Any], context: OperationReadinessContext) -> list[str]:
    reasons = _validate_cable_id_operation(arguments, context)
    duration = _require_float(arguments, "duration_s", reasons)
    if duration is not None and duration <= 0.0:
        reasons.append("duration_s must be greater than zero")
    _require_float(arguments, "target_z_velocity", reasons, default=0.0)
    _require_float(arguments, "target_yaw_rate", reasons, default=0.0)
    return reasons


def _require_float(arguments: dict[str, Any], field_name: str, reasons: list[str], *, default: float | None = None) -> float | None:
    if field_name not in arguments:
        if default is None:
            reasons.append(f"{field_name} is required")
            return None
        arguments[field_name] = default
    try:
        value = float(arguments[field_name])
    except (TypeError, ValueError):
        reasons.append(f"{field_name} must be numeric")
        return None
    arguments[field_name] = value
    return value


def _optional_int(arguments: dict[str, Any], field_name: str) -> int | None:
    if field_name not in arguments:
        return None
    try:
        value = int(arguments[field_name])
    except (TypeError, ValueError):
        return None
    arguments[field_name] = value
    return value


def _target_id(arguments: dict[str, Any]) -> int | None:
    if "target_id" in arguments:
        return _optional_int(arguments, "target_id")
    target = arguments.get("target")
    if isinstance(target, dict):
        target_id = target.get("target_id")
        if target_id is None:
            return None
        try:
            return int(target_id)
        except (TypeError, ValueError):
            return None
    return None


def _deduplicate(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _message_to_dict(message: Any) -> dict[str, Any]:
    if isinstance(message, dict):
        return message
    fields_getter = getattr(message, "get_fields_and_field_types", None)
    if callable(fields_getter):
        return {
            key: _message_value(getattr(message, key))
            for key in fields_getter()
        }
    values = getattr(message, "__dict__", {})
    return {
        key: value
        for key, value in values.items()
        if not key.startswith("_") and isinstance(value, (str, int, float, bool, list, dict, type(None)))
    }


def _message_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_message_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _message_value(item) for key, item in value.items()}
    if callable(getattr(value, "get_fields_and_field_types", None)):
        return _message_to_dict(value)
    return str(value)
