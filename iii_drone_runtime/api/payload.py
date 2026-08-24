"""Payload, charger, and gripper runtime handlers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRejection,
    CommandRequest,
    ErrorCode,
    EventSource,
    HandlerPermission,
    PayloadDomainState,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog
from ..ros_services import create_reentrant_client, wait_for_service_response


GRIPPER_COMMAND_SERVICE = "/payload/charger_gripper/gripper_command"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class GripperServiceAdapter(Protocol):
    def command(self, command: str) -> dict[str, Any]:
        ...


class UnavailableGripperServiceAdapter:
    def command(self, command: str) -> dict[str, Any]:
        del command
        raise RuntimeError(f"gripper service unavailable: {GRIPPER_COMMAND_SERVICE}")


class RosGripperServiceAdapter:
    def __init__(self, *, node_provider: Callable[[], Any | None], service_name: str = GRIPPER_COMMAND_SERVICE):
        self.node_provider = node_provider
        self.service_name = service_name
        self._client = None
        self._client_node = None

    def command(self, command: str) -> dict[str, Any]:
        from iii_drone_interfaces.srv import GripperCommand
        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is not available for gripper commands")
        if node is not self._client_node:
            self._client = None
            self._client_node = node
        if self._client is None:
            self._client = create_reentrant_client(node, GripperCommand, self.service_name)
        if not self._client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"gripper service unavailable: {self.service_name}")
        request = GripperCommand.Request()
        request.gripper_command = (
            GripperCommand.Request.GRIPPER_COMMAND_OPEN
            if command == "open"
            else GripperCommand.Request.GRIPPER_COMMAND_CLOSE
        )
        response = wait_for_service_response(
            self._client,
            request,
            timeout_sec=2.0,
            label="gripper command response",
        )
        success = response.gripper_command_response == GripperCommand.Response.GRIPPER_COMMAND_RESPONSE_SUCCESS
        return {
            "success": bool(success),
            "response_code": int(response.gripper_command_response),
            "service_name": self.service_name,
        }


@dataclass(frozen=True)
class PayloadPermission:
    allowed: bool
    reasons: list[str]


class PayloadPermissionGate:
    def __init__(
        self,
        *,
        mission_state_provider: Callable[[], Any],
        operation_state_provider: Callable[[], Any],
    ):
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider

    def gripper_permission(self) -> PayloadPermission:
        reasons: list[str] = []
        mission = self.mission_state_provider()
        operation = self.operation_state_provider()
        if mission.latest.get("mission_active") is True or mission.mission_state == "active":
            reasons.append("gripper commands are disabled in Mission mode")
        if operation.latest.get("operation_active") is True or operation.active_operation_id:
            reasons.append("gripper commands are disabled while a custom operation action is active")
        return PayloadPermission(allowed=not reasons, reasons=reasons)


class PayloadStatusCache:
    def __init__(self):
        self._battery_voltage: float | None = None
        self._charging_power: float | None = None
        self._charger_operating_mode: int | None = None
        self._charger_status: int | None = None
        self._gripper_status: int | None = None
        self._last_update_at: datetime | None = None

    def subscribe(self, node: Any) -> list[Any]:
        try:
            from std_msgs.msg import Float32
            from iii_drone_interfaces.msg import ChargerOperatingMode, ChargerStatus, GripperStatus
        except Exception:
            return []
        return [
            node.create_subscription(Float32, "/payload/charger_gripper/battery_voltage", self.handle_battery_voltage, 10),
            node.create_subscription(Float32, "/payload/charger_gripper/charging_power", self.handle_charging_power, 10),
            node.create_subscription(ChargerOperatingMode, "/payload/charger_gripper/charger_operating_mode", self.handle_charger_operating_mode, 10),
            node.create_subscription(ChargerStatus, "/payload/charger_gripper/charger_status", self.handle_charger_status, 10),
            node.create_subscription(GripperStatus, "/payload/charger_gripper/gripper_status", self.handle_gripper_status, 10),
        ]

    def handle_battery_voltage(self, message: Any) -> None:
        self._battery_voltage = float(getattr(message, "data"))
        self._last_update_at = _utc_now()

    def handle_charging_power(self, message: Any) -> None:
        self._charging_power = float(getattr(message, "data"))
        self._last_update_at = _utc_now()

    def handle_charger_operating_mode(self, message: Any) -> None:
        self._charger_operating_mode = int(getattr(message, "operating_mode"))
        self._last_update_at = _utc_now()

    def handle_charger_status(self, message: Any) -> None:
        self._charger_status = int(getattr(message, "charger_status"))
        self._last_update_at = _utc_now()

    def handle_gripper_status(self, message: Any) -> None:
        self._gripper_status = int(getattr(message, "gripper_status"))
        self._last_update_at = _utc_now()

    def state(self, *, permission: PayloadPermission | None = None) -> PayloadDomainState:
        latest = {
            "battery_voltage": self._battery_voltage,
            "charging_power": self._charging_power,
            "charger_operating_mode": self._charger_operating_mode,
            "charger_operating_mode_label": self._operating_mode_label(self._charger_operating_mode),
            "charger_status": self._charger_status,
            "charger_status_label": self._charger_status_label(self._charger_status),
            "gripper_status": self._gripper_status,
            "gripper_status_label": self._gripper_status_label(self._gripper_status),
            "permissions": {
                "gripper_commands_allowed": permission.allowed if permission else None,
                "gripper_command_rejections": permission.reasons if permission else [],
            },
        }
        return PayloadDomainState(
            source_label="payload_status",
            source_timestamp=self._last_update_at,
            freshness=Freshness.FRESH if self._last_update_at else Freshness.UNKNOWN,
            source_availability=SourceAvailability.AVAILABLE if self._last_update_at else SourceAvailability.UNAVAILABLE,
            degraded_reason=None if self._last_update_at else "payload status topics have not been received",
            latest=latest,
            gripper_status=latest["gripper_status_label"],
            charger_status=latest["charger_status_label"],
            battery_voltage=self._battery_voltage,
            charging_power=self._charging_power,
        )

    def apply_gripper_command_result(self, command: str, result: dict[str, Any]) -> None:
        if result.get("success") is True:
            self._gripper_status = 0 if command == "open" else 1
            self._last_update_at = _utc_now()

    def _gripper_status_label(self, value: int | None) -> str:
        if value == 0:
            return "open"
        if value == 1:
            return "closed"
        return "unknown"

    def _charger_status_label(self, value: int | None) -> str:
        if value == 0:
            return "disabled"
        if value == 1:
            return "charging"
        if value == 2:
            return "fully_charged"
        return "unknown"

    def _operating_mode_label(self, value: int | None) -> str:
        if value == 0:
            return "open"
        if value is None:
            return "unknown"
        return f"mode_{value}"


class PayloadCommandHandlers:
    def __init__(
        self,
        *,
        status_cache: PayloadStatusCache,
        permission_gate: PayloadPermissionGate,
        gripper_service: GripperServiceAdapter,
        event_log: RuntimeEventLog,
    ):
        self.status_cache = status_cache
        self.permission_gate = permission_gate
        self.gripper_service = gripper_service
        self.event_log = event_log

    def register(self, registry: DispatchRegistry) -> None:
        registry.register_action(
            CommandId.PAYLOAD_GRIPPER_OPEN.value,
            self.handle,
            permission=HandlerPermission.MUTATING,
            transport="ros_service",
            summary="Open payload gripper",
        )
        registry.register_action(
            CommandId.PAYLOAD_GRIPPER_CLOSE.value,
            self.handle,
            permission=HandlerPermission.MUTATING,
            transport="ros_service",
            summary="Close payload gripper",
        )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        command = "open" if request.command_id == CommandId.PAYLOAD_GRIPPER_OPEN.value else "close"
        permission = self.permission_gate.gripper_permission()
        if not permission.allowed:
            return self._reject(request, "; ".join(permission.reasons), ErrorCode.FORBIDDEN)
        try:
            result = self.gripper_service.command(command)
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE)
        self.status_cache.apply_gripper_command_result(command, result)
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=bool(result.get("success", False)),
            started=False,
            result={"gripper": result, "payload": self.status_cache.state(permission=permission).model_dump(mode="json")},
        )

    def _reject(self, request: CommandRequest, reason: str, code: ErrorCode) -> ActionStartResponse:
        self.event_log.record_command_decision(
            command_id=request.command_id,
            request_id=request.request_id,
            accepted=False,
            reason=reason,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=True,
        )
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
                retryable=code != ErrorCode.FORBIDDEN,
            ),
            result={"payload": self.status_cache.state(permission=self.permission_gate.gripper_permission()).model_dump(mode="json")},
        )


def register_payload_command_handlers(
    registry: DispatchRegistry,
    *,
    status_cache: PayloadStatusCache,
    permission_gate: PayloadPermissionGate,
    gripper_service: GripperServiceAdapter,
    event_log: RuntimeEventLog,
) -> PayloadCommandHandlers:
    handlers = PayloadCommandHandlers(
        status_cache=status_cache,
        permission_gate=permission_gate,
        gripper_service=gripper_service,
        event_log=event_log,
    )
    handlers.register(registry)
    return handlers
