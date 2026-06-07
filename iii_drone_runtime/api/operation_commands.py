"""Runtime API command handlers for CustomOperation actions."""

from __future__ import annotations

from typing import Any

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRejection,
    CommandRequest,
    ErrorCode,
    EventSource,
    HandlerPermission,
)

from .custom_operations import NonblockingCustomOperationClient, OperationRecord
from .dispatch import DispatchRegistry
from .events import RuntimeEventLog


OPERATION_START_COMMANDS = {
    CommandId.CUSTOM_OPERATION_FLY_TO_POSITION_START.value: "fly_to_position",
    CommandId.CUSTOM_OPERATION_CABLE_AWARE_FLY_TO_POSITION_START.value: "cable_aware_fly_to_position",
    CommandId.CUSTOM_OPERATION_FLY_TO_OBJECT_START.value: "fly_to_object",
    CommandId.CUSTOM_OPERATION_CABLE_LANDING_START.value: "cable_landing",
    CommandId.CUSTOM_OPERATION_CABLE_TAKEOFF_START.value: "cable_takeoff",
    CommandId.CUSTOM_OPERATION_HOVER_START.value: "hover",
    CommandId.CUSTOM_OPERATION_HOVER_BY_OBJECT_START.value: "hover_by_object",
    CommandId.CUSTOM_OPERATION_HOVER_ON_CABLE_START.value: "hover_on_cable",
}


class CustomOperationCommandHandlers:
    def __init__(
        self,
        *,
        client: NonblockingCustomOperationClient,
        event_log: RuntimeEventLog,
    ):
        self.client = client
        self.event_log = event_log

    def register(self, registry: DispatchRegistry) -> None:
        registry.register_action(
            CommandId.CUSTOM_OPERATION_VALIDATE.value,
            self.handle,
            permission=HandlerPermission.READ_ONLY,
            transport="runtime_custom_operation_client",
            summary="Validate a custom operation request without starting it",
        )
        registry.register_action(
            CommandId.CUSTOM_OPERATION_CANCEL.value,
            self.handle,
            permission=HandlerPermission.MUTATING,
            transport="runtime_custom_operation_client",
            summary="Cancel the active custom operation",
        )
        for command_id in OPERATION_START_COMMANDS:
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.FLIGHT_CRITICAL,
                transport="runtime_custom_operation_client",
                summary=f"Start custom operation {OPERATION_START_COMMANDS[command_id]}",
            )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        self.event_log.record_command_request(
            command_id=request.command_id,
            request_id=request.request_id,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=request.command_id != CommandId.CUSTOM_OPERATION_VALIDATE.value,
        )
        if request.command_id == CommandId.CUSTOM_OPERATION_VALIDATE.value:
            return self._handle_validate(request)
        if request.command_id == CommandId.CUSTOM_OPERATION_CANCEL.value:
            return self._handle_cancel(request)
        if request.command_id in OPERATION_START_COMMANDS:
            return self._handle_start(request)
        return self._reject(request, "unsupported custom operation command", ErrorCode.HANDLER_UNAVAILABLE)

    def _handle_validate(self, request: CommandRequest) -> ActionStartResponse:
        operation = str(request.parameters.get("operation", ""))
        arguments = request.parameters.get("arguments", {})
        if not isinstance(arguments, dict):
            return self._reject(request, "custom operation arguments must be an object", ErrorCode.INVALID_REQUEST)
        validation = self.client.validate(operation, arguments)
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
            result={
                "validation": {
                    "ok": validation.ok,
                    "operation": validation.operation,
                    "arguments": validation.arguments,
                    "rejection_reasons": validation.rejection_reasons,
                }
            },
        )

    def _handle_start(self, request: CommandRequest) -> ActionStartResponse:
        if request.parameters.get("hold_confirmed") is not True:
            return self._reject(
                request,
                "custom operation starts require press-and-hold confirmation",
                ErrorCode.FORBIDDEN,
            )
        operation = OPERATION_START_COMMANDS[request.command_id]
        arguments = _operation_arguments(request.parameters)
        record = self.client.start(operation, arguments, request_id=request.request_id)
        if not record.accepted:
            return self._reject(
                request,
                "; ".join(record.rejection_reasons) or "custom operation rejected",
                ErrorCode.DEGRADED_STATE,
                result={"operation": record.as_dict()},
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
            action_id=record.operation_id,
            result={"operation": record.as_dict()},
        )

    def _handle_cancel(self, request: CommandRequest) -> ActionStartResponse:
        record = self.client.cancel_active()
        if record is None:
            return ActionStartResponse(
                request_id=request.request_id,
                command_id=request.command_id,
                accepted=True,
                started=False,
                result={"cancelled": False, "reason": "no active custom operation"},
            )
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
            action_id=record.operation_id,
            result={"cancelled": True, "operation": record.as_dict()},
        )

    def _reject(
        self,
        request: CommandRequest,
        reason: str,
        code: ErrorCode,
        *,
        result: dict[str, Any] | None = None,
    ) -> ActionStartResponse:
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
                degraded_reason=reason if code == ErrorCode.DEGRADED_STATE else None,
                retryable=code != ErrorCode.FORBIDDEN,
            ),
            result=result,
        )


def _operation_arguments(parameters: dict[str, Any]) -> dict[str, Any]:
    arguments = parameters.get("arguments")
    if isinstance(arguments, dict):
        return dict(arguments)
    return {
        key: value
        for key, value in parameters.items()
        if key not in {"hold_confirmed", "client_label", "operation"}
    }


def operation_record_payload(record: OperationRecord | None) -> dict[str, Any] | None:
    return None if record is None else record.as_dict()


def register_custom_operation_command_handlers(
    registry: DispatchRegistry,
    *,
    client: NonblockingCustomOperationClient,
    event_log: RuntimeEventLog,
) -> CustomOperationCommandHandlers:
    handlers = CustomOperationCommandHandlers(client=client, event_log=event_log)
    handlers.register(registry)
    return handlers
