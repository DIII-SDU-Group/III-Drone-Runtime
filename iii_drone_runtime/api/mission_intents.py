"""Typed operator intent commands for mission-owned recharge behavior."""

from __future__ import annotations

from typing import Any, Callable, Protocol

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRejection,
    CommandRequest,
    ErrorCode,
    EventSource,
    HandlerPermission,
)

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog
from ..ros_services import create_reentrant_client, wait_for_service_response


INTENT_COMMANDS = {
    CommandId.MISSION_RECHARGE_NOW.value: (
        "/mission/inspection_demo/trigger_recharge_now",
        {"inspection_demo"},
        True,
    ),
    CommandId.MISSION_STAY_ON_CABLE.value: (
        "/mission/cable_charging/stay_on_cable",
        {"cable_charging"},
        None,
    ),
    CommandId.MISSION_LEAVE_CABLE_NOW.value: (
        "/mission/cable_charging/interrupt_recharging_now",
        {"cable_charging"},
        True,
    ),
}


class MissionIntentServiceAdapter(Protocol):
    def set_intent(self, service_name: str, value: bool) -> dict[str, Any]:
        ...


class RosMissionIntentServiceAdapter:
    def __init__(self, *, node_provider: Callable[[], Any | None]):
        self.node_provider = node_provider
        self._clients: dict[str, Any] = {}

    def set_intent(self, service_name: str, value: bool) -> dict[str, Any]:
        from std_srvs.srv import SetBool

        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is unavailable")
        client = self._clients.get(service_name)
        if client is None:
            client = create_reentrant_client(node, SetBool, service_name)
            self._clients[service_name] = client
        if not client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"mission intent service unavailable: {service_name}")
        request = SetBool.Request()
        request.data = value
        response = wait_for_service_response(
            client,
            request,
            timeout_sec=2.0,
            label=f"mission intent service: {service_name}",
        )
        return {
            "success": bool(response.success),
            "message": str(response.message),
            "service_name": service_name,
            "value": value,
            "lifecycle": "acknowledged_onboard" if response.success else "rejected",
        }


class MissionIntentCommandHandlers:
    def __init__(
        self,
        *,
        mission_state_provider: Callable[[], Any],
        service: MissionIntentServiceAdapter,
        event_log: RuntimeEventLog,
    ):
        self.mission_state_provider = mission_state_provider
        self.service = service
        self.event_log = event_log

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in INTENT_COMMANDS:
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.MUTATING,
                transport="ros_service",
                summary=f"Mission intent {command_id}",
            )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        service_name, valid_modes, fixed_value = INTENT_COMMANDS[request.command_id]
        mission = self.mission_state_provider()
        active_mode = next((mode.mode_key for mode in mission.modes if mode.active), "")
        if mission.freshness != "fresh":
            return self._reject(request, "mission phase state is stale or unavailable", ErrorCode.STALE_STATE)
        if active_mode not in valid_modes:
            return self._reject(
                request,
                f"{request.command_id} is not valid during mission phase '{active_mode or 'none'}'",
                ErrorCode.FORBIDDEN,
            )
        value = fixed_value if fixed_value is not None else bool(request.parameters.get("value", True))
        try:
            result = self.service.set_intent(service_name, value)
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE)
        if not result.get("success", False):
            return self._reject(request, result.get("message", "onboard intent rejected"), ErrorCode.FORBIDDEN)
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
            result={"intent": result},
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
        )


def register_mission_intent_command_handlers(
    registry: DispatchRegistry,
    *,
    mission_state_provider: Callable[[], Any],
    service: MissionIntentServiceAdapter,
    event_log: RuntimeEventLog,
) -> MissionIntentCommandHandlers:
    handlers = MissionIntentCommandHandlers(
        mission_state_provider=mission_state_provider,
        service=service,
        event_log=event_log,
    )
    handlers.register(registry)
    return handlers
