"""Explicit typed dispatch registry for runtime API commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from iii_drone_contracts import (
    ActionStartResponse,
    ApiError,
    CommandRejection,
    CommandRequest,
    CommandResultMessage,
    ErrorCode,
    HandlerPermission,
    ServiceCallRequest,
    ServiceCallResponse,
)


ActionHandler = Callable[[CommandRequest], ActionStartResponse]
ServiceHandler = Callable[[ServiceCallRequest], ServiceCallResponse]


@dataclass(frozen=True)
class HandlerMetadata:
    permission: HandlerPermission
    transport: str = "runtime_api"
    summary: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "permission": self.permission.value,
            "transport": self.transport,
            "summary": self.summary,
        }


@dataclass(frozen=True)
class RegisteredActionHandler:
    handler: ActionHandler
    metadata: HandlerMetadata


@dataclass(frozen=True)
class RegisteredServiceHandler:
    handler: ServiceHandler
    metadata: HandlerMetadata


@dataclass
class DispatchRegistry:
    action_handlers: dict[str, RegisteredActionHandler]
    service_handlers: dict[tuple[str, str], RegisteredServiceHandler]

    @classmethod
    def empty(cls) -> "DispatchRegistry":
        return cls(action_handlers={}, service_handlers={})

    def register_action(
        self,
        command_id: str,
        handler: ActionHandler,
        *,
        permission: HandlerPermission = HandlerPermission.MUTATING,
        transport: str = "runtime_api",
        summary: str | None = None,
    ) -> None:
        self.action_handlers[command_id] = RegisteredActionHandler(
            handler=handler,
            metadata=HandlerMetadata(permission=permission, transport=transport, summary=summary),
        )

    def register_service(
        self,
        service_type: str,
        service_name: str,
        handler: ServiceHandler,
        *,
        permission: HandlerPermission = HandlerPermission.MUTATING,
        transport: str = "runtime_api",
        summary: str | None = None,
    ) -> None:
        self.service_handlers[(service_type, service_name)] = RegisteredServiceHandler(
            handler=handler,
            metadata=HandlerMetadata(permission=permission, transport=transport, summary=summary),
        )

    def action_permission(self, command_id: str) -> HandlerPermission | None:
        registered = self.action_handlers.get(command_id)
        return registered.metadata.permission if registered else None

    def action_metadata(self) -> dict[str, dict[str, str | None]]:
        return {
            command_id: registered.metadata.as_dict()
            for command_id, registered in sorted(self.action_handlers.items())
        }

    def service_metadata(self) -> dict[str, dict[str, str | None]]:
        return {
            f"{service_type}/{service_name}": registered.metadata.as_dict()
            for (service_type, service_name), registered in sorted(self.service_handlers.items())
        }

    def metadata(self) -> dict[str, dict[str, dict[str, str | None]]]:
        return {"actions": self.action_metadata(), "services": self.service_metadata()}

    def start_action(self, request: CommandRequest) -> tuple[ActionStartResponse, CommandResultMessage | None]:
        registered = self.action_handlers.get(request.command_id)
        if registered is None:
            rejection = CommandRejection(
                code=ErrorCode.HANDLER_UNAVAILABLE,
                message=f"unregistered action command: {request.command_id}",
                request_id=request.request_id,
                command_id=request.command_id,
            )
            return (
                ActionStartResponse(
                    request_id=request.request_id,
                    command_id=request.command_id,
                    accepted=False,
                    started=False,
                    rejection=rejection,
                ),
                None,
            )

        response = registered.handler(request)
        result = None
        if response.accepted:
            result = CommandResultMessage(
                request_id=response.request_id,
                command_id=response.command_id,
                status="accepted",
                action_id=response.action_id,
                result=response.result,
            )
        return response, result

    def call_service(self, request: ServiceCallRequest) -> ServiceCallResponse:
        registered = self.service_handlers.get((request.service_type, request.service_name))
        if registered is None:
            return ServiceCallResponse(
                request_id=request.request_id,
                service_type=request.service_type,
                service_name=request.service_name,
                ok=False,
                error=ApiError(
                    code=ErrorCode.HANDLER_UNAVAILABLE,
                    message=f"unregistered service handler: {request.service_type}/{request.service_name}",
                    request_id=request.request_id,
                ),
            )
        return registered.handler(request)
