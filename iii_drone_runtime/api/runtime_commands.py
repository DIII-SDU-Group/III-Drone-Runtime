"""Daemon-backed runtime-control command handlers."""

from __future__ import annotations

from typing import Any

from iii_drone_contracts import ActionStartResponse, CommandId, CommandRequest, EventSource, HandlerPermission

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog
from .safety import RuntimeMutationGate


RUNTIME_READ_ONLY_COMMANDS = {
    CommandId.RUNTIME_STATUS.value,
    CommandId.RUNTIME_LIST_ENTITIES.value,
    CommandId.RUNTIME_LIST_SERVICES.value,
}

RUNTIME_MUTATING_COMMANDS = {
    CommandId.RUNTIME_BOOT.value,
    CommandId.RUNTIME_START.value,
    CommandId.RUNTIME_STOP.value,
    CommandId.RUNTIME_RESTART.value,
    CommandId.RUNTIME_SHUTDOWN.value,
    CommandId.RUNTIME_SERVICE_START.value,
    CommandId.RUNTIME_SERVICE_STOP.value,
    CommandId.RUNTIME_SERVICE_RESTART.value,
}


def runtime_command_permission(command_id: str) -> str:
    if command_id in RUNTIME_READ_ONLY_COMMANDS:
        return "read_only"
    if command_id in RUNTIME_MUTATING_COMMANDS:
        return "mutating"
    return "unknown"


def _handler_permission(command_id: str) -> HandlerPermission:
    if command_id in RUNTIME_READ_ONLY_COMMANDS:
        return HandlerPermission.READ_ONLY
    if command_id in RUNTIME_MUTATING_COMMANDS:
        return HandlerPermission.RUNTIME_MUTATION
    return HandlerPermission.MUTATING


class RuntimeCommandHandlers:
    def __init__(self, *, daemon_client: Any, event_log: RuntimeEventLog, mutation_gate: RuntimeMutationGate | None = None):
        self.daemon_client = daemon_client
        self.event_log = event_log
        self.mutation_gate = mutation_gate

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in sorted(RUNTIME_READ_ONLY_COMMANDS | RUNTIME_MUTATING_COMMANDS):
            registry.register_action(
                command_id,
                self.handle,
                permission=_handler_permission(command_id),
                transport="runtime_daemon",
                summary=f"Runtime daemon command {command_id}",
            )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        permission = runtime_command_permission(request.command_id)
        mutating = permission == "mutating"
        self.event_log.record_command_request(
            command_id=request.command_id,
            request_id=request.request_id,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=mutating,
        )
        if mutating:
            rejection_reason = self.mutation_gate.rejection_reason(request.command_id) if self.mutation_gate else None
            if rejection_reason is not None:
                self.event_log.record_command_decision(
                    command_id=request.command_id,
                    request_id=request.request_id,
                    accepted=False,
                    reason=rejection_reason,
                    source=EventSource.RUNTIME,
                    client_label=request.client_label,
                    mutating=True,
                )
                return ActionStartResponse(
                    request_id=request.request_id,
                    command_id=request.command_id,
                    accepted=False,
                    started=False,
                    message=rejection_reason,
                    result={"permission": permission},
                )

        try:
            result = self._execute(request.command_id, request.parameters)
        except Exception as exc:
            self.event_log.record_command_decision(
                command_id=request.command_id,
                request_id=request.request_id,
                accepted=False,
                reason=str(exc),
                source=EventSource.RUNTIME,
                client_label=request.client_label,
                mutating=mutating,
            )
            return ActionStartResponse(
                request_id=request.request_id,
                command_id=request.command_id,
                accepted=False,
                started=False,
                message=str(exc),
                result={"permission": permission},
            )

        if mutating:
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
            started=False,
            result={"permission": permission, "daemon": result},
        )

    def _execute(self, command_id: str, parameters: dict[str, Any]) -> Any:
        if command_id == CommandId.RUNTIME_BOOT.value:
            return self.daemon_client.boot(parameters.get("profile", "sim"))
        if command_id == CommandId.RUNTIME_START.value:
            return self.daemon_client.start(
                activate=parameters.get("activate", True),
                select_nodes=parameters.get("select_nodes", []),
                include_dependencies=parameters.get("include_dependencies", False),
            )
        if command_id == CommandId.RUNTIME_STOP.value:
            return self.daemon_client.stop(
                cleanup=parameters.get("cleanup", True),
                select_nodes=parameters.get("select_nodes", []),
                include_dependencies=parameters.get("include_dependencies", False),
            )
        if command_id == CommandId.RUNTIME_RESTART.value:
            return self.daemon_client.restart(
                cold=parameters.get("cold", False),
                select_nodes=parameters.get("select_nodes", []),
                include_dependencies=parameters.get("include_dependencies", False),
            )
        if command_id == CommandId.RUNTIME_SHUTDOWN.value:
            return self.daemon_client.shutdown(
                select_nodes=parameters.get("select_nodes", []),
                include_dependencies=parameters.get("include_dependencies", False),
            )
        if command_id == CommandId.RUNTIME_STATUS.value:
            return self.daemon_client.status()
        if command_id == CommandId.RUNTIME_LIST_ENTITIES.value:
            status = self.daemon_client.status()
            managed_nodes = status.get("managed_nodes") or self.daemon_client.list_nodes()
            return {"managed_nodes": managed_nodes}
        if command_id == CommandId.RUNTIME_LIST_SERVICES.value:
            status = self.daemon_client.status()
            services = status.get("services") or self.daemon_client.list_services()
            return {"services": services}
        if command_id == CommandId.RUNTIME_SERVICE_START.value:
            return self.daemon_client.service_start(parameters["service_id"])
        if command_id == CommandId.RUNTIME_SERVICE_STOP.value:
            return self.daemon_client.service_stop(parameters["service_id"])
        if command_id == CommandId.RUNTIME_SERVICE_RESTART.value:
            return self.daemon_client.service_restart(parameters["service_id"])
        raise ValueError(f"unsupported runtime command: {command_id}")


def register_runtime_command_handlers(
    registry: DispatchRegistry,
    *,
    daemon_client: Any,
    event_log: RuntimeEventLog,
    mutation_gate: RuntimeMutationGate | None = None,
) -> RuntimeCommandHandlers:
    handlers = RuntimeCommandHandlers(
        daemon_client=daemon_client,
        event_log=event_log,
        mutation_gate=mutation_gate,
    )
    handlers.register(registry)
    return handlers
