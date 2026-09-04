"""Daemon-backed runtime-control command handlers."""

from __future__ import annotations

from typing import Any

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRequest,
    EventSource,
    HandlerPermission,
)

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
    CommandId.RUNTIME_SYSTEM_START.value,
    CommandId.RUNTIME_START.value,
    CommandId.RUNTIME_STOP.value,
    CommandId.RUNTIME_RESTART.value,
    CommandId.RUNTIME_PARAMETER_COLD_RESTART.value,
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
    def __init__(
        self,
        *,
        daemon_client: Any,
        event_log: RuntimeEventLog,
        mutation_gate: RuntimeMutationGate | None = None,
        configuration_controller: Any | None = None,
    ):
        self.daemon_client = daemon_client
        self.event_log = event_log
        self.mutation_gate = mutation_gate
        self.configuration_controller = configuration_controller

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in sorted(
            RUNTIME_READ_ONLY_COMMANDS | RUNTIME_MUTATING_COMMANDS
        ):
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
            rejection_reason = (
                self.mutation_gate.rejection_reason(request.command_id)
                if self.mutation_gate
                else None
            )
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
            result = self._execute(
                request.command_id, request.parameters, request_id=request.request_id
            )
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

        if isinstance(result, dict) and result.get("success") is False:
            reason = str(
                result.get("error")
                or result.get("degraded_reason")
                or f"{request.command_id} did not complete successfully"
            )
            self.event_log.record_command_decision(
                command_id=request.command_id,
                request_id=request.request_id,
                accepted=False,
                reason=reason,
                source=EventSource.RUNTIME,
                client_label=request.client_label,
                mutating=mutating,
            )
            return ActionStartResponse(
                request_id=request.request_id,
                command_id=request.command_id,
                accepted=False,
                started=False,
                message=reason,
                result={"permission": permission, "daemon": result},
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

    def _execute(
        self, command_id: str, parameters: dict[str, Any], *, request_id: str
    ) -> Any:
        if command_id == CommandId.RUNTIME_BOOT.value:
            return self.daemon_client.boot(parameters.get("profile", "sim"))
        if command_id == CommandId.RUNTIME_SYSTEM_START.value:
            return self._system_start(
                request_id=request_id, profile=str(parameters.get("profile", "sim"))
            )
        if command_id == CommandId.RUNTIME_START.value:
            return self._start(
                activate=bool(parameters.get("activate", True)),
                select_nodes=list(parameters.get("select_nodes", [])),
                include_dependencies=bool(
                    parameters.get("include_dependencies", False)
                ),
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
        if command_id == CommandId.RUNTIME_PARAMETER_COLD_RESTART.value:
            return self._parameter_cold_restart()
        if command_id == CommandId.RUNTIME_SHUTDOWN.value:
            return self.daemon_client.shutdown(
                select_nodes=parameters.get("select_nodes", []),
                include_dependencies=parameters.get("include_dependencies", False),
            )
        if command_id == CommandId.RUNTIME_STATUS.value:
            return self.daemon_client.status()
        if command_id == CommandId.RUNTIME_LIST_ENTITIES.value:
            status = self.daemon_client.status()
            managed_nodes = (
                status.get("managed_nodes") or self.daemon_client.list_nodes()
            )
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

    def _system_start(self, *, request_id: str, profile: str) -> dict[str, Any]:
        stages: list[dict[str, Any]] = []

        def record(
            stage: str, status: str, detail: str, result: dict[str, Any] | None = None
        ) -> None:
            stage_result = {
                "stage": stage,
                "status": status,
                "detail": detail,
                "result": result or {},
            }
            stages.append(stage_result)
            self.event_log.record_command_progress(
                command_id=CommandId.RUNTIME_SYSTEM_START.value,
                request_id=request_id,
                stage=stage,
                status=status,
                detail=detail,
                result=result,
            )

        initial = self.daemon_client.status()
        record("status", "complete", "Read current supervised system state.", initial)
        if not initial.get("booted"):
            boot = self.daemon_client.boot(profile)
            record(
                "boot",
                "complete",
                f"Booted canonical {profile!r} system profile.",
                boot,
            )
        else:
            record("boot", "skipped", "System was already booted.")

        after_boot = self.daemon_client.status()
        if not after_boot.get("active"):
            started = self._start(
                activate=True,
                select_nodes=[],
                include_dependencies=False,
            )
            record(
                "start",
                "complete",
                "Started services and activated managed lifecycle nodes.",
                started,
            )
        else:
            record("start", "skipped", "Managed system was already active.")

        final = self.daemon_client.status()
        services = final.get("services") or {}
        unready_services = sorted(
            service_id
            for service_id, service in services.items()
            if isinstance(service, dict) and service.get("ready") is False
        )
        degraded_reason = final.get("degraded_reason")
        ready = bool(
            final.get("booted")
            and final.get("active")
            and not unready_services
            and not degraded_reason
        )
        final_status = "complete" if ready else "degraded"
        detail = (
            "Aircraft system is ready."
            if ready
            else "Aircraft system started with degraded or incomplete readiness."
        )
        record("readiness", final_status, detail, final)
        return {
            "success": ready,
            "ready": ready,
            "profile": final.get("profile") or profile,
            "unready_services": unready_services,
            "degraded_reason": degraded_reason,
            "stages": stages,
            "status": final,
        }

    def _start(
        self,
        *,
        activate: bool,
        select_nodes: list[str],
        include_dependencies: bool,
    ) -> dict[str, Any]:
        started = self.daemon_client.start(
            activate=activate,
            select_nodes=select_nodes,
            include_dependencies=include_dependencies,
        )
        if not activate or select_nodes or self.configuration_controller is None:
            return started
        try:
            manifest = self.configuration_controller.manifest()
        except Exception:
            # Configuration is an optional runtime surface. Only a successfully
            # read, explicitly pending state creates the confirmation boundary.
            return started
        if (
            not manifest.status.configuration_server_available
            or not manifest.status.pending_restart
        ):
            return started
        try:
            confirmation = (
                self.configuration_controller.activate_pending_boot_parameters()
            )
            refreshed = self.configuration_controller.manifest()
            if refreshed.status.pending_restart:
                raise RuntimeError(
                    "whole-graph start completed but boot parameters remain pending"
                )
        except Exception:
            self.daemon_client.stop(
                cleanup=True,
                select_nodes=[],
                include_dependencies=False,
            )
            raise
        return {
            **started,
            "configuration_boot_confirmation": confirmation,
        }

    def _parameter_cold_restart(self) -> dict[str, Any]:
        if self.configuration_controller is None:
            raise RuntimeError("configuration controller is unavailable")
        manifest = self.configuration_controller.manifest()
        if not manifest.status.configuration_server_available:
            raise RuntimeError("configuration server is unavailable")
        if not manifest.status.pending_restart:
            raise RuntimeError("no constant parameter changes are pending")
        permission = self.configuration_controller.parameter_cold_restart_permission()
        if not permission.allowed:
            raise RuntimeError("; ".join(permission.reasons))
        status = self.daemon_client.status()
        managed = status.get("managed_nodes") or self.daemon_client.list_nodes()
        node_ids = list(managed.keys()) if isinstance(managed, dict) else list(managed)
        restart_nodes = sorted(
            node_id
            for node_id in node_ids
            if str(node_id).strip("/").split("/")[-1] != "configuration_server"
        )
        if not restart_nodes:
            raise RuntimeError(
                "no managed nodes are available for parameter cold restart"
            )

        stopped = self.daemon_client.stop(
            cleanup=True,
            select_nodes=restart_nodes,
            include_dependencies=False,
        )
        started = self.daemon_client.start(
            activate=True,
            select_nodes=restart_nodes,
            include_dependencies=False,
        )
        try:
            activated = self.configuration_controller.activate_pending_boot_parameters()
        except Exception:
            self.daemon_client.stop(
                cleanup=True,
                select_nodes=restart_nodes,
                include_dependencies=False,
            )
            raise
        manifest = self.configuration_controller.manifest()
        if manifest.status.pending_restart:
            raise RuntimeError(
                "managed nodes restarted but boot parameters remain pending"
            )
        return {
            "success": True,
            "excluded_nodes": ["configuration_server"],
            "restarted_nodes": restart_nodes,
            "stop": stopped,
            "activation": activated,
            "start": started,
            "confirmed_pending_restart": False,
        }


def register_runtime_command_handlers(
    registry: DispatchRegistry,
    *,
    daemon_client: Any,
    event_log: RuntimeEventLog,
    mutation_gate: RuntimeMutationGate | None = None,
    configuration_controller: Any | None = None,
) -> RuntimeCommandHandlers:
    handlers = RuntimeCommandHandlers(
        daemon_client=daemon_client,
        event_log=event_log,
        mutation_gate=mutation_gate,
        configuration_controller=configuration_controller,
    )
    handlers.register(registry)
    return handlers
