"""Perception and powerline-overview runtime handlers."""

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
    PerceptionDomainState,
    PowerlineDomainState,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog


PL_MAPPER_COMMAND_SERVICE = "/perception/pl_mapper/pl_mapper_command"
UPDATE_POWERLINE_OVERVIEW_SERVICE = "/mission/powerline_overview_provider/update_powerline_overview"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class OperationalPermission:
    allowed: bool
    reasons: list[str]


class OperationalPermissionGate:
    def __init__(self, *, mission_state_provider: Callable[[], Any], operation_state_provider: Callable[[], Any]):
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider

    def mutating_permission(self, label: str) -> OperationalPermission:
        reasons: list[str] = []
        mission = self.mission_state_provider()
        operation = self.operation_state_provider()
        if mission.latest.get("mission_active") is True or mission.mission_state == "active":
            reasons.append(f"{label} commands are disabled in Mission mode")
        if operation.latest.get("operation_active") is True or operation.active_operation_id:
            reasons.append(f"{label} commands are disabled while a custom operation action is active")
        return OperationalPermission(allowed=not reasons, reasons=reasons)


class PLMapperServiceAdapter(Protocol):
    def command(self, command: str, *, reset: bool = False) -> dict[str, Any]:
        ...


class PowerlineOverviewServiceAdapter(Protocol):
    def update(self, *, timeout_s: int) -> dict[str, Any]:
        ...


class UnavailablePLMapperServiceAdapter:
    def command(self, command: str, *, reset: bool = False) -> dict[str, Any]:
        del command, reset
        raise RuntimeError(f"PL mapper service unavailable: {PL_MAPPER_COMMAND_SERVICE}")


class RosPLMapperServiceAdapter:
    def __init__(self, *, node_provider: Callable[[], Any | None], service_name: str = PL_MAPPER_COMMAND_SERVICE):
        self.node_provider = node_provider
        self.service_name = service_name
        self._client = None

    def command(self, command: str, *, reset: bool = False) -> dict[str, Any]:
        from iii_drone_interfaces.msg import PLMapperCommand as PLMapperCommandMsg
        from iii_drone_interfaces.srv import PLMapperCommand
        import rclpy

        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is not available for PL mapper commands")
        if self._client is None:
            self._client = node.create_client(PLMapperCommand, self.service_name)
        if not self._client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"PL mapper service unavailable: {self.service_name}")
        request = PLMapperCommand.Request()
        request.pl_mapper_cmd.reset = bool(reset)
        request.pl_mapper_cmd.command = {
            "start": PLMapperCommandMsg.PL_MAPPER_CMD_START,
            "stop": PLMapperCommandMsg.PL_MAPPER_CMD_STOP,
            "pause": PLMapperCommandMsg.PL_MAPPER_CMD_PAUSE,
            "freeze": PLMapperCommandMsg.PL_MAPPER_CMD_FREEZE,
        }[command]
        future = self._client.call_async(request)
        rclpy.spin_until_future_complete(node, future, timeout_sec=2.0)
        if not future.done():
            raise TimeoutError("timed out waiting for PL mapper command response")
        response = future.result()
        success = response.pl_mapper_ack == PLMapperCommand.Response.PL_MAPPER_ACK_SUCCESS
        return {
            "success": bool(success),
            "ack": int(response.pl_mapper_ack),
            "command": command,
            "reset": bool(reset),
            "service_name": self.service_name,
        }


class UnavailablePowerlineOverviewServiceAdapter:
    def update(self, *, timeout_s: int) -> dict[str, Any]:
        del timeout_s
        raise RuntimeError(f"powerline overview service unavailable: {UPDATE_POWERLINE_OVERVIEW_SERVICE}")


class PerceptionStatusCache:
    def __init__(self):
        self._pl_mapper_state: str | None = None
        self._pl_direction_status: str | None = None
        self._hough_status: str | None = None
        self._stored_overview_status: str | None = None
        self._live_powerline_line_count: int | None = None
        self._last_perception_update_at: datetime | None = None
        self._last_powerline_update_at: datetime | None = None
        self._last_live_powerline_sample_at: datetime | None = None
        self._live_powerline_publisher_available = False

    def subscribe(self, node: Any) -> list[Any]:
        try:
            from iii_drone_interfaces.msg import Powerline, StringStamped
        except Exception:
            return []
        subscriptions = [
            node.create_subscription(StringStamped, "/perception/pl_mapper/state", self.handle_pl_mapper_state, 10),
            node.create_subscription(StringStamped, "/perception/pl_dir_computer/status", self.handle_pl_direction_status, 10),
            node.create_subscription(StringStamped, "/perception/hough_transformer/status", self.handle_hough_status, 10),
            node.create_subscription(StringStamped, "/mission/powerline_overview_provider/stored_powerline_status", self.handle_stored_overview_status, 10),
            node.create_subscription(Powerline, "/perception/pl_mapper/powerline", self.handle_live_powerline, 10),
        ]
        if hasattr(node, "create_timer"):
            subscriptions.append(node.create_timer(1.0, lambda: self.refresh_graph_state(node)))
        self.refresh_graph_state(node)
        return subscriptions

    def handle_pl_mapper_state(self, message: Any) -> None:
        self._pl_mapper_state = str(getattr(message, "data"))
        self._last_perception_update_at = _utc_now()

    def handle_pl_direction_status(self, message: Any) -> None:
        self._pl_direction_status = str(getattr(message, "data"))
        self._last_perception_update_at = _utc_now()

    def handle_hough_status(self, message: Any) -> None:
        self._hough_status = str(getattr(message, "data"))
        self._last_perception_update_at = _utc_now()

    def handle_stored_overview_status(self, message: Any) -> None:
        self._stored_overview_status = str(getattr(message, "data"))
        self._last_powerline_update_at = _utc_now()

    def handle_live_powerline(self, message: Any) -> None:
        now = _utc_now()
        self._live_powerline_line_count = len(list(getattr(message, "lines", [])))
        self._last_live_powerline_sample_at = now
        self._last_powerline_update_at = now
        self._live_powerline_publisher_available = True

    def refresh_graph_state(self, node: Any) -> None:
        count_publishers = getattr(node, "count_publishers", None)
        if count_publishers is None:
            return
        try:
            self._live_powerline_publisher_available = count_publishers("/perception/pl_mapper/powerline") > 0
        except Exception:
            return
        if self._live_powerline_publisher_available and self._live_powerline_line_count is not None:
            self._last_powerline_update_at = _utc_now()

    def perception_state(self, *, permission: OperationalPermission | None = None) -> PerceptionDomainState:
        latest = {
            "pl_mapper_state": self._pl_mapper_state or "Unknown",
            "pl_direction_status": self._pl_direction_status or "Unknown",
            "hough_status": self._hough_status or "Unknown",
            "permissions": _permission_payload(permission),
        }
        return PerceptionDomainState(
            source_label="perception_status",
            source_timestamp=self._last_perception_update_at,
            freshness=Freshness.FRESH if self._last_perception_update_at else Freshness.UNKNOWN,
            source_availability=SourceAvailability.AVAILABLE if self._last_perception_update_at else SourceAvailability.UNAVAILABLE,
            degraded_reason=None if self._last_perception_update_at else "perception status topics have not been received",
            latest=latest,
            pl_mapper_state=latest["pl_mapper_state"],
            pl_direction_status=latest["pl_direction_status"],
            hough_status=latest["hough_status"],
        )

    def powerline_state(self, *, permission: OperationalPermission | None = None) -> PowerlineDomainState:
        if self._live_powerline_publisher_available and self._live_powerline_line_count is not None:
            self._last_powerline_update_at = _utc_now()
        live_status = "available" if self._live_powerline_line_count else ("running" if self._live_powerline_publisher_available else "unknown")
        latest = {
            "stored_overview_status": self._stored_overview_status or "Unknown",
            "live_powerline_line_count": self._live_powerline_line_count,
            "live_perception_status": live_status,
            "live_powerline_publisher_available": self._live_powerline_publisher_available,
            "last_live_powerline_sample_at": self._last_live_powerline_sample_at.isoformat()
            if self._last_live_powerline_sample_at
            else None,
            "permissions": _permission_payload(permission),
        }
        return PowerlineDomainState(
            source_label="powerline_status",
            source_timestamp=self._last_powerline_update_at,
            freshness=Freshness.FRESH if self._last_powerline_update_at else Freshness.UNKNOWN,
            source_availability=SourceAvailability.AVAILABLE if self._last_powerline_update_at else SourceAvailability.UNAVAILABLE,
            degraded_reason=None if self._last_powerline_update_at else "powerline status topics have not been received",
            latest=latest,
            stored_overview_status=latest["stored_overview_status"],
            live_perception_status=live_status,
        )


def _permission_payload(permission: OperationalPermission | None) -> dict[str, Any]:
    return {
        "mutating_commands_allowed": permission.allowed if permission else None,
        "mutation_rejections": permission.reasons if permission else [],
    }


PL_MAPPER_COMMANDS = {
    CommandId.PERCEPTION_PL_MAPPER_START.value: "start",
    CommandId.PERCEPTION_PL_MAPPER_STOP.value: "stop",
    CommandId.PERCEPTION_PL_MAPPER_FREEZE.value: "freeze",
    CommandId.PERCEPTION_PL_MAPPER_PAUSE.value: "pause",
}


class PerceptionCommandHandlers:
    def __init__(
        self,
        *,
        status_cache: PerceptionStatusCache,
        permission_gate: OperationalPermissionGate,
        pl_mapper_service: PLMapperServiceAdapter,
        overview_service: PowerlineOverviewServiceAdapter,
        event_log: RuntimeEventLog,
    ):
        self.status_cache = status_cache
        self.permission_gate = permission_gate
        self.pl_mapper_service = pl_mapper_service
        self.overview_service = overview_service
        self.event_log = event_log

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in PL_MAPPER_COMMANDS:
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.MUTATING,
                transport="ros_service",
                summary=f"PL mapper {PL_MAPPER_COMMANDS[command_id]}",
            )
        registry.register_action(
            CommandId.POWERLINE_OVERVIEW_UPDATE.value,
            self.handle,
            permission=HandlerPermission.MUTATING,
            transport="ros_service",
            summary="Update stored powerline overview",
        )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        permission = self.permission_gate.mutating_permission("perception")
        if not permission.allowed:
            return self._reject(request, "; ".join(permission.reasons), ErrorCode.FORBIDDEN)
        try:
            if request.command_id in PL_MAPPER_COMMANDS:
                result = self.pl_mapper_service.command(
                    PL_MAPPER_COMMANDS[request.command_id],
                    reset=bool(request.parameters.get("reset", False)),
                )
            elif request.command_id == CommandId.POWERLINE_OVERVIEW_UPDATE.value:
                result = self.overview_service.update(timeout_s=int(request.parameters.get("timeout_s", 5)))
            else:
                return self._reject(request, "unsupported perception command", ErrorCode.HANDLER_UNAVAILABLE)
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE)
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=bool(result.get("success", False)),
            started=False,
            result={
                "result": result,
                "perception": self.status_cache.perception_state(permission=permission).model_dump(mode="json"),
                "powerline": self.status_cache.powerline_state(permission=permission).model_dump(mode="json"),
            },
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


def register_perception_command_handlers(
    registry: DispatchRegistry,
    *,
    status_cache: PerceptionStatusCache,
    permission_gate: OperationalPermissionGate,
    pl_mapper_service: PLMapperServiceAdapter,
    overview_service: PowerlineOverviewServiceAdapter,
    event_log: RuntimeEventLog,
) -> PerceptionCommandHandlers:
    handlers = PerceptionCommandHandlers(
        status_cache=status_cache,
        permission_gate=permission_gate,
        pl_mapper_service=pl_mapper_service,
        overview_service=overview_service,
        event_log=event_log,
    )
    handlers.register(registry)
    return handlers
