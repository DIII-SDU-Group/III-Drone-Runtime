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
    PylonEndpoint,
    PylonOverviewStatus,
    Point3,
    PowerlineGeometry,
    PowerlineLineGeometry,
    ProjectionPlane,
    PowerlineDomainState,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog
from ..ros_services import create_reentrant_client, wait_for_service_response


PL_MAPPER_COMMAND_SERVICE = "/perception/pl_mapper/pl_mapper_command"
UPDATE_POWERLINE_OVERVIEW_SERVICE = "/mission/powerline_overview_provider/update_powerline_overview"
GET_POWERLINE_OVERVIEW_SERVICE = "/mission/powerline_overview_provider/get_powerline_overview"
CAPTURE_CURRENT_PYLON_SERVICE = "/mission/pylon_overview_provider/capture_current_pylon"
CLEAR_PYLON_OVERVIEW_SERVICE = "/mission/pylon_overview_provider/clear_pylon_overview"


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


class PylonOverviewServiceAdapter(Protocol):
    def capture_current(self, *, pylon_id: int, replace_existing: bool) -> dict[str, Any]:
        ...

    def clear(self) -> dict[str, Any]:
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
        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is not available for PL mapper commands")
        if self._client is None:
            self._client = create_reentrant_client(node, PLMapperCommand, self.service_name)
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
        response = wait_for_service_response(
            self._client,
            request,
            timeout_sec=2.0,
            label="PL mapper command response",
        )
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


class RosPowerlineOverviewServiceAdapter:
    def __init__(self, *, node_provider: Callable[[], Any | None]):
        self.node_provider = node_provider
        self._clients: dict[str, Any] = {}

    def update(self, *, timeout_s: int) -> dict[str, Any]:
        from iii_drone_interfaces.srv import GetPowerlineOverview, UpdatePowerlineOverview

        update = self._call(UpdatePowerlineOverview, UPDATE_POWERLINE_OVERVIEW_SERVICE, timeout_sec=max(2.0, timeout_s + 1.0), timeout_s=timeout_s)
        if not update.success:
            return {"success": False, "message": "powerline overview provider rejected storage"}
        stored = self._call(GetPowerlineOverview, GET_POWERLINE_OVERVIEW_SERVICE, timeout_sec=3.0)
        return {
            "success": bool(stored.success),
            "overview_in_frame": bool(stored.overview_in_frame),
            "overview_gnss_only": bool(stored.overview_gnss_only),
            "overview_source": str(stored.overview_source),
            "stored_powerline": _ros_message_to_dict(stored.stored_powerline),
        }

    def _call(self, service_type: Any, service_name: str, *, timeout_sec: float, **fields: Any) -> Any:
        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is unavailable")
        client = self._clients.get(service_name)
        if client is None:
            client = create_reentrant_client(node, service_type, service_name)
            self._clients[service_name] = client
        if not client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"ROS service unavailable: {service_name}")
        request = service_type.Request()
        for name, value in fields.items():
            setattr(request, name, value)
        return wait_for_service_response(
            client,
            request,
            timeout_sec=timeout_sec,
            label=service_name,
        )


class UnavailablePylonOverviewServiceAdapter:
    def capture_current(self, *, pylon_id: int, replace_existing: bool) -> dict[str, Any]:
        del pylon_id, replace_existing
        raise RuntimeError(f"pylon capture service unavailable: {CAPTURE_CURRENT_PYLON_SERVICE}")

    def clear(self) -> dict[str, Any]:
        raise RuntimeError(f"pylon clear service unavailable: {CLEAR_PYLON_OVERVIEW_SERVICE}")


class RosPylonOverviewServiceAdapter(RosPowerlineOverviewServiceAdapter):
    def capture_current(self, *, pylon_id: int, replace_existing: bool) -> dict[str, Any]:
        from iii_drone_interfaces.srv import CaptureCurrentPylon

        response = self._call(
            CaptureCurrentPylon,
            CAPTURE_CURRENT_PYLON_SERVICE,
            timeout_sec=3.0,
            id=pylon_id,
            replace_existing=replace_existing,
        )
        return {
            "success": bool(response.success),
            "message": str(response.message),
            "captured_at": _ros_stamp_to_iso(response.captured_at),
            "captured_pylon": _ros_message_to_dict(response.captured_pylon),
            "gnss_reference_valid": bool(response.gnss_reference_valid),
            "persistence_source": str(response.persistence_source),
            "stored_pylon_overview": _ros_message_to_dict(response.stored_pylon_overview),
        }

    def clear(self) -> dict[str, Any]:
        from iii_drone_interfaces.srv import ClearPylonOverview

        response = self._call(ClearPylonOverview, CLEAR_PYLON_OVERVIEW_SERVICE, timeout_sec=3.0)
        return {
            "success": bool(response.success),
            "message": str(response.message),
            "stored_pylon_overview": _ros_message_to_dict(response.stored_pylon_overview),
        }


class PerceptionStatusCache:
    def __init__(self, *, overview_stale_after_seconds: float = 3.0, minimum_capture_line_count: int = 4):
        self.overview_stale_after = overview_stale_after_seconds
        self.minimum_capture_line_count = minimum_capture_line_count
        self._pl_mapper_state: str | None = None
        self._pl_direction_status: str | None = None
        self._hough_status: str | None = None
        self._stored_overview_status: str | None = None
        self._stored_pylon_status: str | None = None
        self._pylon_overview = PylonOverviewStatus()
        self._pylon_typed_received = False
        self._live_geometry = PowerlineGeometry()
        self._stored_geometry = PowerlineGeometry()
        self._stored_overview_source = "none"
        self._stored_overview_valid = False
        self._stored_overview_gnss_only = False
        self._stored_powerline_typed_received = False
        self._live_powerline_line_count: int | None = None
        self._last_perception_update_at: datetime | None = None
        self._last_powerline_update_at: datetime | None = None
        self._last_stored_overview_update_at: datetime | None = None
        self._last_pylon_update_at: datetime | None = None
        self._last_live_powerline_sample_at: datetime | None = None
        self._live_powerline_publisher_available = False

    def subscribe(self, node: Any) -> list[Any]:
        try:
            from iii_drone_interfaces.msg import (
                Powerline,
                PowerlineOverviewStatus,
                PylonOverviewStatus as PylonOverviewStatusMsg,
                StringStamped,
            )
        except Exception:
            return []
        subscriptions = [
            node.create_subscription(StringStamped, "/perception/pl_mapper/state", self.handle_pl_mapper_state, 10),
            node.create_subscription(StringStamped, "/perception/pl_dir_computer/status", self.handle_pl_direction_status, 10),
            node.create_subscription(StringStamped, "/perception/hough_transformer/status", self.handle_hough_status, 10),
            node.create_subscription(StringStamped, "/mission/powerline_overview_provider/stored_powerline_status", self.handle_stored_overview_status, 10),
            node.create_subscription(StringStamped, "/mission/pylon_overview_provider/stored_pylon_status", self.handle_stored_pylon_status, 10),
            node.create_subscription(PylonOverviewStatusMsg, "/mission/pylon_overview_provider/overview_status", self.handle_pylon_overview_status, 10),
            node.create_subscription(Powerline, "/perception/pl_mapper/powerline", self.handle_live_powerline, 10),
            node.create_subscription(PowerlineOverviewStatus, "/mission/powerline_overview_provider/overview_status", self.handle_powerline_overview_status, 10),
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
        self._last_stored_overview_update_at = _utc_now()
        self._last_powerline_update_at = self._last_stored_overview_update_at

    def handle_stored_pylon_status(self, message: Any) -> None:
        self._stored_pylon_status = str(getattr(message, "data"))
        self._last_pylon_update_at = _utc_now()

    def handle_pylon_overview_status(self, message: Any) -> None:
        received_at = _utc_now()
        overview = getattr(message, "overview", None)
        self._pylon_overview = PylonOverviewStatus(
            valid=bool(getattr(message, "valid", False)),
            pylon_count=int(getattr(message, "pylon_count", 0)),
            pylon_ids=[int(value) for value in getattr(message, "pylon_ids", [])],
            frame_id=str(getattr(overview, "frame_id", "")),
            pylons=[
                PylonEndpoint(id=int(pylon.id), x=float(pylon.x), y=float(pylon.y))
                for pylon in getattr(overview, "pylons", [])
            ],
            overview_in_frame=bool(getattr(message, "overview_in_frame", False)),
            overview_gnss_only=bool(getattr(message, "overview_gnss_only", False)),
            overview_source=str(getattr(message, "overview_source", "none")),
            persistence_file_present=bool(getattr(message, "persistence_file_present", False)),
            source_timestamp=_ros_stamp_to_datetime(getattr(message, "stamp", None)),
            freshness=Freshness.FRESH,
            degraded_reason=str(getattr(message, "degraded_reason", "")) or None,
        )
        self._pylon_typed_received = True
        self._last_pylon_update_at = received_at

    def mission_overview_rejections(self, *, now: datetime | None = None) -> list[str]:
        current = now or _utc_now()
        reasons: list[str] = []
        if self._last_stored_overview_update_at is None:
            reasons.append("stored powerline overview status has not been received")
        elif (current - self._last_stored_overview_update_at).total_seconds() > self.overview_stale_after:
            reasons.append("stored powerline overview status is stale")
        elif self._stored_powerline_typed_received and not self._stored_overview_valid:
            reasons.append(
                "stored powerline GNSS data is unavailable in the active world frame"
                if self._stored_overview_gnss_only
                else "no valid powerline overview is stored"
            )
        elif not self._stored_powerline_typed_received and not (self._stored_overview_status or "").lower().startswith("powerline stored"):
            reasons.append(self._stored_overview_status or "no valid powerline overview is stored")

        if self._last_pylon_update_at is None:
            reasons.append("stored pylon overview status has not been received")
        elif (current - self._last_pylon_update_at).total_seconds() > self.overview_stale_after:
            reasons.append("stored pylon overview status is stale")
        elif self._pylon_typed_received and not self._pylon_overview.valid:
            reasons.append(self._pylon_overview.degraded_reason or "no valid two-pylon overview is stored")
        elif not self._pylon_typed_received and not (self._stored_pylon_status or "").lower().startswith("pylon overview stored"):
            reasons.append(self._stored_pylon_status or "no valid two-pylon overview is stored")
        return reasons

    def handle_live_powerline(self, message: Any) -> None:
        now = _utc_now()
        self._live_powerline_line_count = len(list(getattr(message, "lines", [])))
        self._last_live_powerline_sample_at = now
        self._last_powerline_update_at = now
        self._live_powerline_publisher_available = True
        self._live_geometry = _powerline_geometry(message)

    def handle_powerline_overview_status(self, message: Any) -> None:
        self._stored_powerline_typed_received = True
        self._stored_overview_valid = bool(getattr(message, "valid", False))
        self._stored_overview_gnss_only = bool(getattr(message, "overview_gnss_only", False))
        self._stored_overview_source = str(getattr(message, "overview_source", "none"))
        self._stored_geometry = _powerline_geometry(getattr(message, "overview", None))
        self._last_stored_overview_update_at = _utc_now()
        self._last_powerline_update_at = self._last_stored_overview_update_at

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

    def powerline_capture_rejections(self, *, now: datetime | None = None) -> list[str]:
        current = now or _utc_now()
        reasons: list[str] = []
        if not self._live_powerline_publisher_available:
            reasons.append("live powerline publisher is unavailable")
        if self._last_live_powerline_sample_at is None:
            reasons.append("no live powerline sample has been received")
        elif (current - self._last_live_powerline_sample_at).total_seconds() > self.overview_stale_after:
            reasons.append("live powerline sample is stale")
        if (self._live_powerline_line_count or 0) < self.minimum_capture_line_count:
            reasons.append(
                f"at least {self.minimum_capture_line_count} live powerline lines are required"
            )
        mapper_state = (self._pl_mapper_state or "").strip().lower()
        if not any(label in mapper_state for label in ("running", "active", "started")):
            reasons.append("PL mapper is not actively producing an overview")
        if self._last_perception_update_at is None:
            reasons.append("perception health state is unavailable")
        elif (current - self._last_perception_update_at).total_seconds() > self.overview_stale_after:
            reasons.append("perception health state is stale")
        return reasons

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
        now = _utc_now()
        if self._live_powerline_publisher_available and self._live_powerline_line_count is not None:
            self._last_powerline_update_at = now
        pylon_overview = self._pylon_overview
        if self._last_pylon_update_at is None:
            pylon_overview = pylon_overview.model_copy(
                update={
                    "freshness": Freshness.UNKNOWN,
                    "degraded_reason": pylon_overview.degraded_reason
                    or "stored pylon overview status has not been received",
                }
            )
        elif (now - self._last_pylon_update_at).total_seconds() > self.overview_stale_after:
            pylon_overview = pylon_overview.model_copy(
                update={
                    "freshness": Freshness.STALE,
                    "degraded_reason": "stored pylon overview status is stale",
                }
            )
        live_status = "available" if self._live_powerline_line_count else ("running" if self._live_powerline_publisher_available else "unknown")
        latest = {
            "stored_overview_status": self._stored_overview_status or "Unknown",
            "stored_pylon_status": self._stored_pylon_status or "Unknown",
            "pylon_overview": pylon_overview.model_dump(mode="json"),
            "mission_overview_rejections": self.mission_overview_rejections(),
            "live_powerline_line_count": self._live_powerline_line_count,
            "live_perception_status": live_status,
            "live_powerline_publisher_available": self._live_powerline_publisher_available,
            "last_live_powerline_sample_at": self._last_live_powerline_sample_at.isoformat()
            if self._last_live_powerline_sample_at
            else None,
            "capture_ready": not self.powerline_capture_rejections(),
            "capture_rejections": self.powerline_capture_rejections(),
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
            pylon_overview=pylon_overview,
            live_geometry=self._live_geometry,
            stored_geometry=self._stored_geometry,
            stored_overview_source=self._stored_overview_source,
            stored_overview_valid=self._stored_overview_valid,
            stored_overview_gnss_only=self._stored_overview_gnss_only,
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
        pylon_service: PylonOverviewServiceAdapter,
        event_log: RuntimeEventLog,
        recording_precondition: Callable[[], dict[str, Any]] | None = None,
    ):
        self.status_cache = status_cache
        self.permission_gate = permission_gate
        self.pl_mapper_service = pl_mapper_service
        self.overview_service = overview_service
        self.pylon_service = pylon_service
        self.event_log = event_log
        self.recording_precondition = recording_precondition

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
        for command_id, summary in (
            (CommandId.PYLON_CAPTURE_CURRENT.value, "Capture current aircraft position as pylon endpoint"),
            (CommandId.PYLON_OVERVIEW_CLEAR.value, "Clear stored pylon overview"),
        ):
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.MUTATING,
                transport="ros_service",
                summary=summary,
            )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        permission = self.permission_gate.mutating_permission("perception")
        if not permission.allowed:
            return self._reject(request, "; ".join(permission.reasons), ErrorCode.FORBIDDEN)
        try:
            if request.command_id == CommandId.POWERLINE_OVERVIEW_UPDATE.value:
                readiness_rejections = self.status_cache.powerline_capture_rejections()
                if readiness_rejections:
                    return self._reject(request, "; ".join(readiness_rejections), ErrorCode.DEGRADED_STATE)
            if request.command_id in {CommandId.POWERLINE_OVERVIEW_UPDATE.value, CommandId.PYLON_CAPTURE_CURRENT.value} and self.recording_precondition is not None:
                self.recording_precondition()
            if request.command_id in PL_MAPPER_COMMANDS:
                result = self.pl_mapper_service.command(
                    PL_MAPPER_COMMANDS[request.command_id],
                    reset=bool(request.parameters.get("reset", False)),
                )
            elif request.command_id == CommandId.POWERLINE_OVERVIEW_UPDATE.value:
                result = self.overview_service.update(timeout_s=int(request.parameters.get("timeout_s", 5)))
                if result.get("success"):
                    freeze = self.pl_mapper_service.command("freeze")
                    result["mapper_freeze"] = freeze
                    if not freeze.get("success", False):
                        result["success"] = False
                        result["message"] = "overview persisted but mapper freeze failed"
            elif request.command_id == CommandId.PYLON_CAPTURE_CURRENT.value:
                result = self.pylon_service.capture_current(
                    pylon_id=int(request.parameters.get("pylon_id", 0)),
                    replace_existing=bool(request.parameters.get("replace_existing", False)),
                )
            elif request.command_id == CommandId.PYLON_OVERVIEW_CLEAR.value:
                result = self.pylon_service.clear()
            else:
                return self._reject(request, "unsupported perception command", ErrorCode.HANDLER_UNAVAILABLE)
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE)
        if not result.get("success", False):
            response = self._reject(
                request,
                str(result.get("message") or "perception provider rejected the command"),
                ErrorCode.DEGRADED_STATE,
            )
            return response.model_copy(
                update={
                    "result": {
                        "result": result,
                        "perception": self.status_cache.perception_state(permission=permission).model_dump(mode="json"),
                        "powerline": self.status_cache.powerline_state(permission=permission).model_dump(mode="json"),
                    }
                }
            )
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
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
    pylon_service: PylonOverviewServiceAdapter,
    event_log: RuntimeEventLog,
    recording_precondition: Callable[[], dict[str, Any]] | None = None,
) -> PerceptionCommandHandlers:
    handlers = PerceptionCommandHandlers(
        status_cache=status_cache,
        permission_gate=permission_gate,
        pl_mapper_service=pl_mapper_service,
        overview_service=overview_service,
        pylon_service=pylon_service,
        event_log=event_log,
        recording_precondition=recording_precondition,
    )
    handlers.register(registry)
    return handlers


def _ros_stamp_to_datetime(stamp: Any) -> datetime | None:
    if stamp is None:
        return None
    seconds = int(getattr(stamp, "sec", 0))
    nanoseconds = int(getattr(stamp, "nanosec", 0))
    if seconds == 0 and nanoseconds == 0:
        return None
    return datetime.fromtimestamp(seconds + nanoseconds / 1_000_000_000, tz=timezone.utc)


def _ros_stamp_to_iso(stamp: Any) -> str | None:
    value = _ros_stamp_to_datetime(stamp)
    return value.isoformat() if value else None


def _ros_message_to_dict(message: Any) -> dict[str, Any]:
    try:
        from rosidl_runtime_py.convert import message_to_ordereddict

        return dict(message_to_ordereddict(message))
    except Exception:
        return {
            name: getattr(message, name)
            for name in getattr(message, "get_fields_and_field_types", lambda: {})()
        }


def _point3(value: Any) -> Point3:
    return Point3(
        x=float(getattr(value, "x", 0.0)),
        y=float(getattr(value, "y", 0.0)),
        z=float(getattr(value, "z", 0.0)),
    )


def _powerline_geometry(message: Any) -> PowerlineGeometry:
    if message is None:
        return PowerlineGeometry()
    plane = getattr(message, "projection_plane", None)
    return PowerlineGeometry(
        lines=[
            PowerlineLineGeometry(
                id=int(getattr(line, "id", 0)),
                position=_point3(getattr(getattr(line, "pose", None), "position", None)),
                projected_position=_point3(getattr(line, "projected_position", None)),
                in_field_of_view=bool(getattr(line, "in_field_of_view", False)),
            )
            for line in getattr(message, "lines", [])
        ],
        projection_plane=ProjectionPlane(
            point=_point3(getattr(plane, "point", None)),
            normal=_point3(getattr(plane, "normal", None)),
        ),
        source_timestamp=_ros_stamp_to_datetime(getattr(message, "stamp", None)),
    )
