"""Rosbag recorder runtime API handlers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Protocol

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRejection,
    CommandRequest,
    ErrorCode,
    EventSource,
    HandlerPermission,
    RosbagDomainState,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog


class RosbagRecorderAdapter(Protocol):
    def status(self) -> dict[str, Any]:
        ...

    def start(self, request: dict[str, Any]) -> dict[str, Any]:
        ...

    def stop(self, request: dict[str, Any]) -> dict[str, Any]:
        ...

    def list_recordings(self) -> list[dict[str, Any]]:
        ...

    def download(self, recording_id: str) -> dict[str, Any]:
        ...


class FilesystemRosbagRecorderAdapter:
    def __init__(self, storage_root: str = "/tmp/iii_drone/rosbags"):
        self.storage_root = Path(storage_root)

    def status(self) -> dict[str, Any]:
        return {
            "recording": False,
            "recording_id": None,
            "output_dir": None,
            "owner": "unknown",
            "message": "rosbag recorder status service unavailable",
        }

    def start(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        raise RuntimeError("rosbag recorder start service unavailable")

    def stop(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        raise RuntimeError("rosbag recorder stop service unavailable")

    def list_recordings(self) -> list[dict[str, Any]]:
        if not self.storage_root.exists():
            return []
        rows = []
        for path in sorted(self.storage_root.iterdir()):
            if not path.is_dir():
                continue
            size = sum(file.stat().st_size for file in path.rglob("*") if file.is_file())
            rows.append({"recording_id": path.name, "path": str(path), "size_bytes": size})
        return rows

    def download(self, recording_id: str) -> dict[str, Any]:
        path = (self.storage_root / recording_id).resolve()
        root = self.storage_root.resolve()
        if root not in path.parents and path != root:
            raise ValueError("recording_id escapes rosbag storage root")
        if not path.exists():
            raise FileNotFoundError(recording_id)
        return {"recording_id": recording_id, "path": str(path), "download_supported": True}


class RosRosbagRecorderAdapter(FilesystemRosbagRecorderAdapter):
    def __init__(
        self,
        *,
        node_provider: Callable[[], Any | None],
        namespace: str = "/mission/rosbag_recorder",
        storage_root: str = "/tmp/iii_drone/rosbags",
    ):
        super().__init__(storage_root=storage_root)
        self.node_provider = node_provider
        self.namespace = namespace.rstrip("/")
        self._clients: dict[str, Any] = {}

    def status(self) -> dict[str, Any]:
        try:
            response = self._call("GetRosbagRecordingStatus", "recording_status")
        except Exception as exc:
            fallback = super().status()
            fallback["message"] = str(exc)
            fallback["source_availability"] = "unavailable"
            return fallback
        return _response_dict(response)

    def start(self, request: dict[str, Any]) -> dict[str, Any]:
        response = self._call(
            "StartRosbagRecording",
            "start_recording",
            {
                "recording_id": request.get("recording_id", ""),
                "output_dir": request.get("output_dir", ""),
                "all_topics": bool(request.get("all_topics", True)),
                "topics": list(request.get("topics", [])),
                "include_hidden_topics": bool(request.get("include_hidden_topics", False)),
            },
        )
        result = _response_dict(response)
        if not result.get("success", False):
            raise RuntimeError(str(result.get("message") or "rosbag recorder rejected start"))
        return result

    def stop(self, request: dict[str, Any]) -> dict[str, Any]:
        response = self._call(
            "StopRosbagRecording",
            "stop_recording",
            {
                "recording_id": request.get("recording_id", ""),
                "timeout_sec": float(request.get("timeout_sec", 5.0)),
            },
        )
        result = _response_dict(response)
        if not result.get("success", False):
            raise RuntimeError(str(result.get("message") or "rosbag recorder rejected stop"))
        return result

    def _call(self, service_type_name: str, service_name: str, fields: dict[str, Any] | None = None) -> Any:
        try:
            from iii_drone_interfaces import srv as srv_module
            import rclpy
        except Exception as exc:
            raise RuntimeError("ROS rosbag recorder services are unavailable") from exc
        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is not available for rosbag recorder services")
        service_type = getattr(srv_module, service_type_name)
        fq_name = f"{self.namespace}/{service_name}"
        client = self._clients.get(fq_name)
        if client is None:
            client = node.create_client(service_type, fq_name)
            self._clients[fq_name] = client
        if not client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"rosbag recorder service unavailable: {fq_name}")
        request = service_type.Request()
        for key, value in (fields or {}).items():
            setattr(request, key, value)
        future = client.call_async(request)
        rclpy.spin_until_future_complete(node, future, timeout_sec=3.0)
        if not future.done():
            raise TimeoutError(f"timed out waiting for {fq_name}")
        return future.result()


class RosbagController:
    def __init__(self, *, adapter: RosbagRecorderAdapter):
        self.adapter = adapter

    def state(self) -> RosbagDomainState:
        status = self.adapter.status()
        return RosbagDomainState(
            source_label="rosbag_recorder",
            freshness=Freshness.FRESH,
            source_availability=SourceAvailability.AVAILABLE,
            latest={
                "status": status,
                "recordings": self.adapter.list_recordings(),
            },
            recording=bool(status.get("recording", False)),
            recording_id=status.get("recording_id") or None,
            output_dir=status.get("output_dir") or None,
            owner=status.get("owner") or _owner_from_status(status),
            size_bytes=status.get("size_bytes"),
        )


class RosbagCommandHandlers:
    def __init__(
        self,
        *,
        controller: RosbagController,
        event_log: RuntimeEventLog,
        mission_state_provider,
    ):
        self.controller = controller
        self.event_log = event_log
        self.mission_state_provider = mission_state_provider

    def register(self, registry: DispatchRegistry) -> None:
        registry.register_action(
            CommandId.ROSBAG_START.value,
            self.handle,
            permission=HandlerPermission.MUTATING,
            transport="rosbag_recorder",
            summary="Start manual rosbag recording",
        )
        registry.register_action(
            CommandId.ROSBAG_STOP.value,
            self.handle,
            permission=HandlerPermission.MUTATING,
            transport="rosbag_recorder",
            summary="Stop rosbag recording",
        )
        registry.register_action(
            CommandId.ROSBAG_LIST.value,
            self.handle,
            permission=HandlerPermission.READ_ONLY,
            transport="rosbag_recorder",
            summary="List rosbag recordings",
        )
        registry.register_action(
            CommandId.ROSBAG_DOWNLOAD.value,
            self.handle,
            permission=HandlerPermission.READ_ONLY,
            transport="rosbag_recorder",
            summary="Prepare rosbag download",
        )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        try:
            if request.command_id == CommandId.ROSBAG_START.value:
                return self._start(request)
            if request.command_id == CommandId.ROSBAG_STOP.value:
                return self._stop(request)
            if request.command_id == CommandId.ROSBAG_LIST.value:
                return self._ok(request, {"recordings": self.controller.adapter.list_recordings()})
            if request.command_id == CommandId.ROSBAG_DOWNLOAD.value:
                return self._ok(request, self.controller.adapter.download(str(request.parameters["recording_id"])))
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE)
        return self._reject(request, "unsupported rosbag command", ErrorCode.HANDLER_UNAVAILABLE)

    def _start(self, request: CommandRequest) -> ActionStartResponse:
        reason = self._press_hold_reason(request, owner_sensitive=False)
        if reason:
            return self._reject(request, reason, ErrorCode.FORBIDDEN)
        response = self.controller.adapter.start(
            {
                "recording_id": request.parameters.get("recording_id", ""),
                "output_dir": request.parameters.get("output_dir", ""),
                "all_topics": request.parameters.get("all_topics", True),
                "topics": request.parameters.get("topics", []),
                "include_hidden_topics": request.parameters.get("include_hidden_topics", False),
                "owner": "manual",
            }
        )
        return self._ok(request, {"rosbag": response, "state": self.controller.state().model_dump(mode="json")})

    def _stop(self, request: CommandRequest) -> ActionStartResponse:
        reason = self._press_hold_reason(request, owner_sensitive=True)
        if reason:
            return self._reject(request, reason, ErrorCode.FORBIDDEN)
        response = self.controller.adapter.stop(
            {
                "recording_id": request.parameters.get("recording_id", ""),
                "timeout_sec": request.parameters.get("timeout_sec", 5.0),
            }
        )
        return self._ok(request, {"rosbag": response, "state": self.controller.state().model_dump(mode="json")})

    def _press_hold_reason(self, request: CommandRequest, *, owner_sensitive: bool) -> str | None:
        mission = self.mission_state_provider()
        status = self.controller.adapter.status()
        mission_active = mission.latest.get("mission_active") is True or mission.mission_state == "active"
        owner = status.get("owner") or _owner_from_status(status)
        needs_hold = mission_active or (owner_sensitive and owner not in {"manual", "gui", "unknown"})
        if needs_hold and request.parameters.get("hold_confirmed") is not True:
            if owner_sensitive and owner not in {"manual", "gui", "unknown"}:
                return f"stopping {owner}-owned rosbag recording requires press-and-hold confirmation"
            return "rosbag recording control during Mission mode requires press-and-hold confirmation"
        return None

    def _ok(self, request: CommandRequest, result: dict[str, Any]) -> ActionStartResponse:
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
            result=result,
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
            message=reason,
            rejection=CommandRejection(
                code=code,
                message=reason,
                request_id=request.request_id,
                command_id=request.command_id,
                retryable=code != ErrorCode.FORBIDDEN,
            ),
        )


def _owner_from_status(status: dict[str, Any]) -> str:
    if status.get("recording") and not status.get("owner"):
        return "runtime"
    return status.get("owner") or "unknown"


def _response_dict(response: Any) -> dict[str, Any]:
    fields = getattr(response, "__slots__", None) or []
    if fields:
        return {field.lstrip("_"): getattr(response, field.lstrip("_"), getattr(response, field, None)) for field in fields}
    names = [
        "recording",
        "recording_id",
        "output_dir",
        "pid",
        "started_at",
        "size_bytes",
        "message",
        "success",
        "was_running",
    ]
    return {name: getattr(response, name) for name in names if hasattr(response, name)}


def register_rosbag_command_handlers(
    registry: DispatchRegistry,
    *,
    controller: RosbagController,
    event_log: RuntimeEventLog,
    mission_state_provider,
) -> RosbagCommandHandlers:
    handlers = RosbagCommandHandlers(
        controller=controller,
        event_log=event_log,
        mission_state_provider=mission_state_provider,
    )
    handlers.register(registry)
    return handlers
