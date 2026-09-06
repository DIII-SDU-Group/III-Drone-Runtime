"""Rosbag recorder runtime API handlers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, sleep as time_sleep
from typing import Any, Callable, Protocol, Sequence

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

from ..ros_services import create_reentrant_client, wait_for_service_response

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog


MISSION_RECORDING_OWNERS = frozenset(
    {
        "inspection",
        "inspection_demo",
        "mission",
        "mission_executor",
        "behavior_tree",
        "reach_cable",
        "leave_cable",
        "cable_charging",
    }
)

INSPECTION_RECORDING_TOPICS = (
    "/fmu/out/vehicle_status_v1",
    "/fmu/out/vehicle_odometry",
    "/fmu/out/vehicle_land_detected",
    "/fmu/out/battery_status",
    "/fmu/out/failsafe_flags",
    "/fmu/out/manual_control_setpoint",
    "/fmu/out/vehicle_command_ack",
    "/fmu/in/vehicle_command",
    "/fmu/in/vehicle_command_mode_executor",
    "/fmu/in/trajectory_setpoint",
    "/fmu/in/config_overrides_request",
    "/fmu/in/mode_completed",
    "/control/maneuver_controller/reference",
    "/control/maneuver_controller/reference_mode",
    "/control/maneuver_controller/current_maneuver",
    "/control/maneuver_controller/maneuver_queue",
    "/control/trajectory_generator/trajectory_path",
    "/mission/mission_executor/maneuver_reference_client/reference_mode",
    "/mission/status",
    "/mission/modes/inspection_demo/status",
    "/mission/modes/reach_cable/status",
    "/mission/modes/leave_cable/status",
    "/mission/modes/cable_charging/status",
    "/sensor/mmwave/points",
    "/sensor/mmwave/points_full",
    "/perception/pl_mapper/powerline",
    "/perception/pl_mapper/projected_points",
    "/perception/pl_mapper/points_est",
    "/perception/pl_mapper/transformed_points",
    "/perception/pl_dir_computer/powerline_direction_pose",
    "/payload/charger_gripper/gripper_status",
    "/payload/charger_gripper/sim_state",
    "/payload/charger_gripper/charger_status",
    "/payload/charger_gripper/charging_power",
    "/payload/charger_gripper/battery_voltage",
    "/tf",
    "/tf_static",
    "/rosout",
)


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

    def available_topics(self) -> list[str]:
        ...


class FilesystemRosbagRecorderAdapter:
    def __init__(self, storage_root: str = "/tmp/iii_drone/rosbags"):
        self.storage_root = Path(storage_root)

    def status(self) -> dict[str, Any]:
        return {
            "recording": False,
            "recording_id": None,
            "output_dir": None,
            "artifact_root": str(self.storage_root),
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

    def available_topics(self) -> list[str]:
        return []


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
                "output_dir": "",
                "all_topics": bool(request.get("all_topics", True)),
                "topics": list(request.get("topics", [])),
                "include_hidden_topics": bool(request.get("include_hidden_topics", False)),
                "owner": str(request.get("owner", "unknown")),
            },
        )
        result = _response_dict(response)
        if not result.get("success", False):
            raise RuntimeError(str(result.get("message") or "rosbag recorder rejected start"))
        return result

    def available_topics(self) -> list[str]:
        node = self.node_provider()
        if node is None:
            return []
        try:
            return sorted(name for name, _types in node.get_topic_names_and_types())
        except Exception:
            return []

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
        except Exception as exc:
            raise RuntimeError("ROS rosbag recorder services are unavailable") from exc
        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is not available for rosbag recorder services")
        service_type = getattr(srv_module, service_type_name)
        fq_name = f"{self.namespace}/{service_name}"
        client = self._clients.get(fq_name)
        if client is None:
            client = create_reentrant_client(node, service_type, fq_name)
            self._clients[fq_name] = client
        if not client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"rosbag recorder service unavailable: {fq_name}")
        request = service_type.Request()
        for key, value in (fields or {}).items():
            setattr(request, key, value)
        return wait_for_service_response(client, request, timeout_sec=3.0, label=fq_name)


class RosbagController:
    def __init__(
        self,
        *,
        adapter: RosbagRecorderAdapter,
        critical_free_space_bytes: int = 1 << 30,
        activation_grace_seconds: float = 10.0,
        recording_start_timeout_seconds: float = 5.0,
        recording_start_poll_interval_seconds: float = 0.1,
        monotonic_clock: Callable[[], float] = monotonic,
        sleep: Callable[[float], None] = time_sleep,
        inspection_topics: Sequence[str] = INSPECTION_RECORDING_TOPICS,
    ):
        self.adapter = adapter
        self.critical_free_space_bytes = critical_free_space_bytes
        self.activation_grace_seconds = activation_grace_seconds
        self.recording_start_timeout_seconds = recording_start_timeout_seconds
        self.recording_start_poll_interval_seconds = recording_start_poll_interval_seconds
        self.monotonic_clock = monotonic_clock
        self.sleep = sleep
        self.inspection_topics = tuple(inspection_topics)
        self._activation_pending_until: float | None = None
        self._last_error: str | None = None

    def ensure_inspection_recording(self) -> dict[str, Any]:
        try:
            status = self.adapter.status()
            self._require_storage(status)
            active_owner = str(status.get("owner") or _owner_from_status(status)).strip().lower()
            if status.get("recording") and active_owner not in MISSION_RECORDING_OWNERS:
                raise RuntimeError(
                    f"{active_owner or 'unknown'}-owned rosbag recording is active; "
                    "stop it before starting inspection"
                )
            if not status.get("recording"):
                self.adapter.start(
                    {
                        "all_topics": False,
                        "topics": list(self.inspection_topics),
                        "owner": "inspection",
                        "include_hidden_topics": False,
                    }
                )
                status = self._wait_for_recording_activation()
            if not status.get("recording"):
                detail = str(status.get("error") or status.get("message") or "recorder remained inactive")
                raise RuntimeError(
                    "inspection recording did not become active within "
                    f"{self.recording_start_timeout_seconds:g}s: {detail}"
                )
            self._require_storage(status)
        except Exception as exc:
            self._last_error = str(exc)
            raise
        if (status.get("owner") or _owner_from_status(status)) == "inspection":
            self._activation_pending_until = self.monotonic_clock() + self.activation_grace_seconds
        self._last_error = None
        return status

    def _wait_for_recording_activation(self) -> dict[str, Any]:
        deadline = self.monotonic_clock() + self.recording_start_timeout_seconds
        status = self.adapter.status()
        while not status.get("recording") and self.monotonic_clock() < deadline:
            remaining = deadline - self.monotonic_clock()
            self.sleep(min(self.recording_start_poll_interval_seconds, remaining))
            status = self.adapter.status()
        return status

    def reconcile(
        self,
        *,
        mission_active: bool,
        nav_mode: str,
        failsafe: bool,
        control_owner: str = "unknown",
        armed: bool | None = None,
        in_air: bool | None = None,
    ) -> None:
        try:
            status = self.adapter.status()
        except Exception as exc:
            self._last_error = f"rosbag status unavailable: {exc}"
            return
        owner = str(status.get("owner") or _owner_from_status(status)).strip().lower()
        mission_owned_recording = bool(status.get("recording")) and owner in MISSION_RECORDING_OWNERS
        if not mission_owned_recording:
            if not status.get("recording"):
                self._reset_mission_recording_state()
            return

        normalized_nav_mode = nav_mode.strip().lower()
        normalized_owner = control_owner.strip().lower()
        px4_has_taken_control = (
            failsafe
            or normalized_nav_mode in {"hold", "position", "manual"}
            or normalized_owner in {
                "px4",
                "px4_hold",
                "px4_position",
                "px4_manual",
            }
        )
        executor_owns_control = not px4_has_taken_control and (
            mission_active or normalized_owner in {"mission", "mode_executor", "mission_executor"}
        )
        if executor_owns_control:
            self._activation_pending_until = None
            return

        activation_pending = self._activation_pending_until is not None
        if not px4_has_taken_control and activation_pending and self.monotonic_clock() < self._activation_pending_until:
            return

        try:
            self.adapter.stop({"recording_id": status.get("recording_id") or "", "timeout_sec": 10.0})
            self._reset_mission_recording_state()
            self._last_error = None
        except Exception as exc:
            self._last_error = f"automatic mission recording finalization failed: {exc}"

    def _reset_mission_recording_state(self) -> None:
        self._activation_pending_until = None

    def _require_storage(self, status: dict[str, Any]) -> None:
        free = status.get("free_space_bytes")
        if free is not None and int(free) < self.critical_free_space_bytes:
            raise RuntimeError(f"rosbag storage critically low: {int(free)} bytes available")

    def state(self) -> RosbagDomainState:
        try:
            status = self.adapter.status()
        except Exception as exc:
            self._last_error = f"rosbag status unavailable: {exc}"
            status = {
                "recording": False,
                "owner": "unknown",
                "source_availability": "unavailable",
                "error": self._last_error,
            }
        availability = status.get("source_availability", "available")
        source_availability = (
            SourceAvailability.UNAVAILABLE
            if availability == "unavailable"
            else SourceAvailability.DEGRADED
            if availability == "degraded"
            else SourceAvailability.AVAILABLE
        )
        recording_error = status.get("error") or self._last_error
        if source_availability != SourceAvailability.AVAILABLE and not recording_error:
            recording_error = status.get("message") or "rosbag recorder unavailable"
        started_at = status.get("started_at") or None
        return RosbagDomainState(
            source_label="rosbag_recorder",
            freshness=Freshness.FRESH if source_availability == SourceAvailability.AVAILABLE else Freshness.STALE,
            source_availability=source_availability,
            latest={
                "status": status,
                "recordings": self.adapter.list_recordings(),
            },
            recording=bool(status.get("recording", False)),
            recording_id=status.get("recording_id") or None,
            output_dir=status.get("output_dir") or None,
            storage_root=status.get("artifact_root") or str(getattr(self.adapter, "storage_root", "")) or None,
            available_topics=_available_topics(self.adapter),
            owner=status.get("owner") or _owner_from_status(status),
            size_bytes=status.get("size_bytes"),
            free_space_bytes=status.get("free_space_bytes"),
            started_at=started_at,
            duration_seconds=_duration_seconds(started_at) if status.get("recording") else None,
            recording_error=recording_error,
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
        if str(request.parameters.get("output_dir", "")).strip():
            return self._reject(request, "output directory is configured system-wide and cannot be overridden", ErrorCode.INVALID_REQUEST)
        response = self.controller.adapter.start(
            {
                "recording_id": request.parameters.get("recording_id", ""),
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


def _available_topics(adapter: RosbagRecorderAdapter) -> list[str]:
    discover = getattr(adapter, "available_topics", None)
    if not callable(discover):
        return []
    try:
        return sorted({str(topic) for topic in discover() if str(topic)})
    except Exception:
        return []


def _response_dict(response: Any) -> dict[str, Any]:
    fields = getattr(response, "__slots__", None) or []
    if fields:
        return {field.lstrip("_"): getattr(response, field.lstrip("_"), getattr(response, field, None)) for field in fields}
    names = [
        "recording",
        "recording_id",
        "output_dir",
        "artifact_root",
        "pid",
        "started_at",
        "size_bytes",
        "free_space_bytes",
        "owner",
        "error",
        "message",
        "success",
        "was_running",
    ]
    return {name: getattr(response, name) for name in names if hasattr(response, name)}


def _duration_seconds(started_at: str | None) -> float | None:
    if not started_at:
        return None
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - started).total_seconds())
    except ValueError:
        return None


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
