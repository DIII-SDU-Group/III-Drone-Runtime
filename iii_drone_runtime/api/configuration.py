"""Configuration server runtime API facade."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRejection,
    CommandRequest,
    ConfigurationApplyRequest,
    ConfigurationApplyResponse,
    ConfigurationDomainState,
    ConfigurationManifest,
    ConfigurationStatus,
    ErrorCode,
    EventSource,
    HandlerPermission,
    ParameterApplyResult,
    ParameterConstraint,
    ParameterDefinition,
    ParameterEdit,
    ParameterGroup,
    ParameterNode,
    ParameterValueType,
    RestartRequired,
    SnapshotDownloadRequest,
    SnapshotLoadRequest,
    SnapshotOperationResponse,
    SnapshotSaveRequest,
    SnapshotSetDefaultRequest,
    SnapshotSummary,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog


CONFIGURATION_SERVER_NAMESPACE = "/configuration/configuration_server"
RUNTIME_SNAPSHOT_PREFIX = "snapshots/runtime_parameters_"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ConfigurationServerAdapter(Protocol):
    def manifest(self) -> ConfigurationManifest:
        ...

    def apply(self, request: ConfigurationApplyRequest) -> ConfigurationApplyResponse:
        ...

    def save_snapshot(self, request: SnapshotSaveRequest) -> SnapshotOperationResponse:
        ...

    def load_snapshot(self, request: SnapshotLoadRequest) -> SnapshotOperationResponse:
        ...

    def list_snapshots(self) -> list[SnapshotSummary]:
        ...

    def download_snapshot(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        ...

    def set_default_snapshot(self, request: SnapshotSetDefaultRequest) -> SnapshotOperationResponse:
        ...


class UnavailableConfigurationServerAdapter:
    def manifest(self) -> ConfigurationManifest:
        return ConfigurationManifest(
            status=ConfigurationStatus(),
            generated_at=_utc_now(),
        )

    def apply(self, request: ConfigurationApplyRequest) -> ConfigurationApplyResponse:
        del request
        raise RuntimeError("configuration server is unavailable")

    def save_snapshot(self, request: SnapshotSaveRequest) -> SnapshotOperationResponse:
        del request
        raise RuntimeError("configuration server is unavailable")

    def load_snapshot(self, request: SnapshotLoadRequest) -> SnapshotOperationResponse:
        del request
        raise RuntimeError("configuration server is unavailable")

    def list_snapshots(self) -> list[SnapshotSummary]:
        return []

    def download_snapshot(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        del request
        raise RuntimeError("configuration server is unavailable")

    def set_default_snapshot(self, request: SnapshotSetDefaultRequest) -> SnapshotOperationResponse:
        del request
        raise RuntimeError("configuration server is unavailable")


class RosConfigurationServerAdapter:
    """ROS service adapter; all configuration data comes from the server."""

    def __init__(self, node: Any, namespace: str = CONFIGURATION_SERVER_NAMESPACE):
        self.node = node
        self.namespace = namespace.rstrip("/")
        self._clients: dict[str, Any] = {}

    def manifest(self) -> ConfigurationManifest:
        raw_manifest = self._load_yaml_service("GetParameterYaml", "get_parameter_yaml", "yaml")
        declared = self._load_yaml_service(
            "GetDeclaredParameters",
            "get_declared_parameters",
            "declared_parameters_yaml",
        )
        current_file, default_file = self._current_files()
        snapshots = self.list_snapshots()
        manifest = _manifest_from_configuration_server_payload(
            raw_manifest=raw_manifest,
            declared_parameters=declared,
            current_snapshot_id=current_file,
            default_snapshot_id=default_file,
            available_snapshots=snapshots,
        )
        return manifest

    def apply(self, request: ConfigurationApplyRequest) -> ConfigurationApplyResponse:
        manifest = self.manifest()
        restart_by_parameter = {
            parameter.name: parameter.restart_required
            for node in manifest.nodes
            for group in node.groups
            for parameter in group.parameters
        }
        results: list[ParameterApplyResult] = []
        for edit in request.edits:
            try:
                response = self._call_service("SetParameterFromGC", "set_parameter_from_gc")
                response_request = response["request"]
                response_request.parameter_name = edit.name
                response_request.parameter_string_value = _service_value_string(edit.value)
                result = response["call"](response_request)
                success = bool(result.success)
                message = str(result.message) if getattr(result, "message", "") else None
            except Exception as exc:
                success = False
                message = str(exc)
            results.append(
                ParameterApplyResult(
                    node_id=edit.node_id,
                    name=edit.name,
                    success=success,
                    message=message,
                    applied_value=edit.value if success else None,
                    restart_required=restart_by_parameter.get(edit.name, RestartRequired.NONE),
                )
            )
        return ConfigurationApplyResponse(
            ok=all(result.success for result in results),
            results=results,
            status=self.manifest().status,
        )

    def save_snapshot(self, request: SnapshotSaveRequest) -> SnapshotOperationResponse:
        service = self._call_service("SaveParameters", "save_parameters")
        service_request = service["request"]
        service_request.file = request.overwrite_snapshot_id or _snapshot_file_from_label(request.label)
        service_request.set_as_default = False
        service_request.overwrite = request.overwrite_snapshot_id is not None
        response = service["call"](service_request)
        snapshot_id = str(response.file or service_request.file)
        return SnapshotOperationResponse(
            ok=bool(response.success),
            snapshot=_summary(snapshot_id, self.manifest().status.default_snapshot_id, snapshot_id),
            status=self.manifest().status,
            message=str(response.message) if getattr(response, "message", "") else None,
        )

    def load_snapshot(self, request: SnapshotLoadRequest) -> SnapshotOperationResponse:
        service = self._call_service("LoadParameters", "load_parameters")
        service_request = service["request"]
        service_request.file = request.snapshot_id
        service_request.set_as_default = False
        response = service["call"](service_request)
        return SnapshotOperationResponse(
            ok=bool(response.success),
            snapshot=_summary(request.snapshot_id, self.manifest().status.default_snapshot_id, request.snapshot_id),
            status=self.manifest().status,
            message=str(response.message) if getattr(response, "message", "") else None,
        )

    def list_snapshots(self) -> list[SnapshotSummary]:
        service = self._call_service("GetParameterFiles", "get_parameter_files")
        response = service["call"](service["request"])
        current_file, default_file = self._current_files()
        return [_summary(file_name, default_file, current_file) for file_name in response.parameter_files]

    def download_snapshot(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        current_file, _default_file = self._current_files()
        if request.snapshot_id != current_file:
            return {
                "snapshot_id": request.snapshot_id,
                "download_supported": False,
                "message": "configuration server only exposes the active snapshot content",
            }
        service = self._call_service("GetParameterYaml", "get_parameter_yaml")
        response = service["call"](service["request"])
        return {
            "snapshot_id": request.snapshot_id,
            "content_type": "application/x-yaml",
            "content": str(response.yaml),
            "download_supported": True,
        }

    def set_default_snapshot(self, request: SnapshotSetDefaultRequest) -> SnapshotOperationResponse:
        current_file, _default_file = self._current_files()
        if request.snapshot_id == current_file:
            service = self._call_service(
                "SetCurrentParameterFileAsDefault",
                "set_current_parameter_file_as_default",
            )
            response = service["call"](service["request"])
        else:
            service = self._call_service("LoadParameters", "load_parameters")
            service_request = service["request"]
            service_request.file = request.snapshot_id
            service_request.set_as_default = True
            response = service["call"](service_request)
        return SnapshotOperationResponse(
            ok=bool(response.success),
            snapshot=_summary(request.snapshot_id, request.snapshot_id, request.snapshot_id),
            status=self.manifest().status,
            message=str(response.message) if getattr(response, "message", "") else None,
        )

    def _current_files(self) -> tuple[str | None, str | None]:
        service = self._call_service("GetCurrentParameterFile", "get_current_parameter_file")
        response = service["call"](service["request"])
        return (
            str(response.current_parameter_file) if response.current_parameter_file else None,
            str(response.default_parameter_file) if response.default_parameter_file else None,
        )

    def _load_yaml_service(self, service_type: str, service_name: str, response_attr: str) -> dict[str, Any]:
        service = self._call_service(service_type, service_name)
        response = service["call"](service["request"])
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError("PyYAML is required to read configuration server payloads") from exc
        return yaml.safe_load(getattr(response, response_attr)) or {}

    def _call_service(self, service_type_name: str, service_name: str) -> dict[str, Any]:
        try:
            from iii_drone_interfaces import srv as srv_module
            import rclpy
        except Exception as exc:
            raise RuntimeError("ROS configuration server services are unavailable") from exc

        service_type = getattr(srv_module, service_type_name)
        fq_name = f"{self.namespace}/{service_name}"
        client = self._clients.get(fq_name)
        if client is None:
            client = self.node.create_client(service_type, fq_name)
            self._clients[fq_name] = client
        if not client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError(f"configuration server service unavailable: {fq_name}")

        def call(request: Any) -> Any:
            future = client.call_async(request)
            rclpy.spin_until_future_complete(self.node, future, timeout_sec=3.0)
            if not future.done():
                raise TimeoutError(f"timed out waiting for {fq_name}")
            return future.result()

        return {"request": service_type.Request(), "call": call}


@dataclass(frozen=True)
class ConfigurationPermission:
    allowed: bool
    reasons: list[str]


class ConfigurationPermissionGate:
    def __init__(
        self,
        *,
        mission_state_provider: Callable[[], Any],
        operation_state_provider: Callable[[], Any],
    ):
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider

    def mutating_permission(self) -> ConfigurationPermission:
        reasons: list[str] = []
        mission = self.mission_state_provider()
        operation = self.operation_state_provider()
        if mission.latest.get("mission_active") is True or mission.mission_state == "active":
            reasons.append("configuration writes are disabled in Mission mode")
        if operation.latest.get("operation_active") is True or operation.active_operation_id:
            reasons.append("configuration writes are disabled while a custom operation action is active")
        return ConfigurationPermission(allowed=not reasons, reasons=reasons)


class ConfigurationRuntimeController:
    def __init__(self, *, adapter: ConfigurationServerAdapter, permission_gate: ConfigurationPermissionGate):
        self.adapter = adapter
        self.permission_gate = permission_gate

    def manifest(self) -> ConfigurationManifest:
        return self.adapter.manifest()

    def state(self) -> ConfigurationDomainState:
        try:
            manifest = self.adapter.manifest()
        except Exception as exc:
            return ConfigurationDomainState(
                source_label="configuration_server",
                freshness=Freshness.UNKNOWN,
                source_availability=SourceAvailability.UNAVAILABLE,
                degraded_reason=str(exc),
                latest={"error": str(exc), "permissions": self._permission_payload()},
            )
        status = manifest.status
        return ConfigurationDomainState(
            source_label="configuration_server",
            source_timestamp=manifest.generated_at,
            freshness=Freshness.FRESH,
            source_availability=SourceAvailability.AVAILABLE,
            latest={
                "manifest": manifest.model_dump(mode="json"),
                "available_snapshots": [snapshot.model_dump(mode="json") for snapshot in manifest.available_snapshots],
                "permissions": self._permission_payload(),
            },
            active_snapshot_id=status.loaded_snapshot_id,
            pending_edits=status.pending_edits,
            unsaved=status.unsaved,
            non_default=status.non_default,
        )

    def apply(self, request: ConfigurationApplyRequest) -> ConfigurationApplyResponse:
        permission = self.permission_gate.mutating_permission()
        if not permission.allowed:
            return ConfigurationApplyResponse(
                ok=False,
                results=[
                    ParameterApplyResult(
                        node_id=edit.node_id,
                        name=edit.name,
                        success=False,
                        message="; ".join(permission.reasons),
                    )
                    for edit in request.edits
                ],
                status=self._status_after_denial(),
            )
        return self.adapter.apply(request)

    def save_snapshot(self, request: SnapshotSaveRequest) -> SnapshotOperationResponse:
        return self._mutating_snapshot_operation(lambda: self.adapter.save_snapshot(request))

    def load_snapshot(self, request: SnapshotLoadRequest) -> SnapshotOperationResponse:
        return self._mutating_snapshot_operation(lambda: self.adapter.load_snapshot(request))

    def set_default_snapshot(self, request: SnapshotSetDefaultRequest) -> SnapshotOperationResponse:
        return self._mutating_snapshot_operation(lambda: self.adapter.set_default_snapshot(request))

    def list_snapshots(self) -> list[SnapshotSummary]:
        return self.adapter.list_snapshots()

    def download_snapshot(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        return self.adapter.download_snapshot(request)

    def _mutating_snapshot_operation(self, operation: Callable[[], SnapshotOperationResponse]) -> SnapshotOperationResponse:
        permission = self.permission_gate.mutating_permission()
        if not permission.allowed:
            return SnapshotOperationResponse(
                ok=False,
                status=self._status_after_denial(),
                message="; ".join(permission.reasons),
            )
        return operation()

    def _status_after_denial(self) -> ConfigurationStatus:
        try:
            return self.adapter.manifest().status
        except Exception:
            return ConfigurationStatus()

    def _permission_payload(self) -> dict[str, Any]:
        permission = self.permission_gate.mutating_permission()
        return {
            "writes_allowed": permission.allowed,
            "write_rejections": permission.reasons,
        }


class ConfigurationCommandHandlers:
    def __init__(self, *, controller: ConfigurationRuntimeController, event_log: RuntimeEventLog):
        self.controller = controller
        self.event_log = event_log

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in (
            CommandId.CONFIGURATION_APPLY.value,
            CommandId.CONFIGURATION_SAVE_SNAPSHOT.value,
            CommandId.CONFIGURATION_LOAD_SNAPSHOT.value,
            CommandId.CONFIGURATION_SET_DEFAULT_SNAPSHOT.value,
        ):
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.MUTATING,
                transport="configuration_server",
                summary=f"Configuration mutation {command_id}",
            )
        for command_id in (
            CommandId.CONFIGURATION_DOWNLOAD_SNAPSHOT.value,
            CommandId.CONFIGURATION_LIST_SNAPSHOTS.value,
        ):
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.READ_ONLY,
                transport="configuration_server",
                summary=f"Configuration read {command_id}",
            )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        try:
            if request.command_id == CommandId.CONFIGURATION_APPLY.value:
                edits = [ParameterEdit(**edit) for edit in request.parameters.get("edits", [])]
                result = self.controller.apply(ConfigurationApplyRequest(edits=edits))
                if not result.ok:
                    return self._reject(request, _failure_message(result), ErrorCode.FORBIDDEN, result.model_dump(mode="json"))
                return self._ok(request, {"configuration": result.model_dump(mode="json")})
            if request.command_id == CommandId.CONFIGURATION_SAVE_SNAPSHOT.value:
                result = self.controller.save_snapshot(SnapshotSaveRequest(**request.parameters))
                return self._snapshot_response(request, result)
            if request.command_id == CommandId.CONFIGURATION_LOAD_SNAPSHOT.value:
                result = self.controller.load_snapshot(SnapshotLoadRequest(**request.parameters))
                return self._snapshot_response(request, result)
            if request.command_id == CommandId.CONFIGURATION_SET_DEFAULT_SNAPSHOT.value:
                result = self.controller.set_default_snapshot(SnapshotSetDefaultRequest(**request.parameters))
                return self._snapshot_response(request, result)
            if request.command_id == CommandId.CONFIGURATION_LIST_SNAPSHOTS.value:
                return self._ok(
                    request,
                    {"snapshots": [snapshot.model_dump(mode="json") for snapshot in self.controller.list_snapshots()]},
                )
            if request.command_id == CommandId.CONFIGURATION_DOWNLOAD_SNAPSHOT.value:
                result = self.controller.download_snapshot(SnapshotDownloadRequest(**request.parameters))
                return self._ok(request, {"snapshot": result})
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE, None)
        return self._reject(request, "unsupported configuration command", ErrorCode.HANDLER_UNAVAILABLE, None)

    def _snapshot_response(self, request: CommandRequest, result: SnapshotOperationResponse) -> ActionStartResponse:
        if not result.ok:
            return self._reject(request, result.message or "configuration operation rejected", ErrorCode.FORBIDDEN, result.model_dump(mode="json"))
        return self._ok(request, {"configuration": result.model_dump(mode="json")})

    def _ok(self, request: CommandRequest, result: dict[str, Any]) -> ActionStartResponse:
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
            result=result,
        )

    def _reject(
        self,
        request: CommandRequest,
        reason: str,
        code: ErrorCode,
        result: dict[str, Any] | None,
    ) -> ActionStartResponse:
        self.event_log.record_command_decision(
            command_id=request.command_id,
            request_id=request.request_id,
            accepted=False,
            reason=reason,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=request.command_id
            not in {
                CommandId.CONFIGURATION_LIST_SNAPSHOTS.value,
                CommandId.CONFIGURATION_DOWNLOAD_SNAPSHOT.value,
            },
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
            result=result,
        )


def register_configuration_command_handlers(
    registry: DispatchRegistry,
    *,
    controller: ConfigurationRuntimeController,
    event_log: RuntimeEventLog,
) -> ConfigurationCommandHandlers:
    handlers = ConfigurationCommandHandlers(controller=controller, event_log=event_log)
    handlers.register(registry)
    return handlers


def _manifest_from_configuration_server_payload(
    *,
    raw_manifest: dict[str, Any],
    declared_parameters: dict[str, Any],
    current_snapshot_id: str | None,
    default_snapshot_id: str | None,
    available_snapshots: list[SnapshotSummary],
) -> ConfigurationManifest:
    flat = _flatten_schema(raw_manifest)
    groups_by_node: dict[str, dict[str, list[ParameterDefinition]]] = {}
    for parameter_name, entry in sorted(flat.items()):
        node_id = _node_id_for_parameter(parameter_name, declared_parameters)
        group_id = _group_id_for_parameter(parameter_name)
        groups_by_node.setdefault(node_id, {}).setdefault(group_id, []).append(
            _parameter_definition(
                node_id=node_id,
                group_id=group_id,
                name=parameter_name,
                entry=entry,
            )
        )

    nodes = [
        ParameterNode(
            node_id=node_id,
            label=node_id.replace("_", " ").title(),
            groups=[
                ParameterGroup(
                    group_id=group_id,
                    label=group_id.replace("/", " / ").replace("_", " ").title(),
                    node_id=node_id,
                    parameters=parameters,
                )
                for group_id, parameters in sorted(groups.items())
            ],
        )
        for node_id, groups in sorted(groups_by_node.items())
    ]
    status = _configuration_status(current_snapshot_id, default_snapshot_id)
    return ConfigurationManifest(
        nodes=nodes,
        loaded_snapshot=_summary(current_snapshot_id, default_snapshot_id, current_snapshot_id) if current_snapshot_id else None,
        default_snapshot=_summary(default_snapshot_id, default_snapshot_id, current_snapshot_id) if default_snapshot_id else None,
        available_snapshots=available_snapshots,
        status=status,
        generated_at=_utc_now(),
    )


def _flatten_schema(raw: dict[str, Any], prefix: str = "") -> dict[str, dict[str, Any]]:
    flat: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        parameter_name = f"{prefix}/{key}" if prefix else f"/{key}"
        if "type" in value and "value" in value:
            flat[parameter_name] = value
        else:
            flat.update(_flatten_schema(value, parameter_name))
    return flat


def _parameter_definition(*, node_id: str, group_id: str, name: str, entry: dict[str, Any]) -> ParameterDefinition:
    value_type = _parameter_value_type(str(entry.get("type", "string")))
    current_value = entry.get("value")
    default_value = entry.get("default_value", entry.get("default"))
    return ParameterDefinition(
        node_id=node_id,
        group_id=group_id,
        name=name,
        value_type=value_type,
        current_value=current_value,
        loaded_value=entry.get("loaded_value", current_value),
        default_value=default_value if default_value is not None else current_value,
        description=entry.get("description"),
        constraints=_constraints(entry),
        restart_required=_restart_required(entry),
        readonly=bool(entry.get("readonly", False) or entry.get("read_only", False) or entry.get("constant", False)),
        reference=entry.get("reference"),
    )


def _parameter_value_type(value: str) -> ParameterValueType:
    normalized = value.lower()
    if normalized in {"int", "integer"}:
        return ParameterValueType.INTEGER
    if normalized in {"int_array", "integer_array"}:
        return ParameterValueType.INTEGER_ARRAY
    if normalized == "float_array":
        return ParameterValueType.FLOAT_ARRAY
    if normalized == "string_array":
        return ParameterValueType.STRING_ARRAY
    if normalized == "bool":
        return ParameterValueType.BOOL
    if normalized == "float":
        return ParameterValueType.FLOAT
    return ParameterValueType.STRING


def _constraints(entry: dict[str, Any]) -> ParameterConstraint | None:
    values = {
        "minimum": entry.get("minimum", entry.get("min")),
        "maximum": entry.get("maximum", entry.get("max")),
        "step": entry.get("step"),
        "choices": entry.get("choices", entry.get("options")),
        "regex": entry.get("regex"),
        "unit": entry.get("unit"),
    }
    if all(value is None for value in values.values()):
        return None
    return ParameterConstraint(**values)


def _restart_required(entry: dict[str, Any]) -> RestartRequired:
    explicit = entry.get("restart_required")
    if explicit in {RestartRequired.RUNTIME.value, RestartRequired.NODE.value, RestartRequired.NONE.value}:
        return RestartRequired(explicit)
    if entry.get("constant") is True:
        return RestartRequired.RUNTIME
    if entry.get("static") is True:
        return RestartRequired.NODE
    return RestartRequired.NONE


def _node_id_for_parameter(parameter_name: str, declared_parameters: dict[str, Any]) -> str:
    declared_nodes = declared_parameters.get(parameter_name) or declared_parameters.get(parameter_name.lstrip("/"))
    if isinstance(declared_nodes, list) and declared_nodes:
        return str(declared_nodes[0]).strip("/") or "configuration"
    parts = [part for part in parameter_name.split("/") if part]
    return parts[0] if parts else "configuration"


def _group_id_for_parameter(parameter_name: str) -> str:
    parts = [part for part in parameter_name.split("/") if part]
    if len(parts) <= 1:
        return "general"
    return "/".join(parts[:-1])


def _configuration_status(current_snapshot_id: str | None, default_snapshot_id: str | None) -> ConfigurationStatus:
    unsaved = bool(current_snapshot_id and current_snapshot_id.startswith(RUNTIME_SNAPSHOT_PREFIX))
    non_default = bool(current_snapshot_id and default_snapshot_id and current_snapshot_id != default_snapshot_id and not unsaved)
    return ConfigurationStatus(
        pending_edits=False,
        unsaved=unsaved,
        non_default=non_default,
        loaded_snapshot_id=current_snapshot_id,
        default_snapshot_id=default_snapshot_id,
    )


def _summary(snapshot_id: str | None, default_snapshot_id: str | None, loaded_snapshot_id: str | None) -> SnapshotSummary:
    return SnapshotSummary(
        snapshot_id=snapshot_id or "",
        label=(snapshot_id or "").rsplit("/", 1)[-1] or "current",
        is_default=bool(snapshot_id and snapshot_id == default_snapshot_id),
        is_loaded=bool(snapshot_id and snapshot_id == loaded_snapshot_id),
    )


def _service_value_string(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    return json.dumps(value)


def _snapshot_file_from_label(label: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", label.strip()).strip("_")
    if not clean:
        clean = "snapshot"
    if not clean.endswith((".yaml", ".yml")):
        clean = f"{clean}.yaml"
    return clean


def _failure_message(response: ConfigurationApplyResponse) -> str:
    messages = [result.message for result in response.results if result.message]
    return "; ".join(messages) or "configuration apply rejected"
