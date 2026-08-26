"""Installed mission-catalog query and maintenance-safe selection commands."""

from __future__ import annotations

import json
import re
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


CATALOG_READ_COMMANDS = {
    CommandId.MISSION_CATALOG_STATUS.value,
    CommandId.MISSION_CATALOG_LIST.value,
    CommandId.MISSION_CATALOG_SHOW.value,
}
CATALOG_SELECT_COMMAND = CommandId.MISSION_CATALOG_SELECT.value
CONTENT_ID = re.compile(r"^sha256:[a-f0-9]{64}$")


class MissionCatalogServiceAdapter(Protocol):
    def catalog(self, *, include_incompatible: bool) -> dict[str, Any]:
        ...

    def select(self, *, catalog_id: str, use_default: bool) -> dict[str, Any]:
        ...


class RosMissionCatalogServiceAdapter:
    def __init__(self, *, node_provider: Callable[[], Any | None]):
        self.node_provider = node_provider
        self._catalog_client = None
        self._select_client = None

    def catalog(self, *, include_incompatible: bool) -> dict[str, Any]:
        from iii_drone_interfaces.srv import GetMissionCatalog

        node = self._node()
        if self._catalog_client is None:
            self._catalog_client = create_reentrant_client(
                node,
                GetMissionCatalog,
                "/mission/mission_executor/get_mission_catalog",
            )
        if not self._catalog_client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError("mission catalog service is unavailable")
        request = GetMissionCatalog.Request()
        request.include_incompatible = include_incompatible
        response = wait_for_service_response(
            self._catalog_client,
            request,
            timeout_sec=3.0,
            label="mission catalog query",
        )
        if not response.success:
            raise RuntimeError(str(response.message) or "mission catalog query was rejected")
        try:
            catalog = json.loads(response.catalog_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("mission catalog service returned malformed JSON") from exc
        if not isinstance(catalog, dict) or catalog.get("schema") != "iii.mission-catalog/v1":
            raise RuntimeError("mission catalog service returned an unsupported contract")
        return catalog

    def select(self, *, catalog_id: str, use_default: bool) -> dict[str, Any]:
        from iii_drone_interfaces.srv import SelectMissionCatalogEntry

        node = self._node()
        if self._select_client is None:
            self._select_client = create_reentrant_client(
                node,
                SelectMissionCatalogEntry,
                "/mission/mission_executor/select_mission_catalog_entry",
            )
        if not self._select_client.wait_for_service(timeout_sec=1.0):
            raise RuntimeError("mission catalog selection service is unavailable")
        request = SelectMissionCatalogEntry.Request()
        request.catalog_id = catalog_id
        request.use_default = use_default
        response = wait_for_service_response(
            self._select_client,
            request,
            timeout_sec=20.0,
            label="mission catalog selection",
        )
        result = {
            "success": bool(response.success),
            "message": str(response.message),
            "active_catalog_id": str(response.active_catalog_id),
            "active_catalog_hash": str(response.active_catalog_hash),
            "active_entry_hash": str(response.active_entry_hash),
            "active_specification_asset_id": str(response.active_specification_asset_id),
            "active_behavior_tree_asset_ids": list(response.active_behavior_tree_asset_ids),
            "temporary_override": bool(response.temporary_override),
            "warning": str(response.warning) or None,
        }
        if not result["success"]:
            raise RuntimeError(result["message"] or "mission catalog selection was rejected")
        return result

    def _node(self) -> Any:
        node = self.node_provider()
        if node is None:
            raise RuntimeError("runtime ROS node is unavailable")
        return node


class MissionCatalogSelectionGate:
    def __init__(
        self,
        *,
        profile: str,
        mission_state_provider: Callable[[], Any],
        operation_state_provider: Callable[[], Any],
        vehicle_state_provider: Callable[[], Any],
    ):
        self.profile = profile
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider
        self.vehicle_state_provider = vehicle_state_provider

    def rejection_reasons(self) -> list[str]:
        mission = self.mission_state_provider()
        operation = self.operation_state_provider()
        reasons: list[str] = []
        if str(getattr(mission, "freshness", "unknown")) != "fresh":
            reasons.append("mission status is stale or unavailable")
        if bool(getattr(mission, "latest", {}).get("mission_active", False)):
            reasons.append("a mission is active")
        if str(getattr(operation, "freshness", "unknown")) != "fresh":
            reasons.append("custom-operation status is stale or unavailable")
        if bool(getattr(operation, "latest", {}).get("operation_active", False)):
            reasons.append("a custom operation is active")
        if self.profile == "sim":
            return reasons

        vehicle = self.vehicle_state_provider()
        if str(getattr(vehicle, "freshness", "unknown")) != "fresh":
            reasons.append("PX4 vehicle state is stale or unavailable")
        if str(getattr(vehicle, "source_availability", "unknown")) != "available":
            reasons.append("PX4 vehicle state is degraded or unavailable")
        if getattr(vehicle, "armed", None) is not False:
            reasons.append("vehicle is not confirmed disarmed")
        if getattr(vehicle, "in_air", None) is not False:
            reasons.append("vehicle is not confirmed landed")
        nav_state = str(getattr(vehicle, "nav_state", "") or "").lower()
        if nav_state not in {"manual", "position", "hold"}:
            reasons.append(f"PX4 navigation state is not maintenance-safe: {nav_state or 'unknown'}")
        return reasons


class MissionCatalogCommandHandlers:
    def __init__(
        self,
        *,
        service: MissionCatalogServiceAdapter,
        status_provider: Callable[[], Any],
        selection_gate: MissionCatalogSelectionGate,
        event_log: RuntimeEventLog,
    ):
        self.service = service
        self.status_provider = status_provider
        self.selection_gate = selection_gate
        self.event_log = event_log

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in sorted(CATALOG_READ_COMMANDS):
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.READ_ONLY,
                transport="ros_service",
                summary=f"Mission catalog command {command_id}",
            )
        registry.register_action(
            CATALOG_SELECT_COMMAND,
            self.handle,
            permission=HandlerPermission.RUNTIME_MUTATION,
            transport="ros_service",
            summary="Maintenance-safe mission catalog selection",
        )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        mutating = request.command_id == CATALOG_SELECT_COMMAND
        self.event_log.record_command_request(
            command_id=request.command_id,
            request_id=request.request_id,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=mutating,
        )
        try:
            result = self._execute(request)
        except Exception as exc:
            return self._reject(request, str(exc), mutating=mutating)
        evidence = None
        if mutating:
            evidence = {
                key: result.get(key)
                for key in (
                    "active_catalog_id",
                    "active_catalog_hash",
                    "active_entry_hash",
                    "active_specification_asset_id",
                    "active_behavior_tree_asset_ids",
                )
            }
        self.event_log.record_command_decision(
            command_id=request.command_id,
            request_id=request.request_id,
            accepted=True,
            reason="mission catalog command completed",
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=mutating,
            details=evidence,
        )
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
            result=result,
            message=result.get("message"),
        )

    def _execute(self, request: CommandRequest) -> dict[str, Any]:
        if request.command_id == CommandId.MISSION_CATALOG_STATUS.value:
            state = self.status_provider()
            return {
                "status": state.model_dump(mode="json"),
                "message": "mission catalog runtime status returned",
            }
        include_all = bool(request.parameters.get("all", False))
        catalog = self.service.catalog(include_incompatible=include_all)
        if request.command_id == CommandId.MISSION_CATALOG_LIST.value:
            return {"catalog": catalog, "message": "mission catalog entries returned"}
        if request.command_id == CommandId.MISSION_CATALOG_SHOW.value:
            catalog_id = _catalog_id(request.parameters)
            entry = next((item for item in catalog.get("entries", []) if item.get("id") == catalog_id), None)
            if entry is None:
                raise RuntimeError(f"unknown or unavailable mission catalog ID: {catalog_id}")
            return {
                "catalog_hash": catalog.get("catalog_hash"),
                "scope": catalog.get("scope"),
                "active_profile": catalog.get("active_profile"),
                "entry": entry,
                "message": "mission catalog entry returned",
            }
        if request.command_id == CATALOG_SELECT_COMMAND:
            reasons = self.selection_gate.rejection_reasons()
            if reasons:
                raise RuntimeError("mission selection is not maintenance-safe: " + "; ".join(reasons))
            use_default = bool(request.parameters.get("default", False))
            catalog_id = "" if use_default else _catalog_id(request.parameters)
            result = self.service.select(catalog_id=catalog_id, use_default=use_default)
            _validate_selection_evidence(result)
            if result.get("warning"):
                result["message"] = f"{result['message']} WARNING: {result['warning']}"
            return result
        raise RuntimeError(f"unsupported mission catalog command: {request.command_id}")

    def _reject(self, request: CommandRequest, reason: str, *, mutating: bool) -> ActionStartResponse:
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
            rejection=CommandRejection(code=ErrorCode.FORBIDDEN, message=reason),
        )


def _catalog_id(parameters: dict[str, Any]) -> str:
    value = parameters.get("catalog_id")
    if not isinstance(value, str) or not value or any(character in value for character in "/\\~$"):
        raise RuntimeError("a logical mission catalog ID is required; filesystem paths are forbidden")
    return value


def _validate_selection_evidence(result: dict[str, Any]) -> None:
    active_catalog_id = result.get("active_catalog_id")
    if (
        not isinstance(active_catalog_id, str)
        or not active_catalog_id
        or any(character in active_catalog_id for character in "/\\~$")
    ):
        raise RuntimeError("mission selection returned an invalid logical catalog ID")
    for field in (
        "active_catalog_hash",
        "active_entry_hash",
        "active_specification_asset_id",
    ):
        if not isinstance(result.get(field), str) or not CONTENT_ID.fullmatch(result[field]):
            raise RuntimeError(f"mission selection returned an invalid {field}")
    tree_ids = result.get("active_behavior_tree_asset_ids")
    if (
        not isinstance(tree_ids, list)
        or not tree_ids
        or tree_ids != sorted(set(tree_ids))
        or any(not isinstance(value, str) or not CONTENT_ID.fullmatch(value) for value in tree_ids)
    ):
        raise RuntimeError("mission selection returned invalid behavior-tree asset identities")


def register_mission_catalog_command_handlers(
    registry: DispatchRegistry,
    *,
    service: MissionCatalogServiceAdapter,
    status_provider: Callable[[], Any],
    selection_gate: MissionCatalogSelectionGate,
    event_log: RuntimeEventLog,
) -> MissionCatalogCommandHandlers:
    handlers = MissionCatalogCommandHandlers(
        service=service,
        status_provider=status_provider,
        selection_gate=selection_gate,
        event_log=event_log,
    )
    handlers.register(registry)
    return handlers
