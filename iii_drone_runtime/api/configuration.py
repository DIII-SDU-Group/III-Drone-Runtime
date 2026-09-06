"""Configuration server runtime API facade."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
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
from iii_drone_contracts.configuration_capture import seal_capture

from ..ros_services import create_reentrant_client, wait_for_service_response

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog

CONFIGURATION_SERVER_NAMESPACE = "/configuration/configuration_server"
RUNTIME_SNAPSHOT_PREFIX = "snapshots/runtime_parameters_"
SERVICE_DISCOVERY_TIMEOUT_SECONDS = 0.2
SERVICE_RESPONSE_TIMEOUT_SECONDS = 3.0
CONFIGURATION_TRANSACTION_TIMEOUT_SECONDS = 30.0
MANIFEST_CACHE_TTL_SECONDS = 15.0
_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"configuration payload is not canonical JSON: {exc}"
        ) from exc


def _decode_canonical_object(raw: str, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} is invalid JSON: {exc}") from exc
    if not isinstance(value, dict) or raw != _canonical_json(value):
        raise RuntimeError(f"{label} must be a canonical JSON object")
    return value


def _validate_session_status(value: dict[str, Any]) -> None:
    required = {
        "schema",
        "session_id",
        "baseline_id",
        "target_id",
        "runtime_profile",
        "release_id",
        "workspace_id",
        "manifest_id",
        "revision",
        "wal_sequence",
        "wal_checksum",
        "created_at",
        "updated_at",
        "active_values",
        "persisted_values",
        "pending_boot_values",
        "divergent",
        "divergent_observations",
        "last_result",
    }
    if (
        set(value) != required
        or value.get("schema") != "iii.configuration-session-status/v1"
    ):
        raise RuntimeError("configuration tuning session status fields are invalid")
    session_id = value["session_id"]
    baseline_id = value["baseline_id"]
    if (session_id is None) != (baseline_id is None):
        raise RuntimeError("configuration tuning session identity is incomplete")
    if session_id is not None and (
        not isinstance(session_id, str)
        or not _SHA256_PATTERN.fullmatch(session_id)
        or not isinstance(baseline_id, str)
        or not _SHA256_PATTERN.fullmatch(baseline_id)
    ):
        raise RuntimeError("configuration tuning session identity is invalid")
    if (
        isinstance(value["revision"], bool)
        or not isinstance(value["revision"], int)
        or value["revision"] < 0
        or not isinstance(value["divergent"], bool)
        or not all(
            isinstance(value[field], str) and bool(value[field])
            for field in (
                "target_id",
                "runtime_profile",
                "release_id",
                "workspace_id",
            )
        )
        or not isinstance(value["manifest_id"], str)
        or not _SHA256_PATTERN.fullmatch(value["manifest_id"])
        or isinstance(value["wal_sequence"], bool)
        or not isinstance(value["wal_sequence"], int)
        or value["wal_sequence"] < 0
        or (value["wal_sequence"] == 0) != (value["wal_checksum"] is None)
        or (
            session_id is None
            and (value["revision"] != 0 or value["wal_sequence"] != 0)
        )
        or (
            value["wal_checksum"] is not None
            and (
                not isinstance(value["wal_checksum"], str)
                or not _SHA256_PATTERN.fullmatch(value["wal_checksum"])
            )
        )
        or (value["created_at"] is None) != (session_id is None)
        or (value["updated_at"] is None) != (session_id is None)
        or (
            value["created_at"] is not None and not isinstance(value["created_at"], str)
        )
        or (
            value["updated_at"] is not None and not isinstance(value["updated_at"], str)
        )
        or not all(
            isinstance(value[field], dict)
            for field in (
                "active_values",
                "persisted_values",
                "pending_boot_values",
                "divergent_observations",
            )
        )
        or (
            value["last_result"] is not None
            and not isinstance(value["last_result"], dict)
        )
    ):
        raise RuntimeError("configuration tuning session status values are invalid")


def _validate_transaction_result(value: dict[str, Any]) -> None:
    required = {
        "schema",
        "ok",
        "status",
        "session_id",
        "transaction_id",
        "request_id",
        "revision",
        "reason",
        "observed_values",
        "results",
        "idempotent_replay",
    }
    allowed = required | {"persistence_reference"}
    if not required.issubset(value) or not set(value).issubset(allowed):
        raise RuntimeError("configuration transaction result fields are invalid")
    if (
        value.get("schema") != "iii.configuration-transaction-result/v1"
        or value.get("status")
        not in {"committed", "recovered-commit", "rejected", "aborted", "divergent"}
        or not isinstance(value.get("ok"), bool)
        or not isinstance(value.get("idempotent_replay"), bool)
        or isinstance(value.get("revision"), bool)
        or not isinstance(value.get("revision"), int)
        or value["revision"] < 0
        or not isinstance(value.get("session_id"), str)
        or not _SHA256_PATTERN.fullmatch(value["session_id"])
        or not isinstance(value.get("transaction_id"), str)
        or not _SHA256_PATTERN.fullmatch(value["transaction_id"])
        or not isinstance(value.get("request_id"), str)
        or not isinstance(value.get("observed_values"), dict)
        or not isinstance(value.get("results"), list)
        or (
            value.get("reason") is not None and not isinstance(value.get("reason"), str)
        )
        or (
            "persistence_reference" in value
            and not isinstance(value["persistence_reference"], str)
        )
    ):
        raise RuntimeError("configuration transaction result values are invalid")
    result_fields = {
        "node_id",
        "name",
        "success",
        "message",
        "applied_value",
        "persisted_value",
        "restart_required",
    }
    for item in value["results"]:
        if (
            not isinstance(item, dict)
            or set(item) != result_fields
            or not isinstance(item["node_id"], str)
            or not isinstance(item["name"], str)
            or not isinstance(item["success"], bool)
            or item["restart_required"] not in {"none", "node", "runtime"}
            or (item["message"] is not None and not isinstance(item["message"], str))
        ):
            raise RuntimeError(
                "configuration transaction per-parameter result is invalid"
            )


def _validate_journal_batch(value: dict[str, Any]) -> None:
    fields = {
        "schema",
        "session",
        "baseline_values",
        "after_sequence",
        "through_sequence",
        "head_sequence",
        "head_checksum",
        "entries",
        "complete",
    }
    if (
        set(value) != fields
        or value.get("schema") != "iii.configuration-journal-batch/v1"
    ):
        raise RuntimeError("configuration journal batch fields are invalid")
    for field in ("after_sequence", "through_sequence", "head_sequence"):
        if (
            isinstance(value[field], bool)
            or not isinstance(value[field], int)
            or value[field] < 0
        ):
            raise RuntimeError("configuration journal batch cursor is invalid")
    if (
        not isinstance(value["baseline_values"], dict)
        or not isinstance(value["entries"], list)
        or not isinstance(value["complete"], bool)
        or value["through_sequence"] < value["after_sequence"]
        or value["through_sequence"] > value["head_sequence"]
        or (
            value["head_checksum"] is not None
            and (
                not isinstance(value["head_checksum"], str)
                or not _SHA256_PATTERN.fullmatch(value["head_checksum"])
            )
        )
    ):
        raise RuntimeError("configuration journal batch values are invalid")
    session = value["session"]
    if session is None:
        if value["entries"] or value["head_sequence"] != 0:
            raise RuntimeError("empty configuration journal batch is inconsistent")
        return
    session_fields = {
        "session_id",
        "baseline_id",
        "target_id",
        "runtime_profile",
        "release_id",
        "workspace_id",
        "manifest_id",
        "created_at",
        "updated_at",
        "revision",
    }
    if not isinstance(session, dict) or set(session) != session_fields:
        raise RuntimeError("configuration journal session is invalid")
    for field in ("session_id", "baseline_id", "manifest_id"):
        if not isinstance(session[field], str) or not _SHA256_PATTERN.fullmatch(
            session[field]
        ):
            raise RuntimeError("configuration journal session identity is invalid")
    previous_checksum: str | None = None
    if value["after_sequence"] > 0 and value["entries"]:
        previous_checksum = value["entries"][0].get("previous_checksum")
    expected_sequence = value["after_sequence"] + 1
    entry_fields = {
        "schema",
        "sequence",
        "previous_checksum",
        "checksum",
        "kind",
        "timestamp",
        "session_id",
        "transaction_id",
        "request_id",
        "revision",
        "body",
    }
    for entry in value["entries"]:
        if not isinstance(entry, dict) or set(entry) != entry_fields:
            raise RuntimeError("configuration journal entry fields are invalid")
        supplied_checksum = entry["checksum"]
        identity_value = {key: item for key, item in entry.items() if key != "checksum"}
        expected_checksum = hashlib.sha256(
            _canonical_json(identity_value).encode("utf-8")
        ).hexdigest()
        if (
            entry["schema"] != "iii.configuration-tuning-wal-entry/v1"
            or entry["sequence"] != expected_sequence
            or entry["session_id"] != session["session_id"]
            or supplied_checksum != expected_checksum
            or (
                expected_sequence > value["after_sequence"] + 1
                and entry["previous_checksum"] != previous_checksum
            )
        ):
            raise RuntimeError("configuration journal checksum chain is invalid")
        previous_checksum = supplied_checksum
        expected_sequence += 1
    expected_through = (
        value["entries"][-1]["sequence"]
        if value["entries"]
        else value["after_sequence"]
    )
    if value["through_sequence"] != expected_through:
        raise RuntimeError("configuration journal through-sequence is invalid")
    if value["complete"] != (value["through_sequence"] == value["head_sequence"]):
        raise RuntimeError("configuration journal completion marker is invalid")
    if (
        value["complete"]
        and value["head_sequence"] > 0
        and value["entries"]
        and previous_checksum != value["head_checksum"]
    ):
        raise RuntimeError("configuration journal head checksum is invalid")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ConfigurationServerAdapter(Protocol):
    def manifest(self) -> ConfigurationManifest: ...

    def apply(
        self, request: ConfigurationApplyRequest
    ) -> ConfigurationApplyResponse: ...

    def save_snapshot(
        self, request: SnapshotSaveRequest
    ) -> SnapshotOperationResponse: ...

    def load_snapshot(
        self, request: SnapshotLoadRequest
    ) -> SnapshotOperationResponse: ...

    def list_snapshots(self) -> list[SnapshotSummary]: ...

    def download_snapshot(self, request: SnapshotDownloadRequest) -> dict[str, Any]: ...

    def journal(
        self, *, session_id: str | None, after_sequence: int, limit: int
    ) -> dict[str, Any]: ...

    def capture_source(self, request: SnapshotDownloadRequest) -> dict[str, Any]: ...

    def delete_snapshot(self, request: dict[str, Any]) -> dict[str, Any]: ...

    def set_default_snapshot(
        self, request: SnapshotSetDefaultRequest
    ) -> SnapshotOperationResponse: ...

    def activate_pending_boot_parameters(self) -> dict[str, Any]: ...


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

    def journal(
        self, *, session_id: str | None, after_sequence: int, limit: int
    ) -> dict[str, Any]:
        del session_id, after_sequence, limit
        raise RuntimeError("configuration server is unavailable")

    def capture_source(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        del request
        raise RuntimeError("configuration server is unavailable")

    def delete_snapshot(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        raise RuntimeError("configuration server is unavailable")

    def set_default_snapshot(
        self, request: SnapshotSetDefaultRequest
    ) -> SnapshotOperationResponse:
        del request
        raise RuntimeError("configuration server is unavailable")

    def activate_pending_boot_parameters(self) -> dict[str, Any]:
        raise RuntimeError("configuration server is unavailable")


class RosConfigurationServerAdapter:
    """ROS service adapter; all configuration data comes from the server."""

    def __init__(
        self,
        node: Any | None = None,
        namespace: str = CONFIGURATION_SERVER_NAMESPACE,
        *,
        node_provider: Callable[[], Any | None] | None = None,
    ):
        if node is None and node_provider is None:
            raise ValueError("a ROS node or node_provider is required")
        self.node = node
        self.node_provider = node_provider
        self.namespace = namespace.rstrip("/")
        self._clients: dict[str, Any] = {}
        self._client_node: Any | None = None
        self._manifest_lock = threading.RLock()
        self._manifest_cache: tuple[float, ConfigurationManifest] | None = None

    def manifest(self) -> ConfigurationManifest:
        with self._manifest_lock:
            now = time.monotonic()
            if (
                self._manifest_cache is not None
                and now - self._manifest_cache[0] < MANIFEST_CACHE_TTL_SECONDS
            ):
                return self._manifest_cache[1].model_copy(deep=True)
            raw_manifest = self._load_yaml_service(
                "GetParameterYaml", "get_parameter_yaml", "yaml"
            )
            declared = self._load_yaml_service(
                "GetDeclaredParameters",
                "get_declared_parameters",
                "declared_parameters_yaml",
            )
            current_file, default_file = self._current_files()
            snapshots = self.list_snapshots()
            pending_service = self._call_service(
                "GetPendingBootParameters",
                "get_pending_boot_parameters",
            )
            pending_response = pending_service["call"](pending_service["request"])
            try:
                import yaml
            except ImportError as exc:
                raise RuntimeError(
                    "PyYAML is required to read configuration server payloads"
                ) from exc
            pending_boot_values = (
                yaml.safe_load(pending_response.pending_parameters_yaml) or {}
            )
            if not isinstance(pending_boot_values, dict):
                raise RuntimeError(
                    "configuration pending boot payload must be a mapping"
                )
            session_service = self._call_service(
                "GetConfigurationSession",
                "get_configuration_session",
            )
            session_response = session_service["call"](session_service["request"])
            if not session_response.success:
                raise RuntimeError(
                    session_response.message
                    or "configuration tuning session status is unavailable"
                )
            session_status = _decode_canonical_object(
                session_response.session_json,
                label="configuration tuning session status",
            )
            _validate_session_status(session_status)
            if pending_boot_values != session_status["pending_boot_values"]:
                raise RuntimeError(
                    "configuration pending boot services disagree on durable state"
                )
            manifest = _manifest_from_configuration_server_payload(
                raw_manifest=raw_manifest,
                declared_parameters=declared,
                current_snapshot_id=current_file,
                default_snapshot_id=default_file,
                available_snapshots=snapshots,
                pending_boot_values=pending_boot_values,
                tuning_session=session_status,
            )
            self._manifest_cache = (time.monotonic(), manifest)
            return manifest.model_copy(deep=True)

    def apply(self, request: ConfigurationApplyRequest) -> ConfigurationApplyResponse:
        manifest = self.manifest()
        restart_by_parameter = {
            parameter.name: parameter.restart_required
            for node in manifest.nodes
            for group in node.groups
            for parameter in group.parameters
        }
        request_id = request.request_id or f"runtime-{uuid.uuid4().hex}"
        expected_revision = (
            manifest.status.tuning_revision
            if request.expected_revision is None
            else request.expected_revision
        )
        operator_id = request.operator_id or "runtime-api"
        document = {
            "schema": "iii.configuration-transaction-request/v1",
            "request_id": request_id,
            "expected_revision": expected_revision,
            "operator_id": operator_id,
            "edits": [
                {
                    "node_id": edit.node_id,
                    "name": edit.name,
                    "value": edit.value,
                }
                for edit in request.edits
            ],
        }
        service = self._call_service(
            "ApplyConfigurationTransaction",
            "apply_configuration_transaction",
            response_timeout_sec=CONFIGURATION_TRANSACTION_TIMEOUT_SECONDS,
        )
        service_request = service["request"]
        service_request.request_json = _canonical_json(document)
        response = service["call"](service_request)
        if not response.result_json:
            raise RuntimeError(response.message or "configuration transaction failed")
        result = _decode_canonical_object(
            response.result_json,
            label="configuration transaction result",
        )
        _validate_transaction_result(result)
        results = [
            ParameterApplyResult(
                node_id=item["node_id"],
                name=item["name"],
                success=bool(item["success"]),
                message=item.get("message"),
                applied_value=item.get("applied_value"),
                persisted_value=item.get("persisted_value"),
                restart_required=restart_by_parameter.get(
                    item["name"], RestartRequired(item.get("restart_required", "none"))
                ),
            )
            for item in result.get("results", [])
        ]
        self._invalidate_manifest()
        return ConfigurationApplyResponse(
            ok=bool(result.get("ok")) and bool(response.success),
            results=results,
            status=self.manifest().status,
            session_id=result.get("session_id"),
            transaction_id=result.get("transaction_id"),
            revision=result.get("revision"),
            transaction_status=result.get("status"),
            idempotent_replay=bool(result.get("idempotent_replay", False)),
        )

    def save_snapshot(self, request: SnapshotSaveRequest) -> SnapshotOperationResponse:
        service = self._call_service("SaveParameters", "save_parameters")
        service_request = service["request"]
        service_request.file = (
            request.overwrite_snapshot_id or _snapshot_file_from_label(request.label)
        )
        service_request.set_as_default = False
        service_request.overwrite = request.overwrite_snapshot_id is not None
        response = service["call"](service_request)
        snapshot_id = str(response.file or service_request.file)
        self._invalidate_manifest()
        manifest = self.manifest()
        return SnapshotOperationResponse(
            ok=bool(response.success),
            snapshot=_summary(
                snapshot_id,
                manifest.status.default_snapshot_id,
                manifest.status.loaded_snapshot_id,
            ),
            status=manifest.status,
            message=str(response.message) if getattr(response, "message", "") else None,
        )

    def load_snapshot(self, request: SnapshotLoadRequest) -> SnapshotOperationResponse:
        service = self._call_service("LoadParameters", "load_parameters")
        service_request = service["request"]
        service_request.file = request.snapshot_id
        service_request.set_as_default = False
        response = service["call"](service_request)
        self._invalidate_manifest()
        return SnapshotOperationResponse(
            ok=bool(response.success),
            snapshot=_summary(
                request.snapshot_id,
                self.manifest().status.default_snapshot_id,
                request.snapshot_id,
            ),
            status=self.manifest().status,
            message=str(response.message) if getattr(response, "message", "") else None,
        )

    def list_snapshots(self) -> list[SnapshotSummary]:
        service = self._call_service("GetParameterFiles", "get_parameter_files")
        response = service["call"](service["request"])
        current_file, default_file = self._current_files()
        return [
            _summary(file_name, default_file, current_file)
            for file_name in response.parameter_files
        ]

    def download_snapshot(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        service = self._call_service("GetParameterFile", "get_parameter_file")
        service_request = service["request"]
        service_request.file = request.snapshot_id
        response = service["call"](service_request)
        if not response.success:
            raise RuntimeError(
                response.message or "configuration snapshot is unavailable"
            )
        content = str(response.parameter_yaml)
        if (
            hashlib.sha256(content.encode("utf-8")).hexdigest()
            != response.content_sha256
        ):
            raise RuntimeError("configuration snapshot content identity is invalid")
        source = {
            "snapshot_id": request.snapshot_id,
            "content_type": "application/x-yaml",
            "content": content,
            "content_sha256": str(response.content_sha256),
            "download_supported": True,
        }
        return source

    def journal(
        self, *, session_id: str | None, after_sequence: int, limit: int
    ) -> dict[str, Any]:
        document = {
            "schema": "iii.configuration-journal-request/v1",
            "session_id": session_id,
            "after_sequence": after_sequence,
            "limit": limit,
        }
        service = self._call_service(
            "GetConfigurationJournal", "get_configuration_journal"
        )
        service_request = service["request"]
        service_request.request_json = _canonical_json(document)
        response = service["call"](service["request"])
        if not response.success:
            raise RuntimeError(
                response.message or "configuration journal is unavailable"
            )
        batch = _decode_canonical_object(
            response.journal_json, label="configuration journal batch"
        )
        _validate_journal_batch(batch)
        return batch

    def _ensure_tuning_session(self) -> dict[str, Any]:
        service = self._call_service(
            "EnsureConfigurationSession", "ensure_configuration_session"
        )
        response = service["call"](service["request"])
        if not response.success:
            raise RuntimeError(
                response.message or "configuration tuning session cannot be opened"
            )
        session = _decode_canonical_object(
            response.session_json,
            label="configuration tuning session status",
        )
        _validate_session_status(session)
        if session["session_id"] is None:
            raise RuntimeError("configuration tuning session was not opened")
        self._invalidate_manifest()
        return session

    def capture_source(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        downloaded = self.download_snapshot(request)
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError(
                "PyYAML is required to read configuration snapshots"
            ) from exc
        parsed = yaml.safe_load(downloaded["content"]) or {}
        values = parsed.get("/**", {}).get("ros__parameters", {})
        if not isinstance(parsed, dict) or not isinstance(values, dict):
            raise RuntimeError("configuration snapshot values are invalid")
        self._ensure_tuning_session()
        manifest = self.manifest()
        status = manifest.status
        batch = self.journal(
            session_id=status.tuning_session_id,
            after_sequence=max(0, status.tuning_journal_sequence - 1),
            limit=1,
        )
        head_entry = batch["entries"][-1] if batch["entries"] else None
        source = {
            "schema": "iii.configuration-capture-source/v1",
            "snapshot_id": request.snapshot_id,
            "snapshot_content_sha256": downloaded["content_sha256"],
            "values": dict(sorted(values.items())),
            "parameter_document": parsed,
            "target_id": status.tuning_target_id,
            "runtime_profile": status.tuning_runtime_profile,
            "release_id": status.tuning_release_id,
            "workspace_id": status.tuning_workspace_id,
            "manifest_id": status.tuning_manifest_id,
            "session_id": status.tuning_session_id,
            "baseline_id": status.tuning_baseline_id,
            "baseline_values": batch["baseline_values"],
            "session_created_at": (
                status.tuning_created_at.isoformat().replace("+00:00", "Z")
                if status.tuning_created_at is not None
                else None
            ),
            "journal_updated_at": (
                status.tuning_updated_at.isoformat().replace("+00:00", "Z")
                if status.tuning_updated_at is not None
                else None
            ),
            "journal_revision": status.tuning_revision,
            "journal_sequence": status.tuning_journal_sequence,
            "journal_checksum": status.tuning_journal_checksum,
            "journal_head_entry": head_entry,
            "pending_boot_values": status.pending_boot_values,
            "source_is_active": request.snapshot_id == status.loaded_snapshot_id,
            "source_is_default": request.snapshot_id == status.default_snapshot_id,
        }
        seal_capture(source)
        return source

    def delete_snapshot(self, request: dict[str, Any]) -> dict[str, Any]:
        service = self._call_service("DeleteParameterFile", "delete_parameter_file")
        service_request = service["request"]
        service_request.request_json = _canonical_json(request)
        response = service["call"](service_request)
        if not response.success:
            raise RuntimeError(
                response.message or "configuration snapshot deletion failed"
            )
        result = _decode_canonical_object(
            response.result_json, label="configuration snapshot delete result"
        )
        if (
            set(result)
            != {
                "schema",
                "snapshot_id",
                "content_sha256",
                "forced",
                "deleted",
            }
            or result.get("schema") != "iii.configuration-snapshot-delete-result/v1"
            or result.get("deleted") is not True
        ):
            raise RuntimeError("configuration snapshot delete result is invalid")
        self._invalidate_manifest()
        return result

    def set_default_snapshot(
        self, request: SnapshotSetDefaultRequest
    ) -> SnapshotOperationResponse:
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
        self._invalidate_manifest()
        return SnapshotOperationResponse(
            ok=bool(response.success),
            snapshot=_summary(
                request.snapshot_id, request.snapshot_id, request.snapshot_id
            ),
            status=self.manifest().status,
            message=str(response.message) if getattr(response, "message", "") else None,
        )

    def activate_pending_boot_parameters(self) -> dict[str, Any]:
        service = self._call_service(
            "ActivatePendingBootParameters",
            "activate_pending_boot_parameters",
        )
        response = service["call"](service["request"])
        if not response.success:
            raise RuntimeError(
                response.message
                or "configuration server rejected pending boot activation"
            )
        self._invalidate_manifest()
        return {
            "success": True,
            "message": str(response.message),
            "activated_parameter_names": list(response.activated_parameter_names),
        }

    def _invalidate_manifest(self) -> None:
        with self._manifest_lock:
            self._manifest_cache = None

    def _current_files(self) -> tuple[str | None, str | None]:
        service = self._call_service(
            "GetCurrentParameterFile", "get_current_parameter_file"
        )
        response = service["call"](service["request"])
        return (
            (
                str(response.current_parameter_file)
                if response.current_parameter_file
                else None
            ),
            (
                str(response.default_parameter_file)
                if response.default_parameter_file
                else None
            ),
        )

    def _load_yaml_service(
        self, service_type: str, service_name: str, response_attr: str
    ) -> dict[str, Any]:
        service = self._call_service(service_type, service_name)
        response = service["call"](service["request"])
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError(
                "PyYAML is required to read configuration server payloads"
            ) from exc
        return yaml.safe_load(getattr(response, response_attr)) or {}

    def _call_service(
        self,
        service_type_name: str,
        service_name: str,
        *,
        response_timeout_sec: float = SERVICE_RESPONSE_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        try:
            from iii_drone_interfaces import srv as srv_module
        except Exception as exc:
            raise RuntimeError(
                "ROS configuration server services are unavailable"
            ) from exc

        service_type = getattr(srv_module, service_type_name)
        fq_name = f"{self.namespace}/{service_name}"
        node = self.node_provider() if self.node_provider is not None else self.node
        if node is None:
            raise RuntimeError("runtime ROS node is unavailable")
        if node is not self._client_node:
            self._clients.clear()
            self._client_node = node
        client = self._clients.get(fq_name)
        if client is None:
            client = create_reentrant_client(node, service_type, fq_name)
            self._clients[fq_name] = client
        if not client.wait_for_service(timeout_sec=SERVICE_DISCOVERY_TIMEOUT_SECONDS):
            raise RuntimeError(f"configuration server service unavailable: {fq_name}")

        def call(request: Any) -> Any:
            return wait_for_service_response(
                client,
                request,
                timeout_sec=response_timeout_sec,
                label=fq_name,
            )

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
        vehicle_state_provider: Callable[[], Any],
    ):
        self.mission_state_provider = mission_state_provider
        self.operation_state_provider = operation_state_provider
        self.vehicle_state_provider = vehicle_state_provider

    def mutating_permission(self, *, constant: bool = False) -> ConfigurationPermission:
        reasons: list[str] = []
        mission = self.mission_state_provider()
        operation = self.operation_state_provider()
        vehicle = self.vehicle_state_provider()
        if (
            mission.latest.get("mission_active") is True
            or mission.mission_state == "active"
        ):
            reasons.append("configuration writes are disabled in Mission mode")
        if (
            operation.latest.get("operation_active") is True
            or operation.active_operation_id
        ):
            reasons.append(
                "configuration writes are disabled while a custom operation action is active"
            )
        if vehicle.source_availability != SourceAvailability.AVAILABLE:
            reasons.append("vehicle state is unavailable")
        elif vehicle.freshness != Freshness.FRESH:
            reasons.append("vehicle state is stale")
        elif vehicle.armed is None or vehicle.in_air is None:
            reasons.append("vehicle armed/landed state is unknown")
        else:
            landed_disarmed = vehicle.armed is False and vehicle.in_air is False
            mode = str(vehicle.nav_state or vehicle.flight_mode or "").strip().lower()
            in_hold = mode in {"hold", "auto_loiter", "4"}
            if constant and not landed_disarmed:
                reasons.append(
                    "constant parameters require the aircraft to be disarmed and landed"
                )
            elif not constant and not (landed_disarmed or in_hold):
                reasons.append(
                    "live parameters require PX4 Hold or a disarmed and landed aircraft"
                )
        return ConfigurationPermission(allowed=not reasons, reasons=reasons)


class ConfigurationRuntimeController:
    def __init__(
        self,
        *,
        adapter: ConfigurationServerAdapter,
        permission_gate: ConfigurationPermissionGate,
        revision_sink: Callable[[ConfigurationApplyResponse], None] | None = None,
        state_sink: Callable[[], None] | None = None,
    ):
        self.adapter = adapter
        self.permission_gate = permission_gate
        self.revision_sink = revision_sink
        self.state_sink = state_sink
        self._mirror_ack: dict[str, Any] | None = None
        self._mirror_lock = threading.RLock()

    def manifest(self) -> ConfigurationManifest:
        return self._with_permissions(self._with_mirror_status(self.adapter.manifest()))

    def state(self) -> ConfigurationDomainState:
        try:
            manifest = self.manifest()
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
                "available_snapshots": [
                    snapshot.model_dump(mode="json")
                    for snapshot in manifest.available_snapshots
                ],
                "permissions": self._permission_payload(status),
            },
            active_snapshot_id=status.loaded_snapshot_id,
            pending_edits=status.pending_edits,
            unsaved=status.unsaved,
            non_default=status.non_default,
        )

    def apply(self, request: ConfigurationApplyRequest) -> ConfigurationApplyResponse:
        permission = self.permission_gate.mutating_permission()
        if not permission.allowed:
            message = "; ".join(permission.reasons)
            return ConfigurationApplyResponse(
                ok=False,
                results=[
                    ParameterApplyResult(
                        node_id=edit.node_id,
                        name=edit.name,
                        success=False,
                        message=message,
                    )
                    for edit in request.edits
                ],
                status=self._status_after_denial(),
            )
        try:
            manifest = self.manifest()
        except Exception as exc:
            return ConfigurationApplyResponse(
                ok=False,
                results=[
                    ParameterApplyResult(
                        node_id=edit.node_id,
                        name=edit.name,
                        success=False,
                        message=str(exc),
                    )
                    for edit in request.edits
                ],
                status=ConfigurationStatus(configuration_server_available=False),
            )
        definitions = {
            (parameter.node_id, parameter.name): parameter
            for node in manifest.nodes
            for group in node.groups
            for parameter in group.parameters
        }
        denied: dict[tuple[str, str], ParameterApplyResult] = {}
        allowed: list[ParameterEdit] = []
        for edit in request.edits:
            definition = definitions.get((edit.node_id, edit.name))
            if definition is None:
                denied[(edit.node_id, edit.name)] = ParameterApplyResult(
                    node_id=edit.node_id,
                    name=edit.name,
                    success=False,
                    message="parameter is not present in the configuration-server manifest",
                )
            elif definition.readonly or not definition.apply_allowed:
                denied[(edit.node_id, edit.name)] = ParameterApplyResult(
                    node_id=edit.node_id,
                    name=edit.name,
                    success=False,
                    message="; ".join(definition.apply_rejection_reasons)
                    or "parameter is read-only",
                    restart_required=definition.restart_required,
                )
            else:
                allowed.append(edit)
        applied = (
            self.adapter.apply(
                ConfigurationApplyRequest(
                    edits=allowed,
                    request_id=request.request_id,
                    expected_revision=request.expected_revision,
                    operator_id=request.operator_id,
                )
            )
            if allowed
            else None
        )
        applied_by_key = (
            {(result.node_id, result.name): result for result in applied.results}
            if applied
            else {}
        )
        results = [
            denied.get((edit.node_id, edit.name))
            or applied_by_key[(edit.node_id, edit.name)]
            for edit in request.edits
        ]
        status = applied.status if applied else manifest.status
        response = ConfigurationApplyResponse(
            ok=all(result.success for result in results),
            results=results,
            status=status,
            session_id=applied.session_id if applied else None,
            transaction_id=applied.transaction_id if applied else None,
            revision=applied.revision if applied else None,
            transaction_status=applied.transaction_status if applied else None,
            idempotent_replay=applied.idempotent_replay if applied else False,
        )
        if (
            response.ok
            and response.transaction_status in {"committed", "recovered-commit"}
            and not response.idempotent_replay
            and self.revision_sink is not None
        ):
            self.revision_sink(response)
        return response

    def save_snapshot(self, request: SnapshotSaveRequest) -> SnapshotOperationResponse:
        return self._mutating_snapshot_operation(
            lambda: self.adapter.save_snapshot(request)
        )

    def load_snapshot(self, request: SnapshotLoadRequest) -> SnapshotOperationResponse:
        return self._mutating_snapshot_operation(
            lambda: self.adapter.load_snapshot(request)
        )

    def set_default_snapshot(
        self, request: SnapshotSetDefaultRequest
    ) -> SnapshotOperationResponse:
        return self._mutating_snapshot_operation(
            lambda: self.adapter.set_default_snapshot(request)
        )

    def list_snapshots(self) -> list[SnapshotSummary]:
        return self.adapter.list_snapshots()

    def download_snapshot(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        return self.adapter.download_snapshot(request)

    def journal(
        self, *, session_id: str | None, after_sequence: int, limit: int = 250
    ) -> dict[str, Any]:
        return self.adapter.journal(
            session_id=session_id,
            after_sequence=after_sequence,
            limit=limit,
        )

    def capture_source(self, request: SnapshotDownloadRequest) -> dict[str, Any]:
        return self.adapter.capture_source(request)

    def delete_snapshot(self, request: dict[str, Any]) -> dict[str, Any]:
        permission = self.permission_gate.mutating_permission()
        if not permission.allowed:
            raise RuntimeError("; ".join(permission.reasons))
        result = self.adapter.delete_snapshot(request)
        if self.state_sink is not None:
            self.state_sink()
        return result

    def acknowledge_mirror(
        self, acknowledgement: Mapping[str, Any]
    ) -> ConfigurationStatus:
        fields = {
            "schema",
            "session_id",
            "revision",
            "sequence",
            "checksum",
            "mirror_id",
        }
        if (
            set(acknowledgement) != fields
            or acknowledgement.get("schema") != "iii.configuration-mirror-ack/v1"
        ):
            raise RuntimeError("configuration mirror acknowledgement is invalid")
        manifest = self.adapter.manifest()
        status = manifest.status
        if (
            acknowledgement["session_id"] != status.tuning_session_id
            or acknowledgement["revision"] != status.tuning_revision
            or acknowledgement["sequence"] != status.tuning_journal_sequence
            or acknowledgement["checksum"] != status.tuning_journal_checksum
            or not isinstance(acknowledgement["mirror_id"], str)
            or not _SHA256_PATTERN.fullmatch(acknowledgement["mirror_id"])
        ):
            raise RuntimeError(
                "configuration mirror acknowledgement does not match the authoritative journal head"
            )
        with self._mirror_lock:
            self._mirror_ack = dict(acknowledgement)
        return self.manifest().status

    def _mutating_snapshot_operation(
        self, operation: Callable[[], SnapshotOperationResponse]
    ) -> SnapshotOperationResponse:
        permission = self.permission_gate.mutating_permission()
        if not permission.allowed:
            return SnapshotOperationResponse(
                ok=False,
                status=self._status_after_denial(),
                message="; ".join(permission.reasons),
            )
        try:
            status = self.adapter.manifest().status
        except Exception as exc:
            return SnapshotOperationResponse(ok=False, message=str(exc))
        if status.configuration_divergent:
            return SnapshotOperationResponse(
                ok=False,
                status=status,
                message="configuration is divergent; reconcile exact observed values before further writes",
            )
        response = operation()
        if response.ok and self.state_sink is not None:
            self.state_sink()
        return response

    def _status_after_denial(self) -> ConfigurationStatus:
        try:
            return self.adapter.manifest().status
        except Exception:
            return ConfigurationStatus()

    def _permission_payload(
        self, status: ConfigurationStatus | None = None
    ) -> dict[str, Any]:
        permission = self.permission_gate.mutating_permission()
        constant_permission = self.permission_gate.mutating_permission(constant=True)
        divergence_reason = "configuration is divergent; reconcile exact observed values before further writes"
        write_rejections = list(permission.reasons)
        constant_rejections = list(constant_permission.reasons)
        if status is not None and status.configuration_divergent:
            write_rejections.append(divergence_reason)
            constant_rejections.append(divergence_reason)
        return {
            "writes_allowed": permission.allowed and not write_rejections,
            "write_rejections": write_rejections,
            "constant_writes_allowed": constant_permission.allowed
            and not constant_rejections,
            "constant_write_rejections": constant_rejections,
        }

    def activate_pending_boot_parameters(self) -> dict[str, Any]:
        return self.adapter.activate_pending_boot_parameters()

    def parameter_cold_restart_permission(self) -> ConfigurationPermission:
        return self.permission_gate.mutating_permission(constant=True)

    def _with_permissions(
        self, manifest: ConfigurationManifest
    ) -> ConfigurationManifest:
        live_permission = self.permission_gate.mutating_permission()
        constant_permission = self.permission_gate.mutating_permission(constant=True)
        for node in manifest.nodes:
            for group in node.groups:
                for parameter in group.parameters:
                    permission = (
                        constant_permission if parameter.constant else live_permission
                    )
                    reasons = list(permission.reasons)
                    if parameter.readonly:
                        reasons.append("parameter is read-only")
                    if manifest.status.configuration_divergent:
                        reasons.append(
                            "configuration is divergent; reconcile exact observed values before further writes"
                        )
                    parameter.apply_allowed = (
                        permission.allowed
                        and not parameter.readonly
                        and not manifest.status.configuration_divergent
                    )
                    parameter.apply_rejection_reasons = reasons
        return manifest

    def _with_mirror_status(
        self, manifest: ConfigurationManifest
    ) -> ConfigurationManifest:
        status = manifest.status
        if status.tuning_session_id is None:
            status.mirror_state = "not-required"
            return manifest
        with self._mirror_lock:
            acknowledgement = (
                None if self._mirror_ack is None else dict(self._mirror_ack)
            )
        if (
            acknowledgement is not None
            and acknowledgement.get("session_id") == status.tuning_session_id
            and acknowledgement.get("revision") == status.tuning_revision
            and acknowledgement.get("sequence") == status.tuning_journal_sequence
            and acknowledgement.get("checksum") == status.tuning_journal_checksum
        ):
            status.mirror_state = "current"
            status.mirror_ack_revision = int(acknowledgement["revision"])
            status.mirror_ack_sequence = int(acknowledgement["sequence"])
            status.mirror_ack_checksum = acknowledgement["checksum"]
            status.mirror_error = None
        else:
            status.mirror_state = "degraded"
            status.mirror_ack_revision = (
                acknowledgement.get("revision") if acknowledgement else None
            )
            status.mirror_ack_sequence = (
                acknowledgement.get("sequence") if acknowledgement else None
            )
            status.mirror_ack_checksum = (
                acknowledgement.get("checksum") if acknowledgement else None
            )
            status.mirror_error = "No verified host mirror acknowledgement for the authoritative journal head."
        return manifest


class ConfigurationCommandHandlers:
    def __init__(
        self, *, controller: ConfigurationRuntimeController, event_log: RuntimeEventLog
    ):
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
                edits = [
                    ParameterEdit(**edit)
                    for edit in request.parameters.get("edits", [])
                ]
                result = self.controller.apply(
                    ConfigurationApplyRequest(
                        edits=edits,
                        request_id=request.request_id,
                        expected_revision=request.parameters.get("expected_revision"),
                        operator_id=request.client_label or "operator",
                    )
                )
                if not result.ok:
                    return self._reject(
                        request,
                        _failure_message(result),
                        ErrorCode.FORBIDDEN,
                        result.model_dump(mode="json"),
                    )
                return self._ok(
                    request, {"configuration": result.model_dump(mode="json")}
                )
            if request.command_id == CommandId.CONFIGURATION_SAVE_SNAPSHOT.value:
                result = self.controller.save_snapshot(
                    SnapshotSaveRequest(**request.parameters)
                )
                return self._snapshot_response(request, result)
            if request.command_id == CommandId.CONFIGURATION_LOAD_SNAPSHOT.value:
                result = self.controller.load_snapshot(
                    SnapshotLoadRequest(**request.parameters)
                )
                return self._snapshot_response(request, result)
            if request.command_id == CommandId.CONFIGURATION_SET_DEFAULT_SNAPSHOT.value:
                result = self.controller.set_default_snapshot(
                    SnapshotSetDefaultRequest(**request.parameters)
                )
                return self._snapshot_response(request, result)
            if request.command_id == CommandId.CONFIGURATION_LIST_SNAPSHOTS.value:
                return self._ok(
                    request,
                    {
                        "snapshots": [
                            snapshot.model_dump(mode="json")
                            for snapshot in self.controller.list_snapshots()
                        ]
                    },
                )
            if request.command_id == CommandId.CONFIGURATION_DOWNLOAD_SNAPSHOT.value:
                result = self.controller.download_snapshot(
                    SnapshotDownloadRequest(**request.parameters)
                )
                return self._ok(request, {"snapshot": result})
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.HANDLER_UNAVAILABLE, None)
        return self._reject(
            request,
            "unsupported configuration command",
            ErrorCode.HANDLER_UNAVAILABLE,
            None,
        )

    def _snapshot_response(
        self, request: CommandRequest, result: SnapshotOperationResponse
    ) -> ActionStartResponse:
        if not result.ok:
            return self._reject(
                request,
                result.message or "configuration operation rejected",
                ErrorCode.FORBIDDEN,
                result.model_dump(mode="json"),
            )
        return self._ok(request, {"configuration": result.model_dump(mode="json")})

    def _ok(
        self, request: CommandRequest, result: dict[str, Any]
    ) -> ActionStartResponse:
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
    pending_boot_values: dict[str, Any] | None = None,
    tuning_session: dict[str, Any] | None = None,
) -> ConfigurationManifest:
    pending_boot_values = pending_boot_values or {}
    tuning_session = tuning_session or {}
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
                persisted_value=pending_boot_values.get(parameter_name),
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
    status = _configuration_status(
        current_snapshot_id,
        default_snapshot_id,
        pending_boot_values,
        tuning_session,
    )
    return ConfigurationManifest(
        nodes=nodes,
        loaded_snapshot=(
            _summary(current_snapshot_id, default_snapshot_id, current_snapshot_id)
            if current_snapshot_id
            else None
        ),
        default_snapshot=(
            _summary(default_snapshot_id, default_snapshot_id, current_snapshot_id)
            if default_snapshot_id
            else None
        ),
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


def _parameter_definition(
    *,
    node_id: str,
    group_id: str,
    name: str,
    entry: dict[str, Any],
    persisted_value: Any = None,
) -> ParameterDefinition:
    value_type = _parameter_value_type(str(entry.get("type", "string")))
    current_value = entry.get("value")
    default_value = entry.get("default_value", entry.get("default"))
    return ParameterDefinition(
        node_id=node_id,
        group_id=group_id,
        name=name,
        value_type=value_type,
        current_value=current_value,
        active_value=current_value,
        persisted_value=(
            persisted_value if persisted_value is not None else current_value
        ),
        loaded_value=entry.get("loaded_value", current_value),
        default_value=default_value if default_value is not None else current_value,
        description=entry.get("description"),
        constraints=_constraints(entry),
        restart_required=_restart_required(entry),
        readonly=bool(entry.get("readonly", False) or entry.get("read_only", False)),
        constant=bool(entry.get("constant", False)),
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
    minimum, minimum_expression = _numeric_or_expression(
        entry.get("minimum", entry.get("min"))
    )
    maximum, maximum_expression = _numeric_or_expression(
        entry.get("maximum", entry.get("max"))
    )
    step, step_expression = _numeric_or_expression(entry.get("step"))
    values = {
        "minimum": minimum,
        "maximum": maximum,
        "step": step,
        "minimum_expression": minimum_expression,
        "maximum_expression": maximum_expression,
        "step_expression": step_expression,
        "choices": entry.get("choices", entry.get("options")),
        "regex": entry.get("regex"),
        "unit": entry.get("unit"),
    }
    if all(value is None for value in values.values()):
        return None
    return ParameterConstraint(**values)


def _numeric_or_expression(value: Any) -> tuple[float | int | None, str | None]:
    if isinstance(value, bool) or value is None:
        return None, None
    if isinstance(value, (int, float)):
        return value, None
    if isinstance(value, str) and value.strip():
        return None, value.strip()
    return None, None


def _restart_required(entry: dict[str, Any]) -> RestartRequired:
    explicit = entry.get("restart_required")
    if explicit in {
        RestartRequired.RUNTIME.value,
        RestartRequired.NODE.value,
        RestartRequired.NONE.value,
    }:
        return RestartRequired(explicit)
    if entry.get("constant") is True:
        return RestartRequired.RUNTIME
    if entry.get("static") is True:
        return RestartRequired.NODE
    return RestartRequired.NONE


def _node_id_for_parameter(
    parameter_name: str, declared_parameters: dict[str, Any]
) -> str:
    declared_nodes = declared_parameters.get(parameter_name) or declared_parameters.get(
        parameter_name.lstrip("/")
    )
    if isinstance(declared_nodes, list) and declared_nodes:
        return str(declared_nodes[0]).strip("/") or "configuration"
    parts = [part for part in parameter_name.split("/") if part]
    return parts[0] if parts else "configuration"


def _group_id_for_parameter(parameter_name: str) -> str:
    parts = [part for part in parameter_name.split("/") if part]
    if len(parts) <= 1:
        return "general"
    return "/".join(parts[:-1])


def _configuration_status(
    current_snapshot_id: str | None,
    default_snapshot_id: str | None,
    pending_boot_values: dict[str, Any] | None = None,
    tuning_session: dict[str, Any] | None = None,
) -> ConfigurationStatus:
    pending_boot_values = pending_boot_values or {}
    tuning_session = tuning_session or {}
    unsaved = bool(
        current_snapshot_id and current_snapshot_id.startswith(RUNTIME_SNAPSHOT_PREFIX)
    )
    non_default = bool(
        current_snapshot_id
        and default_snapshot_id
        and current_snapshot_id != default_snapshot_id
        and not unsaved
    )
    return ConfigurationStatus(
        configuration_server_available=True,
        pending_edits=False,
        unsaved=unsaved,
        non_default=non_default,
        loaded_snapshot_id=current_snapshot_id,
        default_snapshot_id=default_snapshot_id,
        pending_restart=bool(pending_boot_values),
        pending_constant_names=sorted(pending_boot_values),
        pending_boot_values=dict(pending_boot_values),
        tuning_session_id=tuning_session.get("session_id"),
        tuning_baseline_id=tuning_session.get("baseline_id"),
        tuning_target_id=tuning_session.get("target_id"),
        tuning_runtime_profile=tuning_session.get("runtime_profile"),
        tuning_release_id=tuning_session.get("release_id"),
        tuning_workspace_id=tuning_session.get("workspace_id"),
        tuning_manifest_id=tuning_session.get("manifest_id"),
        tuning_revision=int(tuning_session.get("revision", 0)),
        tuning_journal_sequence=int(tuning_session.get("wal_sequence", 0)),
        tuning_journal_checksum=tuning_session.get("wal_checksum"),
        tuning_created_at=tuning_session.get("created_at"),
        tuning_updated_at=tuning_session.get("updated_at"),
        configuration_divergent=bool(tuning_session.get("divergent", False)),
        divergent_observations=dict(tuning_session.get("divergent_observations") or {}),
    )


def _summary(
    snapshot_id: str | None,
    default_snapshot_id: str | None,
    loaded_snapshot_id: str | None,
) -> SnapshotSummary:
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
