from types import SimpleNamespace
import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from iii_drone_contracts import (
    CommandId,
    ConfigurationApplyRequest,
    ConfigurationApplyResponse,
    ConfigurationManifest,
    ConfigurationStatus,
    ParameterApplyResult,
    ParameterDefinition,
    ParameterEdit,
    ParameterGroup,
    ParameterNode,
    ParameterValueType,
    RestartRequired,
    VehicleDomainState,
    SnapshotOperationResponse,
    SnapshotDownloadRequest,
    SnapshotSaveRequest,
    SnapshotSummary,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api import configuration as configuration_module
from iii_drone_runtime.api.configuration import (
    ConfigurationPermission,
    ConfigurationRuntimeController,
    RosConfigurationServerAdapter,
    _decode_canonical_object,
    _manifest_from_configuration_server_payload,
    _validate_session_status,
    _validate_transaction_result,
)
from iii_drone_runtime.api.mission_status import MissionStatusCache
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache


class _FakeConfigurationServer:
    def __init__(self):
        self.current_snapshot_id = "tracked/default.yaml"
        self.default_snapshot_id = "tracked/default.yaml"
        self.values = {
            "/control/gains/p": 1.5,
            "/control/gains/i": 0.3,
            "/control/static_frame": "map",
            "/control/immutable_name": "alpha",
        }
        self.snapshots = {
            "tracked/default.yaml": dict(self.values),
            "snapshots/tuned.yaml": {**self.values, "/control/gains/p": 2.0},
        }
        self.applied = []
        self.saved = []
        self.loaded = []
        self.defaults = []
        self.pending = {}
        self.revision = 0
        self.session_id = "a" * 64

    def manifest(self):
        status = self._status()
        snapshots = self.list_snapshots()
        return ConfigurationManifest(
            nodes=[
                ParameterNode(
                    node_id="controller",
                    label="Controller",
                    groups=[
                        ParameterGroup(
                            node_id="controller",
                            group_id="control/gains",
                            label="Control Gains",
                            parameters=[
                                ParameterDefinition(
                                    node_id="controller",
                                    group_id="control/gains",
                                    name="/control/gains/p",
                                    value_type=ParameterValueType.FLOAT,
                                    current_value=self.values["/control/gains/p"],
                                    loaded_value=self.snapshots[
                                        self.current_snapshot_id
                                    ]["/control/gains/p"],
                                    default_value=self.snapshots[
                                        self.default_snapshot_id
                                    ]["/control/gains/p"],
                                ),
                                ParameterDefinition(
                                    node_id="controller",
                                    group_id="control/gains",
                                    name="/control/gains/i",
                                    value_type=ParameterValueType.FLOAT,
                                    current_value=self.values["/control/gains/i"],
                                    loaded_value=self.snapshots[
                                        self.current_snapshot_id
                                    ]["/control/gains/i"],
                                    default_value=self.snapshots[
                                        self.default_snapshot_id
                                    ]["/control/gains/i"],
                                ),
                            ],
                        ),
                        ParameterGroup(
                            node_id="controller",
                            group_id="control",
                            label="Control",
                            parameters=[
                                ParameterDefinition(
                                    node_id="controller",
                                    group_id="control",
                                    name="/control/static_frame",
                                    value_type=ParameterValueType.STRING,
                                    current_value=self.values["/control/static_frame"],
                                    restart_required=RestartRequired.NODE,
                                ),
                                ParameterDefinition(
                                    node_id="controller",
                                    group_id="control",
                                    name="/control/immutable_name",
                                    value_type=ParameterValueType.STRING,
                                    current_value=self.values[
                                        "/control/immutable_name"
                                    ],
                                    active_value=self.values["/control/immutable_name"],
                                    persisted_value=self.pending.get(
                                        "/control/immutable_name",
                                        self.values["/control/immutable_name"],
                                    ),
                                    restart_required=RestartRequired.RUNTIME,
                                    constant=True,
                                ),
                            ],
                        ),
                    ],
                )
            ],
            loaded_snapshot=SnapshotSummary(
                snapshot_id=self.current_snapshot_id,
                label=self.current_snapshot_id,
                is_loaded=True,
                is_default=self.current_snapshot_id == self.default_snapshot_id,
            ),
            default_snapshot=SnapshotSummary(
                snapshot_id=self.default_snapshot_id,
                label=self.default_snapshot_id,
                is_default=True,
                is_loaded=self.current_snapshot_id == self.default_snapshot_id,
            ),
            available_snapshots=snapshots,
            status=status,
        )

    def apply(self, request: ConfigurationApplyRequest):
        results = []
        for edit in request.edits:
            self.applied.append(edit)
            if edit.name == "/control/immutable_name":
                self.pending[edit.name] = edit.value
            else:
                self.values[edit.name] = edit.value
            results.append(
                ParameterApplyResult(
                    node_id=edit.node_id,
                    name=edit.name,
                    success=True,
                    applied_value=edit.value,
                    persisted_value=edit.value,
                    restart_required=(
                        RestartRequired.RUNTIME
                        if edit.name == "/control/immutable_name"
                        else (
                            RestartRequired.NODE
                            if edit.name == "/control/static_frame"
                            else RestartRequired.NONE
                        )
                    ),
                )
            )
        self.current_snapshot_id = "snapshots/runtime_parameters_test.yaml"
        self.snapshots[self.current_snapshot_id] = dict(self.values)
        self.revision += 1
        return ConfigurationApplyResponse(
            ok=all(result.success for result in results),
            results=results,
            status=self._status(),
            session_id=self.session_id,
            transaction_id=f"{self.revision:x}" * 64,
            revision=self.revision,
            transaction_status="committed",
        )

    def activate_pending_boot_parameters(self):
        names = sorted(self.pending)
        self.values.update(self.pending)
        self.pending.clear()
        return {"success": True, "activated_parameter_names": names}

    def save_snapshot(self, request):
        snapshot_id = request.overwrite_snapshot_id or f"snapshots/{request.label}.yaml"
        self.saved.append(request)
        self.snapshots[snapshot_id] = dict(self.values)
        self.current_snapshot_id = snapshot_id
        return SnapshotOperationResponse(
            ok=True,
            snapshot=SnapshotSummary(
                snapshot_id=snapshot_id, label=snapshot_id, is_loaded=True
            ),
            status=self._status(),
        )

    def load_snapshot(self, request):
        self.loaded.append(request.snapshot_id)
        self.values = dict(self.snapshots[request.snapshot_id])
        self.current_snapshot_id = request.snapshot_id
        return SnapshotOperationResponse(
            ok=True,
            snapshot=SnapshotSummary(
                snapshot_id=request.snapshot_id, label=request.snapshot_id
            ),
            status=self._status(),
        )

    def list_snapshots(self):
        return [
            SnapshotSummary(
                snapshot_id=snapshot_id,
                label=snapshot_id,
                is_default=snapshot_id == self.default_snapshot_id,
                is_loaded=snapshot_id == self.current_snapshot_id,
            )
            for snapshot_id in sorted(self.snapshots)
        ]

    def download_snapshot(self, request):
        return {
            "snapshot_id": request.snapshot_id,
            "content_type": "application/x-yaml",
            "content": f"snapshot: {request.snapshot_id}\n",
            "download_supported": True,
        }

    def capture_source(self, request):
        return {
            "schema": "iii.configuration-capture-source/v1",
            "snapshot_id": request.snapshot_id,
            "runtime_profile": "sim",
        }

    def set_default_snapshot(self, request):
        self.defaults.append(request.snapshot_id)
        self.default_snapshot_id = request.snapshot_id
        return SnapshotOperationResponse(
            ok=True,
            snapshot=SnapshotSummary(
                snapshot_id=request.snapshot_id, label=request.snapshot_id
            ),
            status=self._status(),
        )

    def _status(self):
        unsaved = self.current_snapshot_id.startswith("snapshots/runtime_parameters_")
        return ConfigurationStatus(
            configuration_server_available=True,
            unsaved=unsaved,
            non_default=self.current_snapshot_id != self.default_snapshot_id
            and not unsaved,
            loaded_snapshot_id=self.current_snapshot_id,
            default_snapshot_id=self.default_snapshot_id,
            pending_restart=bool(self.pending),
            pending_constant_names=sorted(self.pending),
            tuning_session_id=self.session_id if self.revision else None,
            tuning_baseline_id="b" * 64 if self.revision else None,
            tuning_target_id="sim",
            tuning_runtime_profile="sim",
            tuning_release_id="c" * 64,
            tuning_workspace_id="workspace-test",
            tuning_manifest_id="d" * 64,
            tuning_revision=self.revision,
            tuning_journal_sequence=self.revision * 2,
            tuning_journal_checksum="e" * 64 if self.revision else None,
        )


def _mission_cache(*, active=False):
    cache = MissionStatusCache()
    cache.handle_message(
        SimpleNamespace(
            active_catalog_id="inspection-production",
            catalog_hash="sha256:" + "a" * 64,
            active_entry_hash="sha256:" + "b" * 64,
            default_catalog_id="inspection-production",
            configuration_profile="sim",
            classification="production",
            compatible_profiles=["real", "opti_track", "sim"],
            temporary_override=False,
            experimental=False,
            experimental_warning="",
            catalog_ready=True,
            catalog_error="",
            mission_active=active,
            mission_state_label="active" if active else "ready",
            required_modes=["mission"],
            registered_modes=["mission"],
            owned_mode="Mission",
            control_owner="mission" if active else "",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )
    return cache


def _operation_cache(*, active=False):
    cache = CustomOperationStatusCache()
    cache.handle_message(
        SimpleNamespace(
            operation_state_label="active" if active else "ready",
            operation_active=active,
            active_operation="hover" if active else "",
            custom_operation_modes_registered=True,
            required_modes=["CustomOperation"],
            registered_modes=["CustomOperation"],
            owned_mode="CustomOperation",
            control_owner="custom_operation" if active else "",
            cancel_available=active,
            degraded=False,
            degraded_reasons=[],
        )
    )
    return cache


def _vehicle_provider(
    *, armed=False, in_air=False, mode="hold", fresh=True, available=True
):
    state = VehicleDomainState(
        freshness=Freshness.FRESH if fresh else Freshness.STALE,
        source_availability=(
            SourceAvailability.AVAILABLE
            if available
            else SourceAvailability.UNAVAILABLE
        ),
        armed=armed,
        in_air=in_air,
        nav_state=mode,
    )
    return SimpleNamespace(
        state=lambda: state,
        dangerous_command_rejection_reason=lambda: None,
    )


def _client(
    adapter, *, mission_active=False, operation_active=False, vehicle_provider=None
):
    if vehicle_provider is None:
        mode = (
            "mission"
            if mission_active
            else "custom_operation" if operation_active else "hold"
        )
        vehicle_provider = _vehicle_provider(mode=mode)
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
                lease_timeout_seconds=60.0,
            ),
            mission_status=_mission_cache(active=mission_active),
            operation_status=_operation_cache(active=operation_active),
            configuration_adapter=adapter,
            px4_state_provider=vehicle_provider,
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()[
        "session_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_ros_configuration_adapter_resolves_runtime_node_lazily_and_rebuilds_clients(
    monkeypatch,
):
    first_node = object()
    second_node = object()
    current_node = [None]
    created_with = []

    class _Client:
        def wait_for_service(self, timeout_sec):
            return True

    def create_client(node, service_type, service_name):
        del service_type, service_name
        created_with.append(node)
        return _Client()

    monkeypatch.setattr(configuration_module, "create_reentrant_client", create_client)
    adapter = RosConfigurationServerAdapter(node_provider=lambda: current_node[0])

    with pytest.raises(RuntimeError, match="runtime ROS node is unavailable"):
        adapter._call_service("GetParameterYaml", "get_parameter_yaml")

    current_node[0] = first_node
    adapter._call_service("GetParameterYaml", "get_parameter_yaml")
    adapter._call_service("GetParameterYaml", "get_parameter_yaml")
    current_node[0] = second_node
    adapter._call_service("GetParameterYaml", "get_parameter_yaml")

    assert created_with == [first_node, second_node]


def test_ros_configuration_adapter_caches_manifest_and_invalidates_after_mutation(
    monkeypatch,
):
    adapter = RosConfigurationServerAdapter(node=object())
    calls = {"yaml": 0, "files": 0, "snapshots": 0, "pending": 0, "session": 0}

    def load_yaml(_service_type, service_name, _response_attr):
        calls["yaml"] += 1
        if service_name == "get_parameter_yaml":
            return {"control": {"gain": {"type": "float", "value": 1.0}}}
        return {"/control/gain": ["/controller"]}

    def current_files():
        calls["files"] += 1
        return "tracked/default.yaml", "tracked/default.yaml"

    def snapshots():
        calls["snapshots"] += 1
        return []

    def call_service(_service_type, service_name):
        assert service_name in {
            "get_pending_boot_parameters",
            "get_configuration_session",
        }
        calls[
            "pending" if service_name == "get_pending_boot_parameters" else "session"
        ] += 1
        return {
            "request": object(),
            "call": lambda _request: (
                SimpleNamespace(pending_parameters_yaml="{}")
                if service_name == "get_pending_boot_parameters"
                else SimpleNamespace(
                    success=True,
                    message="",
                    session_json=json.dumps(
                        {
                            "schema": "iii.configuration-session-status/v1",
                            "session_id": None,
                            "baseline_id": None,
                            "target_id": "sim",
                            "runtime_profile": "sim",
                            "release_id": "a" * 64,
                            "workspace_id": "workspace-test",
                            "manifest_id": "b" * 64,
                            "revision": 0,
                            "wal_sequence": 0,
                            "wal_checksum": None,
                            "created_at": None,
                            "updated_at": None,
                            "active_values": {},
                            "persisted_values": {},
                            "pending_boot_values": {},
                            "divergent": False,
                            "divergent_observations": {},
                            "last_result": None,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            ),
        }

    monkeypatch.setattr(adapter, "_load_yaml_service", load_yaml)
    monkeypatch.setattr(adapter, "_current_files", current_files)
    monkeypatch.setattr(adapter, "list_snapshots", snapshots)
    monkeypatch.setattr(adapter, "_call_service", call_service)

    first = adapter.manifest()
    first.nodes.clear()
    second = adapter.manifest()

    assert len(second.nodes) == 1
    assert calls == {"yaml": 2, "files": 1, "snapshots": 1, "pending": 1, "session": 1}

    adapter._invalidate_manifest()
    adapter.manifest()
    assert calls == {"yaml": 4, "files": 2, "snapshots": 2, "pending": 2, "session": 2}


def test_configuration_transport_rejects_noncanonical_or_extended_session_status():
    status = {
        "schema": "iii.configuration-session-status/v1",
        "session_id": None,
        "baseline_id": None,
        "target_id": "sim",
        "runtime_profile": "sim",
        "release_id": "a" * 64,
        "workspace_id": "workspace-test",
        "manifest_id": "b" * 64,
        "revision": 0,
        "wal_sequence": 0,
        "wal_checksum": None,
        "created_at": None,
        "updated_at": None,
        "active_values": {},
        "persisted_values": {},
        "pending_boot_values": {},
        "divergent": False,
        "divergent_observations": {},
        "last_result": None,
    }
    pretty = json.dumps(status, indent=2)
    with pytest.raises(RuntimeError, match="canonical JSON object"):
        _decode_canonical_object(pretty, label="configuration tuning session status")

    status["untrusted_authority"] = True
    with pytest.raises(RuntimeError, match="fields are invalid"):
        _validate_session_status(status)


def test_configuration_transport_rejects_malformed_transaction_result():
    result = {
        "schema": "iii.configuration-transaction-result/v1",
        "ok": True,
        "status": "committed",
        "session_id": "a" * 64,
        "transaction_id": "b" * 64,
        "request_id": "request-batch",
        "revision": 1,
        "reason": None,
        "observed_values": {},
        "results": [],
        "persistence_reference": "snapshots/runtime.yaml",
        "idempotent_replay": False,
    }
    _validate_transaction_result(result)

    result["status"] = "accepted-by-display-text"
    with pytest.raises(RuntimeError, match="values are invalid"):
        _validate_transaction_result(result)


def test_ros_configuration_adapter_returns_verified_snapshot_download(monkeypatch):
    adapter = RosConfigurationServerAdapter(node=object())
    content = "/**:\n  ros__parameters:\n    gain: 2.0\n"
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    monkeypatch.setattr(
        adapter,
        "_call_service",
        lambda service_type, service_name: {
            "request": SimpleNamespace(file=""),
            "call": lambda request: SimpleNamespace(
                success=True,
                message="",
                parameter_yaml=content,
                content_sha256=checksum,
            ),
        },
    )

    downloaded = adapter.download_snapshot(
        SnapshotDownloadRequest(snapshot_id="snapshots/tuned.yaml")
    )

    assert downloaded == {
        "snapshot_id": "snapshots/tuned.yaml",
        "content_type": "application/x-yaml",
        "content": content,
        "content_sha256": checksum,
        "download_supported": True,
    }


def test_ros_configuration_adapter_uses_one_revision_bound_batch_service(monkeypatch):
    adapter = RosConfigurationServerAdapter(node=object())
    manifest = ConfigurationManifest(
        status=ConfigurationStatus(
            configuration_server_available=True,
            tuning_revision=4,
        ),
        nodes=[
            ParameterNode(
                node_id="controller",
                label="Controller",
                groups=[
                    ParameterGroup(
                        group_id="control",
                        label="Control",
                        node_id="controller",
                        parameters=[
                            ParameterDefinition(
                                node_id="controller",
                                group_id="control",
                                name="/control/gain",
                                value_type=ParameterValueType.FLOAT,
                                current_value=1.0,
                            )
                        ],
                    )
                ],
            )
        ],
    )
    captured = []

    def call(request):
        captured.append(json.loads(request.request_json))
        result = {
            "schema": "iii.configuration-transaction-result/v1",
            "ok": True,
            "status": "committed",
            "session_id": "a" * 64,
            "transaction_id": "b" * 64,
            "request_id": "request-batch",
            "revision": 5,
            "reason": None,
            "observed_values": {"/control/gain": 2.0},
            "results": [
                {
                    "node_id": "controller",
                    "name": "/control/gain",
                    "success": True,
                    "message": "applied, read back, and persisted",
                    "applied_value": 2.0,
                    "persisted_value": 2.0,
                    "restart_required": "none",
                }
            ],
            "persistence_reference": "snapshots/runtime.yaml",
            "idempotent_replay": False,
        }
        return SimpleNamespace(
            success=True,
            message="",
            result_json=json.dumps(result, sort_keys=True, separators=(",", ":")),
        )

    monkeypatch.setattr(adapter, "manifest", lambda: manifest.model_copy(deep=True))
    monkeypatch.setattr(
        adapter,
        "_call_service",
        lambda service_type, service_name: (
            {
                "request": SimpleNamespace(request_json=""),
                "call": call,
            }
            if (service_type, service_name)
            == ("ApplyConfigurationTransaction", "apply_configuration_transaction")
            else pytest.fail("per-key configuration service was used")
        ),
    )

    response = adapter.apply(
        ConfigurationApplyRequest(
            edits=[
                ParameterEdit(node_id="controller", name="/control/gain", value=2.0)
            ],
            request_id="request-batch",
            expected_revision=4,
            operator_id="operator-batch",
        )
    )

    assert response.ok is True and response.revision == 5
    assert captured == [
        {
            "schema": "iii.configuration-transaction-request/v1",
            "request_id": "request-batch",
            "expected_revision": 4,
            "operator_id": "operator-batch",
            "edits": [
                {
                    "node_id": "controller",
                    "name": "/control/gain",
                    "value": 2.0,
                }
            ],
        }
    ]


def test_manifest_permissions_are_evaluated_once_per_parameter_class():
    adapter = _FakeConfigurationServer()

    class _Gate:
        def __init__(self):
            self.calls = []

        def mutating_permission(self, *, constant=False):
            self.calls.append(constant)
            return ConfigurationPermission(allowed=True, reasons=[])

    gate = _Gate()
    controller = ConfigurationRuntimeController(adapter=adapter, permission_gate=gate)

    manifest = controller.manifest()

    assert manifest.nodes
    assert gate.calls == [False, True]


def test_configuration_manifest_and_status_expose_restart_metadata_and_badges():
    adapter = _FakeConfigurationServer()
    client = _client(adapter)
    headers = _headers(client)

    manifest = client.get("/configuration/manifest", headers=headers).json()
    status = client.get("/configuration/status", headers=headers).json()

    parameters = {
        parameter["name"]: parameter
        for node in manifest["nodes"]
        for group in node["groups"]
        for parameter in group["parameters"]
    }
    assert parameters["/control/static_frame"]["restart_required"] == "node"
    assert parameters["/control/immutable_name"]["restart_required"] == "runtime"
    assert parameters["/control/immutable_name"]["readonly"] is False
    assert parameters["/control/immutable_name"]["constant"] is True
    assert parameters["/control/immutable_name"]["apply_allowed"] is True
    assert manifest["status"]["badges"] == []
    assert status["active_snapshot_id"] == "tracked/default.yaml"
    assert status["latest"]["permissions"]["writes_allowed"] is True


def test_divergent_configuration_blocks_parameter_and_snapshot_mutations():
    adapter = _FakeConfigurationServer()
    original_manifest = adapter.manifest

    def divergent_manifest():
        manifest = original_manifest()
        manifest.status.configuration_divergent = True
        manifest.status.divergent_observations = {"/control/gains/p": 9.0}
        return manifest

    adapter.manifest = divergent_manifest
    controller = ConfigurationRuntimeController(
        adapter=adapter,
        permission_gate=SimpleNamespace(
            mutating_permission=lambda **_kwargs: ConfigurationPermission(
                allowed=True, reasons=[]
            )
        ),
    )

    manifest = controller.manifest()
    parameters = [
        parameter
        for node in manifest.nodes
        for group in node.groups
        for parameter in group.parameters
    ]
    assert parameters and all(not parameter.apply_allowed for parameter in parameters)
    assert all(
        any(
            "configuration is divergent" in reason
            for reason in parameter.apply_rejection_reasons
        )
        for parameter in parameters
    )

    saved = controller.save_snapshot(SnapshotSaveRequest(label="must-not-save"))
    assert saved.ok is False
    assert "configuration is divergent" in saved.message
    assert adapter.saved == []


def test_configuration_server_payload_is_normalized_without_local_parameter_files():
    manifest = _manifest_from_configuration_server_payload(
        raw_manifest={
            "control": {
                "gains": {
                    "p": {
                        "type": "float",
                        "value": 1.5,
                        "min": 0.0,
                        "max": 10.0,
                        "description": "P gain",
                    }
                },
                "static_frame": {"type": "string", "value": "map", "static": True},
                "immutable_name": {
                    "type": "string",
                    "value": "alpha",
                    "constant": True,
                },
                "dependent_limit": {
                    "type": "float",
                    "value": 2.0,
                    "min": "/control/gains/p + 0.5",
                },
            }
        },
        declared_parameters={
            "/control/gains/p": ["/controller"],
            "/control/static_frame": ["/controller"],
            "/control/immutable_name": ["/controller"],
            "/control/dependent_limit": ["/controller"],
        },
        current_snapshot_id="snapshots/runtime_parameters_test.yaml",
        default_snapshot_id="tracked/default.yaml",
        available_snapshots=[],
        pending_boot_values={"/control/immutable_name": "beta"},
        tuning_session={
            "session_id": "a" * 64,
            "baseline_id": "b" * 64,
            "revision": 7,
            "divergent": True,
            "divergent_observations": {"/control/gains/p": 1.7},
        },
    )

    parameters = {
        parameter.name: parameter
        for node in manifest.nodes
        for group in node.groups
        for parameter in group.parameters
    }
    assert manifest.nodes[0].node_id == "controller"
    assert parameters["/control/gains/p"].constraints.minimum == 0.0
    assert parameters["/control/dependent_limit"].constraints.minimum is None
    assert (
        parameters["/control/dependent_limit"].constraints.minimum_expression
        == "/control/gains/p + 0.5"
    )
    assert parameters["/control/static_frame"].restart_required == "node"
    assert parameters["/control/immutable_name"].restart_required == "runtime"
    assert parameters["/control/immutable_name"].constant is True
    assert manifest.status.unsaved is True
    assert manifest.status.badges == ["Restart required", "Unsaved"]
    assert manifest.status.tuning_revision == 7
    assert manifest.status.pending_boot_values == {"/control/immutable_name": "beta"}
    assert manifest.status.configuration_divergent is True


def test_configuration_apply_returns_per_parameter_results_and_unsaved_state():
    adapter = _FakeConfigurationServer()
    client = _client(adapter)
    headers = _headers(client)

    response = client.post(
        "/configuration/apply",
        headers=headers,
        json={
            "edits": [
                {"node_id": "controller", "name": "/control/gains/p", "value": 1.7},
                {
                    "node_id": "controller",
                    "name": "/control/immutable_name",
                    "value": "beta",
                },
            ]
        },
    )
    status = client.get("/configuration/status", headers=headers).json()

    payload = response.json()
    assert payload["ok"] is True
    assert payload["results"][0]["success"] is True
    assert payload["results"][1]["success"] is True
    assert payload["results"][1]["restart_required"] == "runtime"
    assert status["unsaved"] is True
    assert status["latest"]["manifest"]["status"]["badges"] == [
        "Restart required",
        "Unsaved",
    ]


def test_committed_revision_is_published_once_with_authoritative_full_state():
    adapter = _FakeConfigurationServer()
    client = _client(adapter)
    headers = _headers(client)

    response = client.post(
        "/configuration/apply",
        headers=headers,
        json={
            "request_id": "revision-event-1",
            "expected_revision": 0,
            "operator_id": "operator-test",
            "edits": [
                {"node_id": "controller", "name": "/control/gains/p", "value": 1.7}
            ],
        },
    )
    events = client.get("/events/recent", headers=headers).json()
    revisions = [
        event for event in events if event["category"] == "configuration_revision"
    ]
    state_value = client.get("/configuration/status", headers=headers).json()

    assert response.status_code == 200
    assert len(revisions) == 1
    assert revisions[0]["details"] == {
        "schema": "iii.configuration-revision-event/v1",
        "session_id": "a" * 64,
        "transaction_id": "1" * 64,
        "revision": 1,
        "journal_sequence": 2,
        "journal_checksum": "e" * 64,
    }
    assert state_value["latest"]["manifest"]["status"]["tuning_revision"] == 1
    assert state_value["latest"]["manifest"]["status"]["mirror_state"] == "degraded"


def test_idempotent_apply_replay_does_not_publish_a_duplicate_revision():
    adapter = _FakeConfigurationServer()
    published = []
    controller = ConfigurationRuntimeController(
        adapter=adapter,
        permission_gate=SimpleNamespace(
            mutating_permission=lambda **_kwargs: ConfigurationPermission(
                allowed=True, reasons=[]
            )
        ),
        revision_sink=published.append,
    )
    request = ConfigurationApplyRequest(
        edits=[ParameterEdit(node_id="controller", name="/control/gains/p", value=1.7)],
        request_id="revision-replay",
        expected_revision=0,
    )
    committed = controller.apply(request)
    replay = committed.model_copy(update={"idempotent_replay": True})
    adapter.apply = lambda _request: replay

    controller.apply(request)

    assert len(published) == 1


def test_successful_snapshot_mutation_requests_an_authoritative_state_patch():
    adapter = _FakeConfigurationServer()
    patches = []
    controller = ConfigurationRuntimeController(
        adapter=adapter,
        permission_gate=SimpleNamespace(
            mutating_permission=lambda **_kwargs: ConfigurationPermission(
                allowed=True, reasons=[]
            )
        ),
        state_sink=lambda: patches.append("configuration"),
    )

    response = controller.save_snapshot(SnapshotSaveRequest(label="operator"))

    assert response.ok is True
    assert patches == ["configuration"]


def test_mirror_acknowledgement_must_match_exact_authoritative_head():
    adapter = _FakeConfigurationServer()
    adapter.revision = 2
    controller = ConfigurationRuntimeController(
        adapter=adapter,
        permission_gate=SimpleNamespace(
            mutating_permission=lambda **_kwargs: ConfigurationPermission(
                allowed=True, reasons=[]
            )
        ),
    )
    assert controller.manifest().status.mirror_state == "degraded"
    acknowledgement = {
        "schema": "iii.configuration-mirror-ack/v1",
        "session_id": adapter.session_id,
        "revision": 2,
        "sequence": 4,
        "checksum": "e" * 64,
        "mirror_id": "f" * 64,
    }

    acknowledged = controller.acknowledge_mirror(acknowledgement)

    assert acknowledged.mirror_state == "current"
    assert acknowledged.mirror_ack_revision == 2
    with pytest.raises(RuntimeError, match="authoritative journal head"):
        controller.acknowledge_mirror({**acknowledgement, "sequence": 3})


def test_gc_mirror_state_and_ack_are_cli_credential_scoped():
    adapter = _FakeConfigurationServer()
    adapter.revision = 1
    client = _client(adapter)
    cli_headers = {"X-III-CLI-Token": "cli-secret"}

    assert client.get("/cli/configuration/state").status_code == 401
    state_response = client.get("/cli/configuration/state", headers=cli_headers)
    acknowledgement = {
        "schema": "iii.configuration-mirror-ack/v1",
        "session_id": adapter.session_id,
        "revision": 1,
        "sequence": 2,
        "checksum": "e" * 64,
        "mirror_id": "f" * 64,
    }
    assert (
        client.post("/cli/configuration/mirror/ack", json=acknowledgement).status_code
        == 401
    )
    acknowledged = client.post(
        "/cli/configuration/mirror/ack",
        headers=cli_headers,
        json=acknowledgement,
    )

    assert state_response.status_code == 200
    assert state_response.json()["manifest"]["status"]["mirror_state"] == "degraded"
    assert acknowledged.status_code == 200
    assert acknowledged.json()["status"]["mirror_state"] == "current"


def test_cli_capture_source_rejects_target_profile_mismatch():
    client = _client(_FakeConfigurationServer())
    cli_headers = {"X-III-CLI-Token": "cli-secret"}

    mismatch = client.get(
        "/cli/configuration/capture-source/snapshots%2Ftuned.yaml",
        headers=cli_headers,
        params={"expected_profile": "real"},
    )
    accepted = client.get(
        "/cli/configuration/capture-source/snapshots%2Ftuned.yaml",
        headers=cli_headers,
        params={"expected_profile": "sim"},
    )

    assert mismatch.status_code == 409
    assert "profile mismatch" in mismatch.json()["detail"]
    assert accepted.status_code == 200
    assert accepted.json()["snapshot_id"] == "snapshots/tuned.yaml"


@pytest.mark.parametrize(
    ("snapshot_id", "is_active"),
    [("tracked/default.yaml", True), ("snapshots/inactive.yaml", False)],
)
def test_capture_source_reads_arbitrary_set_without_loading_it(
    monkeypatch, snapshot_id, is_active
):
    adapter = RosConfigurationServerAdapter(node=object())
    sealed = []
    contract_seal_capture = configuration_module.seal_capture
    monkeypatch.setattr(
        configuration_module,
        "seal_capture",
        lambda value: sealed.append(value) or contract_seal_capture(value),
    )
    status = ConfigurationStatus(
        configuration_server_available=True,
        loaded_snapshot_id="tracked/default.yaml",
        default_snapshot_id="tracked/default.yaml",
        tuning_session_id="a" * 64,
        tuning_baseline_id="b" * 64,
        tuning_target_id="sim",
        tuning_runtime_profile="sim",
        tuning_release_id="c" * 64,
        tuning_workspace_id="workspace-test",
        tuning_manifest_id="d" * 64,
        tuning_revision=3,
        tuning_journal_sequence=6,
        tuning_journal_checksum="e" * 64,
        tuning_created_at="2026-08-27T12:00:00Z",
        tuning_updated_at="2026-08-27T12:00:03Z",
        pending_boot_values={"/control/frame": "odom"},
    )
    monkeypatch.setattr(
        adapter,
        "download_snapshot",
        lambda request: {
            "snapshot_id": request.snapshot_id,
            "content": (
                "/**:\n  ros__parameters:\n    /control/gain: 2.0\n"
                "sensor:\n  example:\n    ros__parameters:\n      frame_id: sensor\n"
            ),
            "content_sha256": "f" * 64,
        },
    )
    monkeypatch.setattr(adapter, "_ensure_tuning_session", lambda: {})
    monkeypatch.setattr(
        adapter,
        "manifest",
        lambda: ConfigurationManifest(status=status),
    )
    head_entry = {
        "schema": "iii.configuration-tuning-wal-entry/v1",
        "sequence": 6,
        "previous_checksum": "f" * 64,
        "checksum": "",
        "kind": "committed",
        "timestamp": "2026-08-27T12:00:03Z",
        "session_id": "a" * 64,
        "transaction_id": "1" * 64,
        "request_id": "request-6",
        "revision": 3,
        "body": {"operator_id": "operator-test"},
    }
    head_entry["checksum"] = configuration_module.hashlib.sha256(
        configuration_module._canonical_json(
            {key: item for key, item in head_entry.items() if key != "checksum"}
        ).encode("utf-8")
    ).hexdigest()
    status.tuning_journal_checksum = head_entry["checksum"]
    monkeypatch.setattr(
        adapter,
        "journal",
        lambda **_kwargs: {
            "baseline_values": {"/control/gain": 1.0},
            "entries": [head_entry],
        },
    )

    source = adapter.capture_source(
        configuration_module.SnapshotDownloadRequest(snapshot_id=snapshot_id)
    )

    assert source["snapshot_id"] == snapshot_id
    assert source["source_is_active"] is is_active
    assert source["values"] == {"/control/gain": 2.0}
    assert source["parameter_document"]["sensor"]["example"]["ros__parameters"] == {
        "frame_id": "sensor"
    }
    assert source["baseline_values"] == {"/control/gain": 1.0}
    assert sealed == [source]
    assert status.loaded_snapshot_id == "tracked/default.yaml"


def test_configuration_snapshot_operations_use_server_adapter():
    adapter = _FakeConfigurationServer()
    client = _client(adapter)
    headers = _headers(client)

    saved = client.post(
        "/configuration/snapshots/save",
        headers=headers,
        json={"label": "operator_tuned"},
    )
    loaded = client.post(
        "/configuration/snapshots/load",
        headers=headers,
        json={"snapshot_id": "snapshots/tuned.yaml"},
    )
    defaulted = client.post(
        "/configuration/snapshots/default",
        headers=headers,
        json={"snapshot_id": "snapshots/tuned.yaml"},
    )
    listing = client.get("/configuration/snapshots", headers=headers)
    download = client.get(
        "/configuration/snapshots/snapshots/tuned.yaml/download", headers=headers
    )

    assert saved.json()["ok"] is True
    assert adapter.saved[0].label == "operator_tuned"
    assert loaded.json()["ok"] is True
    assert adapter.loaded == ["snapshots/tuned.yaml"]
    assert defaulted.json()["ok"] is True
    assert adapter.defaults == ["snapshots/tuned.yaml"]
    assert any(
        row["snapshot_id"] == "snapshots/operator_tuned.yaml"
        for row in listing.json()["snapshots"]
    )
    assert download.json()["download_supported"] is True


def test_configuration_commands_cover_apply_list_and_download():
    adapter = _FakeConfigurationServer()
    client = _client(adapter)
    headers = _headers(client)

    applied = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "config-apply",
            "command_id": CommandId.CONFIGURATION_APPLY.value,
            "parameters": {
                "edits": [
                    {"node_id": "controller", "name": "/control/gains/i", "value": 0.4}
                ]
            },
        },
    )
    listed = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "config-list",
            "command_id": CommandId.CONFIGURATION_LIST_SNAPSHOTS.value,
        },
    )
    downloaded = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "config-download",
            "command_id": CommandId.CONFIGURATION_DOWNLOAD_SNAPSHOT.value,
            "parameters": {"snapshot_id": "tracked/default.yaml"},
        },
    )

    assert applied.json()["accepted"] is True
    assert adapter.applied[0].name == "/control/gains/i"
    assert listed.json()["accepted"] is True
    assert listed.json()["result"]["snapshots"]
    assert downloaded.json()["accepted"] is True
    assert (
        downloaded.json()["result"]["snapshot"]["content_type"] == "application/x-yaml"
    )


def test_configuration_writes_reject_in_mission_and_active_custom_operation():
    mission_adapter = _FakeConfigurationServer()
    mission_client = _client(mission_adapter, mission_active=True)
    mission_headers = _headers(mission_client)
    operation_adapter = _FakeConfigurationServer()
    operation_client = _client(operation_adapter, operation_active=True)
    operation_headers = _headers(operation_client)

    mission_apply = mission_client.post(
        "/commands/actions/start",
        headers=mission_headers,
        json={
            "request_id": "config-apply-mission",
            "command_id": CommandId.CONFIGURATION_APPLY.value,
            "parameters": {
                "edits": [
                    {"node_id": "controller", "name": "/control/gains/p", "value": 1.8}
                ]
            },
        },
    )
    mission_list = mission_client.post(
        "/commands/actions/start",
        headers=mission_headers,
        json={
            "request_id": "config-list-mission",
            "command_id": CommandId.CONFIGURATION_LIST_SNAPSHOTS.value,
        },
    )
    operation_save = operation_client.post(
        "/configuration/snapshots/save",
        headers=operation_headers,
        json={"label": "blocked"},
    )

    assert mission_apply.json()["accepted"] is False
    assert "Mission mode" in mission_apply.json()["rejection"]["message"]
    assert mission_adapter.applied == []
    assert mission_list.json()["accepted"] is True
    assert operation_save.json()["ok"] is False
    assert "custom operation action is active" in operation_save.json()["message"]


def _manifest_parameters(client, headers):
    manifest = client.get("/configuration/manifest", headers=headers).json()
    return {
        parameter["name"]: parameter
        for node in manifest["nodes"]
        for group in node["groups"]
        for parameter in group["parameters"]
    }


def test_configuration_permission_matrix_allows_live_only_in_hold_or_landed():
    hold_client = _client(
        _FakeConfigurationServer(),
        vehicle_provider=_vehicle_provider(armed=True, in_air=True, mode="hold"),
    )
    hold_parameters = _manifest_parameters(hold_client, _headers(hold_client))
    position_client = _client(
        _FakeConfigurationServer(),
        vehicle_provider=_vehicle_provider(armed=True, in_air=True, mode="position"),
    )
    position_parameters = _manifest_parameters(
        position_client, _headers(position_client)
    )

    assert hold_parameters["/control/gains/p"]["apply_allowed"] is True
    assert hold_parameters["/control/immutable_name"]["apply_allowed"] is False
    assert hold_parameters["/control/immutable_name"]["apply_rejection_reasons"] == [
        "constant parameters require the aircraft to be disarmed and landed"
    ]
    assert position_parameters["/control/gains/p"]["apply_allowed"] is False
    assert position_parameters["/control/gains/p"]["apply_rejection_reasons"] == [
        "live parameters require PX4 Hold or a disarmed and landed aircraft"
    ]


@pytest.mark.parametrize(
    ("vehicle_provider", "expected_reason"),
    [
        (_vehicle_provider(fresh=False), "vehicle state is stale"),
        (_vehicle_provider(available=False), "vehicle state is unavailable"),
        (
            _vehicle_provider(armed=None, in_air=None),
            "vehicle armed/landed state is unknown",
        ),
    ],
)
def test_configuration_unknown_or_stale_vehicle_state_fails_closed(
    vehicle_provider, expected_reason
):
    adapter = _FakeConfigurationServer()
    client = _client(adapter, vehicle_provider=vehicle_provider)
    headers = _headers(client)

    response = client.post(
        "/configuration/apply",
        headers=headers,
        json={
            "edits": [
                {"node_id": "controller", "name": "/control/gains/p", "value": 1.8}
            ]
        },
    ).json()

    assert response["ok"] is False
    assert response["results"][0]["message"] == expected_reason
    assert adapter.applied == []


def test_constant_edit_is_persisted_pending_without_changing_active_value():
    adapter = _FakeConfigurationServer()
    client = _client(adapter)
    headers = _headers(client)

    response = client.post(
        "/configuration/apply",
        headers=headers,
        json={
            "edits": [
                {
                    "node_id": "controller",
                    "name": "/control/immutable_name",
                    "value": "beta",
                }
            ]
        },
    ).json()
    manifest = client.get("/configuration/manifest", headers=headers).json()
    parameter = _manifest_parameters(client, headers)["/control/immutable_name"]

    assert response["ok"] is True
    assert parameter["active_value"] == "alpha"
    assert parameter["persisted_value"] == "beta"
    assert manifest["status"]["pending_restart"] is True
    assert manifest["status"]["pending_constant_names"] == ["/control/immutable_name"]
