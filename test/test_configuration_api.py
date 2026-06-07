from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import (
    CommandId,
    ConfigurationApplyRequest,
    ConfigurationApplyResponse,
    ConfigurationManifest,
    ConfigurationStatus,
    ParameterApplyResult,
    ParameterDefinition,
    ParameterGroup,
    ParameterNode,
    ParameterValueType,
    RestartRequired,
    SnapshotOperationResponse,
    SnapshotSummary,
)
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.configuration import _manifest_from_configuration_server_payload
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
                                    loaded_value=self.snapshots[self.current_snapshot_id]["/control/gains/p"],
                                    default_value=self.snapshots[self.default_snapshot_id]["/control/gains/p"],
                                ),
                                ParameterDefinition(
                                    node_id="controller",
                                    group_id="control/gains",
                                    name="/control/gains/i",
                                    value_type=ParameterValueType.FLOAT,
                                    current_value=self.values["/control/gains/i"],
                                    loaded_value=self.snapshots[self.current_snapshot_id]["/control/gains/i"],
                                    default_value=self.snapshots[self.default_snapshot_id]["/control/gains/i"],
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
                                    current_value=self.values["/control/immutable_name"],
                                    restart_required=RestartRequired.RUNTIME,
                                    readonly=True,
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
                results.append(
                    ParameterApplyResult(
                        node_id=edit.node_id,
                        name=edit.name,
                        success=False,
                        message="boot-only parameter",
                        restart_required=RestartRequired.RUNTIME,
                    )
                )
                continue
            self.values[edit.name] = edit.value
            results.append(
                ParameterApplyResult(
                    node_id=edit.node_id,
                    name=edit.name,
                    success=True,
                    applied_value=edit.value,
                    restart_required=RestartRequired.NODE if edit.name == "/control/static_frame" else RestartRequired.NONE,
                )
            )
        self.current_snapshot_id = "snapshots/runtime_parameters_test.yaml"
        self.snapshots[self.current_snapshot_id] = dict(self.values)
        return ConfigurationApplyResponse(ok=all(result.success for result in results), results=results, status=self._status())

    def save_snapshot(self, request):
        snapshot_id = request.overwrite_snapshot_id or f"snapshots/{request.label}.yaml"
        self.saved.append(request)
        self.snapshots[snapshot_id] = dict(self.values)
        self.current_snapshot_id = snapshot_id
        return SnapshotOperationResponse(
            ok=True,
            snapshot=SnapshotSummary(snapshot_id=snapshot_id, label=snapshot_id, is_loaded=True),
            status=self._status(),
        )

    def load_snapshot(self, request):
        self.loaded.append(request.snapshot_id)
        self.values = dict(self.snapshots[request.snapshot_id])
        self.current_snapshot_id = request.snapshot_id
        return SnapshotOperationResponse(ok=True, snapshot=SnapshotSummary(snapshot_id=request.snapshot_id, label=request.snapshot_id), status=self._status())

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

    def set_default_snapshot(self, request):
        self.defaults.append(request.snapshot_id)
        self.default_snapshot_id = request.snapshot_id
        return SnapshotOperationResponse(ok=True, snapshot=SnapshotSummary(snapshot_id=request.snapshot_id, label=request.snapshot_id), status=self._status())

    def _status(self):
        unsaved = self.current_snapshot_id.startswith("snapshots/runtime_parameters_")
        return ConfigurationStatus(
            unsaved=unsaved,
            non_default=self.current_snapshot_id != self.default_snapshot_id and not unsaved,
            loaded_snapshot_id=self.current_snapshot_id,
            default_snapshot_id=self.default_snapshot_id,
        )


def _mission_cache(*, active=False):
    cache = MissionStatusCache()
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/mission.yaml",
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


def _client(adapter, *, mission_active=False, operation_active=False):
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            mission_status=_mission_cache(active=mission_active),
            operation_status=_operation_cache(active=operation_active),
            configuration_adapter=adapter,
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


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
    assert parameters["/control/immutable_name"]["readonly"] is True
    assert manifest["status"]["badges"] == []
    assert status["active_snapshot_id"] == "tracked/default.yaml"
    assert status["latest"]["permissions"]["writes_allowed"] is True


def test_configuration_server_payload_is_normalized_without_local_parameter_files():
    manifest = _manifest_from_configuration_server_payload(
        raw_manifest={
            "control": {
                "gains": {
                    "p": {"type": "float", "value": 1.5, "min": 0.0, "max": 10.0, "description": "P gain"}
                },
                "static_frame": {"type": "string", "value": "map", "static": True},
                "immutable_name": {"type": "string", "value": "alpha", "constant": True},
            }
        },
        declared_parameters={
            "/control/gains/p": ["/controller"],
            "/control/static_frame": ["/controller"],
            "/control/immutable_name": ["/controller"],
        },
        current_snapshot_id="snapshots/runtime_parameters_test.yaml",
        default_snapshot_id="tracked/default.yaml",
        available_snapshots=[],
    )

    parameters = {
        parameter.name: parameter
        for node in manifest.nodes
        for group in node.groups
        for parameter in group.parameters
    }
    assert manifest.nodes[0].node_id == "controller"
    assert parameters["/control/gains/p"].constraints.minimum == 0.0
    assert parameters["/control/static_frame"].restart_required == "node"
    assert parameters["/control/immutable_name"].restart_required == "runtime"
    assert manifest.status.unsaved is True
    assert manifest.status.badges == ["Unsaved"]


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
                {"node_id": "controller", "name": "/control/immutable_name", "value": "beta"},
            ]
        },
    )
    status = client.get("/configuration/status", headers=headers).json()

    payload = response.json()
    assert payload["ok"] is False
    assert payload["results"][0]["success"] is True
    assert payload["results"][1]["success"] is False
    assert payload["results"][1]["restart_required"] == "runtime"
    assert status["unsaved"] is True
    assert status["latest"]["manifest"]["status"]["badges"] == ["Unsaved"]


def test_configuration_snapshot_operations_use_server_adapter():
    adapter = _FakeConfigurationServer()
    client = _client(adapter)
    headers = _headers(client)

    saved = client.post("/configuration/snapshots/save", headers=headers, json={"label": "operator_tuned"})
    loaded = client.post("/configuration/snapshots/load", headers=headers, json={"snapshot_id": "snapshots/tuned.yaml"})
    defaulted = client.post(
        "/configuration/snapshots/default",
        headers=headers,
        json={"snapshot_id": "snapshots/tuned.yaml"},
    )
    listing = client.get("/configuration/snapshots", headers=headers)
    download = client.get("/configuration/snapshots/snapshots/tuned.yaml/download", headers=headers)

    assert saved.json()["ok"] is True
    assert adapter.saved[0].label == "operator_tuned"
    assert loaded.json()["ok"] is True
    assert adapter.loaded == ["snapshots/tuned.yaml"]
    assert defaulted.json()["ok"] is True
    assert adapter.defaults == ["snapshots/tuned.yaml"]
    assert any(row["snapshot_id"] == "snapshots/operator_tuned.yaml" for row in listing.json()["snapshots"])
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
            "parameters": {"edits": [{"node_id": "controller", "name": "/control/gains/i", "value": 0.4}]},
        },
    )
    listed = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "config-list", "command_id": CommandId.CONFIGURATION_LIST_SNAPSHOTS.value},
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
    assert downloaded.json()["result"]["snapshot"]["content_type"] == "application/x-yaml"


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
            "parameters": {"edits": [{"node_id": "controller", "name": "/control/gains/p", "value": 1.8}]},
        },
    )
    mission_list = mission_client.post(
        "/commands/actions/start",
        headers=mission_headers,
        json={"request_id": "config-list-mission", "command_id": CommandId.CONFIGURATION_LIST_SNAPSHOTS.value},
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
