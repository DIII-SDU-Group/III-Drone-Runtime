from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import (
    CommandId,
    ConfigurationApplyResponse,
    ConfigurationManifest,
    ConfigurationStatus,
    ParameterDefinition,
    ParameterGroup,
    ParameterNode,
    ParameterValueType,
    ParameterApplyResult,
    SnapshotOperationResponse,
    SnapshotSummary,
    VehicleDomainState,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache


class _FakeGoal:
    accepted = True

    def cancel(self):
        return True


class _FakeOperationTransport:
    def start(self, **kwargs):
        del kwargs
        return _FakeGoal()


class _FakeGripperService:
    def __init__(self):
        self.commands = []

    def command(self, command):
        self.commands.append(command)
        return {"success": True, "command": command}


class _FakePLMapperService:
    def __init__(self):
        self.commands = []

    def command(self, command, *, reset=False):
        self.commands.append((command, reset))
        return {"success": True, "command": command, "reset": reset}


class _FakeOverviewService:
    def update(self, *, timeout_s):
        return {"success": True, "timeout_s": timeout_s}


class _FakeRosbagAdapter:
    def status(self):
        return {"recording": False, "owner": "unknown"}

    def start(self, request):
        return {"success": True, **request}

    def stop(self, request):
        return {"success": True, **request}

    def list_recordings(self):
        return [{"recording_id": "bag-1"}]

    def download(self, recording_id):
        return {"recording_id": recording_id, "download_supported": True}


class _FakeConfigurationServer:
    def __init__(self):
        self.applied = []

    def manifest(self):
        return ConfigurationManifest(
            nodes=[
                ParameterNode(
                    node_id="controller",
                    label="Controller",
                    groups=[
                        ParameterGroup(
                            group_id="gains",
                            label="Gains",
                            node_id="controller",
                            parameters=[
                                ParameterDefinition(
                                    node_id="controller",
                                    group_id="gains",
                                    name="/control/gains/p",
                                    value_type=ParameterValueType.FLOAT,
                                    current_value=1.0,
                                )
                            ],
                        )
                    ],
                )
            ],
            status=ConfigurationStatus(loaded_snapshot_id="tracked/default.yaml"),
        )

    def apply(self, request):
        self.applied.extend(request.edits)
        return ConfigurationApplyResponse(
            ok=True,
            results=[
                ParameterApplyResult(
                    node_id=edit.node_id,
                    name=edit.name,
                    success=True,
                    applied_value=edit.value,
                )
                for edit in request.edits
            ],
            status=ConfigurationStatus(loaded_snapshot_id="tracked/default.yaml"),
        )

    def save_snapshot(self, request):
        return SnapshotOperationResponse(ok=True, snapshot=SnapshotSummary(snapshot_id=request.label, label=request.label))

    def load_snapshot(self, request):
        return SnapshotOperationResponse(ok=True, snapshot=SnapshotSummary(snapshot_id=request.snapshot_id, label=request.snapshot_id))

    def list_snapshots(self):
        return [SnapshotSummary(snapshot_id="tracked/default.yaml", label="tracked/default.yaml")]

    def download_snapshot(self, request):
        return {"snapshot_id": request.snapshot_id, "download_supported": True}

    def set_default_snapshot(self, request):
        return SnapshotOperationResponse(ok=True, snapshot=SnapshotSummary(snapshot_id=request.snapshot_id, label=request.snapshot_id))


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
            control_owner="custom_operation",
            cancel_available=active,
            degraded=False,
            degraded_reasons=[],
        )
    )
    return cache


def _client(*, mission_active=False, operation_active=False, configuration=None, gripper=None, pl_mapper=None):
    nav_state = "mission" if mission_active else "custom_operation" if operation_active else "hold"
    vehicle_state = VehicleDomainState(
        freshness=Freshness.FRESH,
        source_availability=SourceAvailability.AVAILABLE,
        armed=False,
        in_air=False,
        nav_state=nav_state,
    )
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
            custom_operation_transport=_FakeOperationTransport(),
            gripper_service=gripper or _FakeGripperService(),
            pl_mapper_service=pl_mapper or _FakePLMapperService(),
            powerline_overview_service=_FakeOverviewService(),
            rosbag_adapter=_FakeRosbagAdapter(),
            configuration_adapter=configuration or _FakeConfigurationServer(),
            px4_state_provider=SimpleNamespace(
                state=lambda: vehicle_state,
                dangerous_command_rejection_reason=lambda: None,
            ),
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def _start(client, headers, command_id, parameters=None):
    return client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": command_id, "command_id": command_id, "parameters": parameters or {}},
    ).json()


def test_handler_permission_metadata_is_visible_for_all_operator_classes():
    client = _client()
    headers = _headers(client)

    metadata = client.get("/commands/handlers", headers=headers).json()["actions"]

    assert metadata[CommandId.CUSTOM_OPERATION_VALIDATE.value]["permission"] == "read_only"
    assert metadata[CommandId.ROSBAG_LIST.value]["permission"] == "read_only"
    assert metadata[CommandId.CONFIGURATION_LIST_SNAPSHOTS.value]["permission"] == "read_only"
    assert metadata[CommandId.PAYLOAD_GRIPPER_OPEN.value]["permission"] == "mutating"
    assert metadata[CommandId.PERCEPTION_PL_MAPPER_START.value]["permission"] == "mutating"
    assert metadata[CommandId.CONFIGURATION_APPLY.value]["permission"] == "mutating"
    assert metadata[CommandId.PX4_HOLD.value]["permission"] == "flight_critical"
    assert metadata[CommandId.RUNTIME_STOP.value]["permission"] == "runtime_mutation"


def test_mission_mode_allows_declared_read_only_calls_and_rejects_mutations():
    configuration = _FakeConfigurationServer()
    gripper = _FakeGripperService()
    pl_mapper = _FakePLMapperService()
    client = _client(mission_active=True, configuration=configuration, gripper=gripper, pl_mapper=pl_mapper)
    headers = _headers(client)

    validate = _start(
        client,
        headers,
        CommandId.CUSTOM_OPERATION_VALIDATE.value,
        {"operation": "hover", "arguments": {"duration_s": 2.0}},
    )
    rosbag_list = _start(client, headers, CommandId.ROSBAG_LIST.value)
    config_list = _start(client, headers, CommandId.CONFIGURATION_LIST_SNAPSHOTS.value)
    operation_start = _start(
        client,
        headers,
        CommandId.CUSTOM_OPERATION_HOVER_START.value,
        {"hold_confirmed": True, "duration_s": 2.0},
    )
    gripper_open = _start(client, headers, CommandId.PAYLOAD_GRIPPER_OPEN.value)
    pl_start = _start(client, headers, CommandId.PERCEPTION_PL_MAPPER_START.value)
    config_apply = _start(
        client,
        headers,
        CommandId.CONFIGURATION_APPLY.value,
        {"edits": [{"node_id": "controller", "name": "/control/gains/p", "value": 1.8}]},
    )

    assert validate["accepted"] is True
    assert validate["result"]["validation"]["ok"] is False
    assert "Mission mode" in "; ".join(validate["result"]["validation"]["rejection_reasons"])
    assert rosbag_list["accepted"] is True
    assert config_list["accepted"] is True
    assert operation_start["accepted"] is False
    assert "Mission mode" in operation_start["rejection"]["message"]
    assert gripper_open["accepted"] is False
    assert "Mission mode" in gripper_open["rejection"]["message"]
    assert pl_start["accepted"] is False
    assert "Mission mode" in pl_start["rejection"]["message"]
    assert config_apply["accepted"] is False
    assert "Mission mode" in config_apply["rejection"]["message"]
    assert gripper.commands == []
    assert pl_mapper.commands == []
    assert configuration.applied == []


def test_custom_operation_idle_allows_subsystem_mutations_and_active_operation_rejects_them():
    idle_configuration = _FakeConfigurationServer()
    idle_gripper = _FakeGripperService()
    idle_pl_mapper = _FakePLMapperService()
    idle_client = _client(configuration=idle_configuration, gripper=idle_gripper, pl_mapper=idle_pl_mapper)
    idle_headers = _headers(idle_client)
    active_configuration = _FakeConfigurationServer()
    active_gripper = _FakeGripperService()
    active_pl_mapper = _FakePLMapperService()
    active_client = _client(
        operation_active=True,
        configuration=active_configuration,
        gripper=active_gripper,
        pl_mapper=active_pl_mapper,
    )
    active_headers = _headers(active_client)

    idle_gripper_response = _start(idle_client, idle_headers, CommandId.PAYLOAD_GRIPPER_OPEN.value)
    idle_pl_response = _start(idle_client, idle_headers, CommandId.PERCEPTION_PL_MAPPER_START.value)
    idle_config_response = _start(
        idle_client,
        idle_headers,
        CommandId.CONFIGURATION_APPLY.value,
        {"edits": [{"node_id": "controller", "name": "/control/gains/p", "value": 1.8}]},
    )
    active_gripper_response = _start(active_client, active_headers, CommandId.PAYLOAD_GRIPPER_OPEN.value)
    active_pl_response = _start(active_client, active_headers, CommandId.PERCEPTION_PL_MAPPER_START.value)
    active_config_response = _start(
        active_client,
        active_headers,
        CommandId.CONFIGURATION_APPLY.value,
        {"edits": [{"node_id": "controller", "name": "/control/gains/p", "value": 1.8}]},
    )

    assert idle_gripper_response["accepted"] is True
    assert idle_pl_response["accepted"] is True
    assert idle_config_response["accepted"] is True
    assert idle_gripper.commands == ["open"]
    assert idle_pl_mapper.commands == [("start", False)]
    assert idle_configuration.applied
    assert active_gripper_response["accepted"] is False
    assert "custom operation action is active" in active_gripper_response["rejection"]["message"]
    assert active_pl_response["accepted"] is False
    assert "custom operation action is active" in active_pl_response["rejection"]["message"]
    assert active_config_response["accepted"] is False
    assert "custom operation action is active" in active_config_response["rejection"]["message"]
    assert active_gripper.commands == []
    assert active_pl_mapper.commands == []
    assert active_configuration.applied == []
