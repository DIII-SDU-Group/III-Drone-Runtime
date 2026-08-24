from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache
from iii_drone_runtime.api.payload import PayloadStatusCache, RosGripperServiceAdapter
from iii_drone_runtime.api import payload as payload_module


class _FakeGripperService:
    def __init__(self):
        self.commands = []

    def command(self, command):
        self.commands.append(command)
        return {"success": True, "command": command, "service_name": "/payload/charger_gripper/gripper_command"}


class _FakeRosGripperClient:
    def __init__(self):
        self.requests = []

    def wait_for_service(self, timeout_sec):
        return timeout_sec == 1.0

    def call_async(self, request):
        self.requests.append(request)
        response = SimpleNamespace(gripper_command_response=0)

        class _Future:
            def add_done_callback(self, callback):
                callback(self)

            def exception(self):
                return None

            def result(self):
                return response

        return _Future()


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


def _payload_status():
    cache = PayloadStatusCache()
    cache.handle_battery_voltage(SimpleNamespace(data=24.5))
    cache.handle_charging_power(SimpleNamespace(data=120.0))
    cache.handle_charger_operating_mode(SimpleNamespace(operating_mode=1))
    cache.handle_charger_status(SimpleNamespace(charger_status=1))
    cache.handle_gripper_status(SimpleNamespace(gripper_status=0))
    return cache


def _client(*, mission_active=False, operation_active=False, gripper_service=None):
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
            payload_status=_payload_status(),
            gripper_service=gripper_service or _FakeGripperService(),
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_payload_status_includes_current_gui_fields_and_permissions():
    client = _client()
    headers = _headers(client)

    response = client.get("/payload/status", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["battery_voltage"] == 24.5
    assert payload["charging_power"] == 120.0
    assert payload["gripper_status"] == "open"
    assert payload["charger_status"] == "charging"
    assert payload["latest"]["charger_operating_mode_label"] == "mode_1"
    assert payload["latest"]["permissions"]["gripper_commands_allowed"] is True


def test_ros_gripper_adapter_resolves_runtime_node_lazily_and_rebuilds_client(monkeypatch):
    first_node = object()
    second_node = object()
    current_node = [None]
    clients = []
    created_with = []

    def create_client(node, service_type, service_name):
        del service_type
        created_with.append((node, service_name))
        client = _FakeRosGripperClient()
        clients.append(client)
        return client

    monkeypatch.setattr(payload_module, "create_reentrant_client", create_client)
    adapter = RosGripperServiceAdapter(node_provider=lambda: current_node[0])

    with pytest.raises(RuntimeError, match="runtime ROS node is not available"):
        adapter.command("close")

    current_node[0] = first_node
    assert adapter.command("close")["success"] is True
    assert clients[0].requests[0].gripper_command == 1

    current_node[0] = second_node
    assert adapter.command("open")["success"] is True
    assert clients[1].requests[0].gripper_command == 0
    assert created_with == [
        (first_node, "/payload/charger_gripper/gripper_command"),
        (second_node, "/payload/charger_gripper/gripper_command"),
    ]


def test_gripper_commands_call_typed_service_path_and_update_status():
    service = _FakeGripperService()
    client = _client(gripper_service=service)
    headers = _headers(client)

    close_response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "gripper-close", "command_id": CommandId.PAYLOAD_GRIPPER_CLOSE.value},
    )
    status = client.get("/payload/status", headers=headers)

    assert close_response.status_code == 200
    assert close_response.json()["accepted"] is True
    assert service.commands == ["close"]
    assert close_response.json()["result"]["gripper"]["service_name"] == "/payload/charger_gripper/gripper_command"
    assert status.json()["gripper_status"] == "closed"


def test_gripper_commands_reject_in_mission_mode_with_explicit_reason():
    service = _FakeGripperService()
    client = _client(mission_active=True, gripper_service=service)
    headers = _headers(client)

    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "gripper-open", "command_id": CommandId.PAYLOAD_GRIPPER_OPEN.value},
    )

    assert response.json()["accepted"] is False
    assert response.json()["rejection"]["code"] == "forbidden"
    assert "Mission mode" in response.json()["rejection"]["message"]
    assert service.commands == []


def test_gripper_commands_reject_during_active_custom_operation_with_explicit_reason():
    service = _FakeGripperService()
    client = _client(operation_active=True, gripper_service=service)
    headers = _headers(client)

    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "gripper-open", "command_id": CommandId.PAYLOAD_GRIPPER_OPEN.value},
    )

    assert response.json()["accepted"] is False
    assert "custom operation action is active" in response.json()["rejection"]["message"]
    assert service.commands == []
