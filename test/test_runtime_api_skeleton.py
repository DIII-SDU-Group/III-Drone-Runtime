from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache
from iii_drone_runtime.api.px4_state import FusedPx4StateProvider
from iii_drone_runtime.api.system_adapter import RuntimeSystemStatus
from test_px4_state import _FakeCommandAdapter, _command_status


def _client() -> TestClient:
    app = create_app(
        RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            profile="sim",
            browser_password="secret",
            cli_token="cli-secret",
        )
    )
    return TestClient(app)


class _MutableSystemAdapter:
    daemon_client = SimpleNamespace()

    def __init__(self, *, booted: bool, active: bool):
        self.booted = booted
        self.active = active

    def status(self) -> RuntimeSystemStatus:
        return RuntimeSystemStatus(
            api_state="up",
            daemon_systemd_state="active",
            daemon_socket_state="responding",
            runtime_booted=self.booted,
            system_active=self.active,
        )


def _mission_status_message() -> SimpleNamespace:
    return SimpleNamespace(
        active_catalog_id="inspection-production",
        catalog_hash="sha256:" + "a" * 64,
        active_entry_hash="sha256:" + "b" * 64,
        catalog_ready=True,
        mission_active=False,
        mission_state_label="ready",
        required_modes=["executor"],
        registered_modes=["executor"],
        owned_mode="executor",
        modes=[
            SimpleNamespace(
                mode_key="executor",
                display_name="Inspection Demo",
                mode_id=77,
                mode_id_valid=True,
                registered=True,
                active=False,
                tree_running=False,
                tree_finished=False,
            )
        ],
        required_modes_registered=True,
    )


def test_identity_is_minimal_and_unauthenticated():
    response = _client().get("/identity")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime_id"] == "test-runtime"
    assert payload["runtime_name"] == "Test Runtime"
    assert "system" not in payload
    assert "vehicle" not in payload


def test_developer_endpoints_accept_requests_without_credentials():
    client = _client()

    assert client.get("/session").status_code == 200
    assert client.post("/commands/actions/start", json={"request_id": "r", "command_id": "px4.hold"}).status_code == 200
    assert client.get("/cli/readiness").status_code == 200

    response = client.get("/session", headers={"Authorization": "Bearer wrong"})
    assert response.status_code == 200


def test_detailed_state_and_logs_are_available_to_the_developer():
    client = _client()

    protected_reads = [
        "/runtime/status",
        "/system/health",
        "/subsystems/health",
        "/mission/status",
        "/operations/status",
        "/payload/status",
        "/perception/status",
        "/powerline/status",
        "/rosbag/status",
        "/configuration/status",
        "/vehicle/status",
        "/control/status",
        "/map/state",
        "/events/recent",
        "/logs/sources",
        "/logs/runtime_api/tail",
    ]

    assert client.get("/identity").status_code == 200
    assert client.get("/health").status_code == 200
    for path in protected_reads:
        assert client.get(path).status_code == 200, path


def test_login_and_auth_gated_stub_routes():
    client = _client()

    login = client.post("/session/login", json={"password": "secret", "client_label": "pytest"})
    assert login.status_code == 200
    token = login.json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.get("/session", headers=headers).status_code == 200
    action = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "req-1", "command_id": "px4.hold"},
    )
    assert action.status_code == 200
    assert action.json()["accepted"] is False

    service = client.post(
        "/commands/services/call",
        headers=headers,
        json={"request_id": "req-2", "service_type": "logs", "service_name": "logs.list_sources"},
    )
    assert service.status_code == 200
    assert service.json()["ok"] is False


def test_cli_token_endpoint_and_openapi_schema():
    client = _client()

    readiness = client.get("/cli/readiness", headers={"X-III-CLI-Token": "cli-secret"})
    assert readiness.status_code == 200
    assert readiness.json()["accepted"] is True

    vehicle = client.get(
        "/cli/vehicle/status", headers={"X-III-CLI-Token": "cli-secret"}
    )
    assert vehicle.status_code == 200
    assert vehicle.json()["armed"] is None
    assert client.get("/cli/vehicle/status").status_code == 200

    openapi = client.get("/openapi.json")
    assert openapi.status_code == 200
    schemas = openapi.json()["components"]["schemas"]
    assert "ApiIdentity" in schemas
    assert "CommandRequest" in schemas
    assert "ActionStartResponse" in schemas


def test_websocket_sends_initial_snapshot_without_a_token():
    client = _client()

    with client.websocket_connect("/ws") as websocket:
        message = websocket.receive_json()

    assert message["message_type"] == "snapshot"
    assert message["message_id"] == "initial-snapshot"
    assert "system" in message["payload"]


def test_websocket_initial_snapshot_hydrates_live_vehicle_and_control_state():
    app = create_app(
        RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            profile="sim",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        px4_state_provider=FusedPx4StateProvider(command_adapter=_FakeCommandAdapter(_command_status())),
    )
    client = TestClient(app)
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]

    with client.websocket_connect(f"/ws?token={token}") as websocket:
        message = websocket.receive_json()

    payload = message["payload"]
    assert payload["vehicle"]["armed"] is True
    assert payload["vehicle"]["in_air"] is True
    assert payload["vehicle"]["nav_state"] == "hold"
    assert payload["control"]["latest"]["command_permissions"]["px4.arm"] == []


def test_mission_status_reconciles_system_running_from_live_adapter_without_health_read():
    system_adapter = _MutableSystemAdapter(booted=False, active=False)
    mission_status = MissionStatusCache()
    mission_status.handle_message(_mission_status_message())
    client = TestClient(
        create_app(
            RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                profile="sim",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            system_adapter=system_adapter,
            mission_status=mission_status,
        )
    )

    stopped = client.get("/mission/status")
    assert "system is not running" in stopped.json()["latest"]["activation_rejections"]

    system_adapter.booted = True
    system_adapter.active = True
    running = client.get("/mission/status")
    assert "system is not running" not in running.json()["latest"]["activation_rejections"]

    system_adapter.active = False
    stopped_again = client.get("/mission/status")
    assert "system is not running" in stopped_again.json()["latest"]["activation_rejections"]
