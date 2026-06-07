from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.px4_state import FusedPx4StateProvider
from iii_drone_runtime.api.session import BrowserSessionLease
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


def test_identity_is_minimal_and_unauthenticated():
    response = _client().get("/identity")

    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime_id"] == "test-runtime"
    assert payload["runtime_name"] == "Test Runtime"
    assert "system" not in payload
    assert "vehicle" not in payload


def test_authenticated_endpoints_reject_missing_or_invalid_credentials():
    client = _client()

    assert client.get("/session").status_code == 401
    assert client.post("/commands/actions/start", json={"request_id": "r", "command_id": "px4.hold"}).status_code == 401
    assert client.get("/cli/readiness").status_code == 401

    response = client.get("/session", headers={"Authorization": "Bearer wrong"})
    assert response.status_code == 401


def test_detailed_state_and_logs_reject_unauthenticated_browser_requests():
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
        assert client.get(path).status_code == 401, path


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

    openapi = client.get("/openapi.json")
    assert openapi.status_code == 200
    schemas = openapi.json()["components"]["schemas"]
    assert "ApiIdentity" in schemas
    assert "CommandRequest" in schemas
    assert "ActionStartResponse" in schemas


def test_websocket_rejects_invalid_token_and_sends_initial_snapshot():
    client = _client()

    login = client.post("/session/login", json={"password": "secret"})
    token = login.json()["session_token"]

    with client.websocket_connect(f"/ws?token={token}") as websocket:
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
