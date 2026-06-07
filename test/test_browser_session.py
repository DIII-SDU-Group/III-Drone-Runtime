from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.session import BrowserSessionLease


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


def _client(clock: _Clock):
    settings = RuntimeApiSettings(
        runtime_id="test-runtime",
        runtime_name="Test Runtime",
        browser_password="secret",
        cli_token="cli-secret",
        heartbeat_interval_seconds=2.0,
        lease_timeout_seconds=8.0,
    )
    lease = BrowserSessionLease(
        lease_timeout_seconds=8.0,
        token_factory=lambda: "session-token",
        time_fn=clock,
    )
    return TestClient(create_app(settings=settings, session_lease=lease))


def test_login_returns_token_usable_by_rest_and_websocket():
    client = _client(_Clock())

    login = client.post("/session/login", json={"password": "secret", "client_label": "browser-a"})
    assert login.status_code == 200
    token = login.json()["session_token"]

    session = client.get("/session", headers={"Authorization": f"Bearer {token}"})
    assert session.status_code == 200
    assert session.json()["client_label"] == "browser-a"

    with client.websocket_connect(f"/ws?token={token}") as websocket:
        assert websocket.receive_json()["message_type"] == "snapshot"


def test_second_login_rejected_while_heartbeat_fresh():
    client = _client(_Clock())

    assert client.post("/session/login", json={"password": "secret"}).status_code == 200

    second = client.post("/session/login", json={"password": "secret"})
    assert second.status_code == 409
    assert "already active" in second.json()["detail"]


def test_refresh_style_reuse_and_heartbeat_updates_metadata():
    clock = _Clock()
    client = _client(clock)
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    before = client.get("/session", headers=headers).json()["last_heartbeat_at"]
    clock.advance(2.0)
    heartbeat = client.post("/session/heartbeat", headers=headers)
    after = heartbeat.json()["last_heartbeat_at"]

    assert heartbeat.status_code == 200
    assert before != after
    assert client.get("/session", headers=headers).status_code == 200


def test_session_expires_after_missed_heartbeat_and_allows_new_login():
    clock = _Clock()
    client = _client(clock)
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    clock.advance(8.1)

    assert client.get("/session", headers=headers).status_code == 401
    assert client.post("/session/login", json={"password": "secret"}).status_code == 200


def test_logout_releases_active_session():
    client = _client(_Clock())
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.post("/session/logout", headers=headers).status_code == 200
    assert client.get("/session", headers=headers).status_code == 401
    assert client.post("/session/login", json={"password": "secret"}).status_code == 200
