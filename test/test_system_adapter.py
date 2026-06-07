from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter


class _FakeDaemonClient:
    def __init__(self, *, ping=True):
        self._ping = ping
        self.calls = []

    def ping(self):
        return self._ping

    def status(self):
        self.calls.append(("status",))
        return {"booted": True, "managed_nodes": {"node-a": "active", "node-b": "active"}}

    def list_nodes(self):
        self.calls.append(("list_nodes",))
        return ["node-a"]

    def list_services(self):
        self.calls.append(("list_services",))
        return ["micro_ros_agent"]

    def log_dir(self, entity_id):
        self.calls.append(("log_dir", entity_id))
        return f"/tmp/{entity_id}"


class _FakeSystemd:
    def __init__(self, *, active=False):
        self.active = active
        self.calls = []

    def is_active(self, service):
        self.calls.append(("is_active", service))
        return self.active

    def start(self, service):
        self.calls.append(("start", service))
        self.active = True

    def restart(self, service):
        self.calls.append(("restart", service))
        self.active = True


def test_system_adapter_reports_api_daemon_socket_and_boot_state():
    adapter = RuntimeSystemAdapter(
        daemon_client=_FakeDaemonClient(ping=True),
        systemd=_FakeSystemd(active=True),
        daemon_service="iii-system-daemon.service",
    )

    status = adapter.status()

    assert status.api_state == "up"
    assert status.daemon_systemd_state == "active"
    assert status.daemon_socket_state == "responding"
    assert status.runtime_booted is True
    assert status.system_active is True


def test_system_adapter_remains_up_when_daemon_down():
    adapter = RuntimeSystemAdapter(
        daemon_client=_FakeDaemonClient(ping=False),
        systemd=_FakeSystemd(active=False),
    )

    status = adapter.status()

    assert status.api_state == "up"
    assert status.daemon_systemd_state == "inactive"
    assert status.daemon_socket_state == "unavailable"
    assert status.runtime_booted is None


def test_system_adapter_start_restart_and_daemon_calls():
    daemon = _FakeDaemonClient(ping=True)
    systemd = _FakeSystemd(active=False)
    adapter = RuntimeSystemAdapter(daemon_client=daemon, systemd=systemd, daemon_service="test.service")

    assert adapter.start_daemon().daemon_systemd_state == "active"
    assert adapter.restart_daemon().daemon_systemd_state == "active"
    assert adapter.list_nodes() == ["node-a"]
    assert adapter.list_services() == ["micro_ros_agent"]
    assert adapter.log_dir("node-a") == "/tmp/node-a"
    assert ("start", "test.service") in systemd.calls
    assert ("restart", "test.service") in systemd.calls


def test_runtime_api_exposes_system_adapter_routes():
    adapter = RuntimeSystemAdapter(
        daemon_client=_FakeDaemonClient(ping=True),
        systemd=_FakeSystemd(active=True),
    )
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            system_adapter=adapter,
        )
    )
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.get("/runtime/status").status_code == 401
    assert client.get("/runtime/status", headers=headers).json()["api_state"] == "up"
    assert client.post("/runtime/daemon/start", headers=headers).json()["daemon_systemd_state"] == "active"
    assert client.get("/runtime/daemon/nodes", headers=headers).json()["managed_nodes"] == ["node-a"]
    assert client.get("/runtime/daemon/services", headers=headers).json()["services"] == ["micro_ros_agent"]
    assert client.get("/runtime/daemon/log-dir/node-a", headers=headers).json()["log_dir"] == "/tmp/node-a"
