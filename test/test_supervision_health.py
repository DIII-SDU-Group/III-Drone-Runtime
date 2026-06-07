from types import SimpleNamespace
import sys
import types

from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.supervision_health import SupervisionHealthCache
from iii_drone_runtime.api.system_adapter import RuntimeSystemStatus


def _client(cache: SupervisionHealthCache, system_adapter=None) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            supervision_health=cache,
            system_adapter=system_adapter,
        )
    )


class _SocketHealthySystemAdapter:
    daemon_client = SimpleNamespace()

    def status(self):
        return RuntimeSystemStatus(
            api_state="up",
            daemon_systemd_state="active",
            daemon_socket_state="responding",
            runtime_booted=True,
            system_active=True,
        )


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_supervision_health_cache_represents_missing_topic_explicitly():
    state = SupervisionHealthCache().state()

    assert state.source_availability == "unavailable"
    assert state.booted is None
    assert "not been received" in state.degraded_reason


def test_supervision_health_cache_converts_typed_message_to_system_domain():
    cache = SupervisionHealthCache()
    cache.handle_message(
        SimpleNamespace(
            profile="sim",
            system_state=5,
            ready=False,
            degraded=True,
            degraded_reasons=["px4_gazebo: waiting"],
            managed_node_count=2,
            active_managed_node_count=1,
            service_count=1,
            ready_service_count=0,
            daemon_ready=True,
            runtime_booted=True,
            system_active=False,
            subsystems=[
                SimpleNamespace(
                    subsystem_id="px4_gazebo",
                    label="PX4 Gazebo",
                    status=2,
                    ready=False,
                    degraded=True,
                    reason="waiting",
                    degraded_reasons=["waiting"],
                    owner="supervision",
                )
            ],
        )
    )

    state = cache.state()

    assert state.source_availability == "available"
    assert state.daemon_state == "ready"
    assert state.booted is True
    assert state.active is False
    assert state.latest["subsystems"][0]["subsystem_id"] == "px4_gazebo"


def test_supervision_health_cache_can_subscribe_to_typed_topic(monkeypatch):
    package = types.ModuleType("iii_drone_interfaces")
    msg_module = types.ModuleType("iii_drone_interfaces.msg")
    msg_module.SystemHealthStatus = object
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces", package)
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces.msg", msg_module)

    calls = []

    class _Node:
        def create_subscription(self, message_type, topic, callback, queue_size):
            calls.append((message_type, topic, callback, queue_size))
            return "subscription"

    cache = SupervisionHealthCache()

    assert cache.subscribe(_Node()) == "subscription"
    assert calls[0][1] == "/supervision/system_health"
    assert calls[0][3] == 10


def test_runtime_api_exposes_supervision_health_state():
    cache = SupervisionHealthCache()
    cache.handle_message(
        SimpleNamespace(
            profile="sim",
            system_state=4,
            ready=True,
            degraded=False,
            degraded_reasons=[],
            managed_node_count=1,
            active_managed_node_count=1,
            service_count=1,
            ready_service_count=1,
            daemon_ready=True,
            runtime_booted=True,
            system_active=True,
            subsystems=[],
        )
    )
    client = _client(cache)

    response = client.get("/system/health", headers=_headers(client))

    assert response.status_code == 200
    assert response.json()["source_label"] == "supervision_health"
    assert response.json()["booted"] is True


def test_runtime_api_falls_back_to_daemon_socket_when_supervision_health_topic_is_missing():
    client = _client(SupervisionHealthCache(), system_adapter=_SocketHealthySystemAdapter())

    response = client.get("/system/health", headers=_headers(client))

    assert response.status_code == 200
    assert response.json()["source_label"] == "daemon_socket"
    assert response.json()["source_availability"] == "available"
    assert response.json()["daemon_state"] == "responding"
    assert response.json()["booted"] is True
    assert response.json()["active"] is True


def test_runtime_api_prefers_daemon_socket_activity_when_supervision_health_lags():
    cache = SupervisionHealthCache()
    cache.handle_message(
        SimpleNamespace(
            profile="sim",
            system_state=4,
            ready=False,
            degraded=False,
            degraded_reasons=[],
            managed_node_count=1,
            active_managed_node_count=0,
            service_count=1,
            ready_service_count=1,
            daemon_ready=True,
            runtime_booted=True,
            system_active=False,
            subsystems=[],
        )
    )
    client = _client(cache, system_adapter=_SocketHealthySystemAdapter())

    response = client.get("/system/health", headers=_headers(client))

    assert response.status_code == 200
    assert response.json()["source_label"] == "supervision_health+daemon_socket"
    assert response.json()["source_availability"] == "available"
    assert response.json()["booted"] is True
    assert response.json()["active"] is True
    assert response.json()["latest"]["runtime_status"]["system_active"] is True


def test_subsystem_health_marks_required_missing_subsystems_degraded():
    cache = SupervisionHealthCache()
    cache.handle_message(
        SimpleNamespace(
            profile="sim",
            system_state=4,
            ready=True,
            degraded=False,
            degraded_reasons=[],
            managed_node_count=1,
            active_managed_node_count=1,
            service_count=1,
            ready_service_count=1,
            daemon_ready=True,
            runtime_booted=True,
            system_active=True,
            subsystems=[
                SimpleNamespace(
                    subsystem_id="mission",
                    label="Mission",
                    status=1,
                    ready=True,
                    degraded=False,
                    reason="",
                    degraded_reasons=[],
                    owner="mission",
                )
            ],
        )
    )

    rows = {row["subsystem_id"]: row for row in cache.subsystem_health()}

    assert rows["mission"]["source_availability"] == "available"
    assert rows["perception"]["source_availability"] == "unavailable"
    assert rows["perception"]["degraded"] is True


def test_runtime_api_exposes_required_subsystem_health():
    cache = SupervisionHealthCache()
    cache.handle_message(
        SimpleNamespace(
            profile="sim",
            system_state=4,
            ready=True,
            degraded=False,
            degraded_reasons=[],
            managed_node_count=1,
            active_managed_node_count=1,
            service_count=1,
            ready_service_count=1,
            daemon_ready=True,
            runtime_booted=True,
            system_active=True,
            subsystems=[
                SimpleNamespace(
                    subsystem_id="configuration",
                    label="Configuration",
                    status=1,
                    ready=True,
                    degraded=False,
                    reason="",
                    degraded_reasons=[],
                    owner="configuration",
                )
            ],
        )
    )
    client = _client(cache)

    response = client.get("/subsystems/health", headers=_headers(client))

    assert response.status_code == 200
    rows = {row["subsystem_id"]: row for row in response.json()["subsystems"]}
    assert rows["configuration"]["ready"] is True
    assert rows["supervision"]["source_availability"] == "unavailable"
