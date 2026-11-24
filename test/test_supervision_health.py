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
    token = client.post("/session/login", json={}).json()["session_token"]
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
    assert calls[0][3].depth == 1
    assert calls[0][3].durability.name == "TRANSIENT_LOCAL"


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
    client = _client(cache, system_adapter=_SocketHealthySystemAdapter())

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


def test_subsystem_health_reports_subsystems_supervision_does_not_run():
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
    # Supervision is reporting but runs no perception process: not degraded.
    assert rows["perception"]["source_availability"] == "not_supervised"
    assert rows["perception"]["degraded"] is False


def test_subsystem_health_is_degraded_without_supervision_health():
    rows = {row["subsystem_id"]: row for row in SupervisionHealthCache().subsystem_health()}

    assert rows["perception"]["source_availability"] == "unavailable"
    assert rows["perception"]["degraded"] is True


def _process(subsystem_id, alive=True):
    return SimpleNamespace(
        subsystem_id=subsystem_id,
        label=subsystem_id,
        status=1 if alive else 3,
        ready=alive,
        degraded=not alive,
        reason="" if alive else "process is not alive",
        degraded_reasons=[] if alive else ["process is not alive"],
        owner="supervision",
    )


def test_subsystem_health_aggregates_supervised_processes():
    # Supervision publishes one subsystem per service and process (HIL,
    # 2026-10-05); the operator subsystems used to read as missing.
    cache = SupervisionHealthCache()
    cache.handle_message(
        SimpleNamespace(
            profile="hil",
            system_state=4,
            ready=False,
            degraded=True,
            degraded_reasons=[],
            managed_node_count=12,
            active_managed_node_count=11,
            service_count=1,
            ready_service_count=1,
            daemon_ready=True,
            runtime_booted=True,
            system_active=False,
            subsystems=[
                _process("micro_ros_agent"),
                _process("configuration_server"),
                _process("hough_transformer"),
                _process("pl_dir_computer"),
                _process("pl_mapper", alive=False),
                _process("maneuver_controller"),
                _process("trajectory_generator"),
                _process("tf"),
                _process("mission_executor"),
                _process("rosbag_recorder"),
            ],
        )
    )

    rows = {row["subsystem_id"]: row for row in cache.subsystem_health()}

    assert rows["control"]["ready"] is True
    assert rows["control"]["members"] == ["maneuver_controller", "trajectory_generator", "tf", "micro_ros_agent"]
    assert rows["perception"]["ready"] is False
    assert rows["perception"]["degraded"] is True
    assert rows["perception"]["degraded_reasons"] == ["pl_mapper: process is not alive"]
    assert rows["configuration"]["members"] == ["configuration_server"]
    assert rows["supervision"]["ready"] is True
    assert rows["payload"]["source_availability"] == "not_supervised"
    assert rows["payload"]["degraded"] is False


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
    # The daemon publishing the health message is the supervision subsystem.
    assert rows["supervision"]["ready"] is True
