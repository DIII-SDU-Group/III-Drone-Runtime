from types import ModuleType, SimpleNamespace
import sys

from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache


def _client(cache: CustomOperationStatusCache) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            operation_status=cache,
        )
    )


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_operation_status_subscription_matches_transient_best_effort_publisher(monkeypatch):
    interfaces = ModuleType("iii_drone_interfaces")
    msg_module = ModuleType("iii_drone_interfaces.msg")
    rclpy_module = ModuleType("rclpy")
    qos_module = ModuleType("rclpy.qos")

    class _QoSProfile:
        def __init__(self, *, depth):
            self.depth = depth
            self.reliability = None
            self.durability = None

    class _ReliabilityPolicy:
        BEST_EFFORT = "best_effort"

    class _DurabilityPolicy:
        TRANSIENT_LOCAL = "transient_local"

    msg_module.CustomOperationModeStatus = object
    msg_module.StringStamped = str
    qos_module.QoSProfile = _QoSProfile
    qos_module.ReliabilityPolicy = _ReliabilityPolicy
    qos_module.DurabilityPolicy = _DurabilityPolicy
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces", interfaces)
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces.msg", msg_module)
    monkeypatch.setitem(sys.modules, "rclpy", rclpy_module)
    monkeypatch.setitem(sys.modules, "rclpy.qos", qos_module)
    calls = []

    class _Node:
        def create_subscription(self, message_type, topic, callback, qos):
            calls.append((message_type, topic, callback, qos))
            return "subscription"

    subscriptions = CustomOperationStatusCache().subscribe(_Node())

    assert subscriptions == ["subscription", "subscription"]
    assert calls[0][1] == "/mission/custom_operation/mode_status"
    assert calls[0][3].depth == 1
    assert calls[0][3].reliability == "best_effort"
    assert calls[0][3].durability == "transient_local"
    assert calls[1][1] == "/mission/custom_operation/status"
    assert calls[1][3].reliability == "best_effort"
    assert calls[1][3].durability == "transient_local"


def test_operation_status_cache_represents_idle_and_active_states():
    cache = CustomOperationStatusCache()
    cache.handle_message(
        SimpleNamespace(
            operation_active=False,
            active_operation="",
            operation_state_label="ready",
            custom_operation_modes_registered=True,
            required_modes=["custom_operation"],
            registered_modes=["custom_operation"],
            owned_mode="CustomOperation",
            mode_id=27,
            control_owner="custom_operation",
            cancel_available=False,
            degraded=False,
            degraded_reasons=[],
        )
    )
    idle_state = cache.state()

    cache.handle_message(
        SimpleNamespace(
            operation_active=True,
            active_operation="fly_to_position",
            operation_state_label="active",
            custom_operation_modes_registered=True,
            required_modes=["custom_operation"],
            registered_modes=["custom_operation"],
            owned_mode="CustomOperation",
            mode_id=27,
            control_owner="custom_operation",
            cancel_available=True,
            degraded=False,
            degraded_reasons=[],
        )
    )
    active_state = cache.state()

    assert idle_state.status == "custom_operation_idle"
    assert idle_state.latest["start_allowed"] is True
    assert idle_state.latest["mode_id"] == 27
    assert cache.mode_id() == 27
    assert active_state.status == "custom_operation_active"
    assert active_state.active_operation_id == "fly_to_position"
    assert "another custom operation is active" in active_state.latest["start_rejections"]


def test_operation_status_cache_exposes_registration_rejection_reason():
    cache = CustomOperationStatusCache()
    cache.handle_message(
        SimpleNamespace(
            operation_active=False,
            active_operation="",
            operation_state_label="degraded",
            custom_operation_modes_registered=False,
            required_modes=["custom_operation"],
            registered_modes=[],
            owned_mode="CustomOperation",
            control_owner="unknown",
            cancel_available=False,
            degraded=True,
            degraded_reasons=["mode registration unavailable"],
        )
    )

    state = cache.state()

    assert state.latest["start_allowed"] is False
    assert "CustomOperation mode is not registered" in state.latest["start_rejections"]
    assert state.degraded_reason == "mode registration unavailable"


def test_operation_status_cache_uses_legacy_status_topic_as_registration_fallback():
    cache = CustomOperationStatusCache()
    cache.handle_legacy_status_message(SimpleNamespace(data='{"mode_id":42,"active":false}'))

    state = cache.state()

    assert state.source_label == "custom_operation_legacy_status"
    assert state.source_availability == "available"
    assert state.degraded_reason is None
    assert state.latest["custom_operation_modes_registered"] is True
    assert state.latest["registered_modes"] == ["custom_operation"]
    assert state.latest["legacy_status"]["mode_id"] == 42


def test_operation_status_cache_uses_legacy_publisher_graph_when_samples_are_missing():
    cache = CustomOperationStatusCache()

    class _Node:
        def count_publishers(self, topic):
            assert topic == "/mission/custom_operation/status"
            return 1

    cache.refresh_graph_state(_Node())
    state = cache.state()

    assert state.source_label == "custom_operation_legacy_status"
    assert state.source_availability == "available"
    assert state.degraded_reason is None
    assert state.latest["custom_operation_modes_registered"] is True
    assert state.latest["legacy_status_topic_available"] is True


def test_runtime_api_exposes_operation_status_domain():
    cache = CustomOperationStatusCache()
    cache.handle_message(
        SimpleNamespace(
            operation_active=True,
            active_operation="hover",
            operation_state_label="active",
            custom_operation_modes_registered=True,
            required_modes=["custom_operation"],
            registered_modes=["custom_operation"],
            owned_mode="CustomOperation",
            control_owner="custom_operation",
            cancel_available=True,
            degraded=False,
            degraded_reasons=[],
        )
    )
    client = _client(cache)

    response = client.get("/operations/status", headers=_headers(client))

    assert response.status_code == 200
    assert response.json()["status"] == "custom_operation_active"
    assert response.json()["active_operation_id"] == "hover"
