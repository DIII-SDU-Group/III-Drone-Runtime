from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.runtime_commands import (
    RUNTIME_MUTATING_COMMANDS,
    RUNTIME_READ_ONLY_COMMANDS,
    runtime_command_permission,
)
from iii_drone_runtime.api.safety import RuntimeMutationGate, VehicleSafetyState
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter


class _FakeDaemonClient:
    def __init__(self):
        self.calls = []

    def ping(self):
        return True

    def status(self):
        self.calls.append(("status",))
        return {
            "booted": True,
            "active": True,
            "managed_nodes": {"node-a": "active"},
            "services": {"service-a": {"ready": True, "reason": "ready"}},
        }

    def boot(self, profile):
        self.calls.append(("boot", profile))
        return {"success": True, "profile": profile}

    def start(self, **kwargs):
        self.calls.append(("start", kwargs))
        return {"success": True}

    def stop(self, **kwargs):
        self.calls.append(("stop", kwargs))
        return {"success": True}

    def restart(self, **kwargs):
        self.calls.append(("restart", kwargs))
        return {"success": True}

    def shutdown(self, **kwargs):
        self.calls.append(("shutdown", kwargs))
        return {"success": True}

    def list_nodes(self):
        self.calls.append(("list_nodes",))
        return ["node-a"]

    def list_services(self):
        self.calls.append(("list_services",))
        return ["service-a"]

    def service_start(self, service_id):
        self.calls.append(("service_start", service_id))
        return {"success": True, "service_id": service_id}

    def service_stop(self, service_id):
        self.calls.append(("service_stop", service_id))
        return {"success": True, "service_id": service_id}

    def service_restart(self, service_id):
        self.calls.append(("service_restart", service_id))
        return {"success": True, "service_id": service_id}


class _FakeSystemd:
    def is_active(self, service):
        del service
        return True

    def start(self, service):
        del service

    def restart(self, service):
        del service


def _client(daemon: _FakeDaemonClient, mutation_gate=None) -> TestClient:
    adapter = RuntimeSystemAdapter(daemon_client=daemon, systemd=_FakeSystemd())
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            system_adapter=adapter,
            mutation_gate=mutation_gate
            if mutation_gate is not None
            else RuntimeMutationGate(VehicleSafetyState(known=True, fresh=True, armed=False, in_air=False)),
        )
    )


def _client_without_runtime_mutation_gate(daemon: _FakeDaemonClient) -> TestClient:
    adapter = RuntimeSystemAdapter(daemon_client=daemon, systemd=_FakeSystemd())
    return TestClient(
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


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_runtime_command_classification_sets_are_explicit():
    assert "runtime.status" in RUNTIME_READ_ONLY_COMMANDS
    assert "runtime.list_entities" in RUNTIME_READ_ONLY_COMMANDS
    assert "runtime.stop" in RUNTIME_MUTATING_COMMANDS
    assert "runtime.service.restart" in RUNTIME_MUTATING_COMMANDS
    assert runtime_command_permission("runtime.status") == "read_only"
    assert runtime_command_permission("runtime.stop") == "mutating"


def test_runtime_status_and_list_commands_use_daemon_and_serialize_results():
    daemon = _FakeDaemonClient()
    client = _client(daemon)
    headers = _headers(client)

    status = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "r-1", "command_id": "runtime.status"},
    ).json()
    nodes = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "r-2", "command_id": "runtime.list_entities"},
    ).json()

    assert status["accepted"] is True
    assert status["result"]["permission"] == "read_only"
    assert status["result"]["daemon"]["booted"] is True
    assert nodes["result"]["daemon"]["managed_nodes"] == {"node-a": "active"}

    services = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "r-3", "command_id": "runtime.list_services"},
    ).json()
    assert services["result"]["daemon"]["services"] == {"service-a": {"ready": True, "reason": "ready"}}


def test_runtime_mutating_commands_use_daemon_and_emit_event_entries():
    daemon = _FakeDaemonClient()
    client = _client(daemon)
    headers = _headers(client)

    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "r-3",
            "command_id": "runtime.service.restart",
            "client_label": "pytest",
            "parameters": {"service_id": "micro_ros_agent"},
        },
    ).json()

    assert response["accepted"] is True
    assert ("service_restart", "micro_ros_agent") in daemon.calls

    events = client.get("/events/recent", headers=headers).json()
    categories = [event["category"] for event in events]
    assert "command_request" in categories
    assert "command_decision" in categories
    assert events[-1]["command_id"] == "runtime.service.restart"


def test_runtime_mutating_commands_are_not_vehicle_gated_by_default():
    daemon = _FakeDaemonClient()
    client = _client_without_runtime_mutation_gate(daemon)
    headers = _headers(client)

    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "r-default-gate", "command_id": "runtime.shutdown"},
    ).json()

    assert response["accepted"] is True
    assert ("shutdown", {"select_nodes": [], "include_dependencies": False}) in daemon.calls


def test_runtime_command_errors_are_serialized_as_rejected_results():
    class _FailingDaemon(_FakeDaemonClient):
        def stop(self, **kwargs):
            del kwargs
            raise RuntimeError("daemon unavailable")

    client = _client(_FailingDaemon())
    headers = _headers(client)

    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "r-4", "command_id": "runtime.stop"},
    ).json()

    assert response["accepted"] is False
    assert response["message"] == "daemon unavailable"
