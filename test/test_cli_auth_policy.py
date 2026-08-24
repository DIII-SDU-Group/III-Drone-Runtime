from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.safety import RuntimeMutationGate, VehicleSafetyState
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter


class _FakeDaemonClient:
    def status(self):
        return {"booted": True, "active": True}

    def stop(self, **kwargs):
        del kwargs
        return {"success": True}


class _FakeSystemd:
    def is_active(self, service):
        del service
        return True

    def start(self, service):
        del service

    def restart(self, service):
        del service


def _client() -> TestClient:
    return TestClient(
        create_app(
            RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="browser-secret",
                cli_token="cli-secret",
            ),
            system_adapter=RuntimeSystemAdapter(daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()),
            mutation_gate=RuntimeMutationGate(VehicleSafetyState(known=True, fresh=True, armed=False, in_air=False)),
        )
    )


def test_cli_token_is_separate_from_browser_password():
    client = _client()

    assert client.get("/cli/readiness", headers={"X-III-CLI-Token": "browser-secret"}).status_code == 401
    assert client.get("/cli/readiness", headers={"X-III-CLI-Token": "cli-secret"}).status_code == 200


def test_read_only_cli_commands_pass_during_active_gui_session():
    client = _client()
    session_token = client.post("/session/login", json={"password": "browser-secret"}).json()["session_token"]

    response = client.post(
        "/cli/commands",
        headers={"X-III-CLI-Token": "cli-secret"},
        json={"request_id": "cli-1", "command_id": "runtime.status", "client_label": "remote-cli"},
    )

    assert response.status_code == 200
    assert response.json()["accepted"] is True
    assert client.get("/session", headers={"Authorization": f"Bearer {session_token}"}).status_code == 200


def test_mutating_cli_commands_conflict_during_active_gui_session_and_emit_event():
    client = _client()
    session_token = client.post(
        "/session/login",
        json={"password": "browser-secret", "client_label": "operator-browser"},
    ).json()["session_token"]

    response = client.post(
        "/cli/commands",
        headers={"X-III-CLI-Token": "cli-secret"},
        json={"request_id": "cli-2", "command_id": "runtime.stop", "client_label": "remote-cli"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted"] is False
    assert payload["rejection"]["code"] == "conflict"
    assert "browser GUI session" in payload["rejection"]["message"]

    events = client.get("/events/recent", headers={"Authorization": f"Bearer {session_token}"}).json()
    assert events[-1]["command_id"] == "runtime.stop"
    assert events[-1]["details"]["client_label"] == "remote-cli"


def test_mutating_cli_commands_pass_without_active_gui_session():
    client = _client()

    response = client.post(
        "/cli/commands",
        headers={"X-III-CLI-Token": "cli-secret"},
        json={"request_id": "cli-3", "command_id": "runtime.stop"},
    )

    assert response.status_code == 200
    assert response.json()["accepted"] is True
