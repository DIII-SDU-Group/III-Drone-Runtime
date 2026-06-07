from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.safety import RuntimeMutationGate, VehicleSafetyState
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_runtime_commands import _FakeDaemonClient, _FakeSystemd


def _client(state: VehicleSafetyState) -> TestClient:
    adapter = RuntimeSystemAdapter(daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd())
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            system_adapter=adapter,
            mutation_gate=RuntimeMutationGate(state),
        )
    )


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def _runtime_stop(client: TestClient, headers: dict[str, str]) -> dict:
    return client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "gate-1", "command_id": "runtime.stop"},
    ).json()


def test_disarmed_fresh_state_allows_runtime_mutation():
    client = _client(VehicleSafetyState(known=True, fresh=True, armed=False, in_air=False))
    response = _runtime_stop(client, _headers(client))

    assert response["accepted"] is True


def test_armed_state_blocks_runtime_mutation_with_explicit_reason():
    client = _client(VehicleSafetyState(known=True, fresh=True, armed=True, in_air=False))
    headers = _headers(client)
    response = _runtime_stop(client, headers)

    assert response["accepted"] is False
    assert response["message"] == "vehicle is armed"
    events = client.get("/events/recent", headers=headers).json()
    assert events[-1]["message"] == "command rejected: runtime.stop - vehicle is armed"


def test_in_flight_state_blocks_runtime_mutation():
    client = _client(VehicleSafetyState(known=True, fresh=True, armed=False, in_air=True))
    response = _runtime_stop(client, _headers(client))

    assert response["accepted"] is False
    assert response["message"] == "vehicle is in flight"


def test_stale_state_fails_closed():
    client = _client(VehicleSafetyState(known=True, fresh=False, armed=False, in_air=False))
    response = _runtime_stop(client, _headers(client))

    assert response["accepted"] is False
    assert response["message"] == "vehicle state stale"


def test_unknown_state_fails_closed_but_read_only_status_still_available():
    client = _client(VehicleSafetyState(known=False, fresh=False, reason="vehicle state unknown"))
    headers = _headers(client)

    stop = _runtime_stop(client, headers)
    status = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "gate-2", "command_id": "runtime.status"},
    ).json()

    assert stop["accepted"] is False
    assert stop["message"] == "vehicle state unknown"
    assert status["accepted"] is True
