from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.safety import (
    ReceiverClockGate,
    RuntimeMutationGate,
    VehicleSafetyState,
)
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_runtime_commands import _FakeDaemonClient, _FakeSystemd


def _client(state: VehicleSafetyState) -> TestClient:
    adapter = RuntimeSystemAdapter(
        daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()
    )
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
    token = client.post("/session/login", json={"password": "secret"}).json()[
        "session_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def _runtime_stop(client: TestClient, headers: dict[str, str]) -> dict:
    return client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "gate-1", "command_id": "runtime.stop"},
    ).json()


def test_disarmed_fresh_state_allows_runtime_mutation():
    client = _client(
        VehicleSafetyState(known=True, fresh=True, armed=False, in_air=False)
    )
    response = _runtime_stop(client, _headers(client))

    assert response["accepted"] is True


def test_armed_state_blocks_runtime_mutation_with_explicit_reason():
    client = _client(
        VehicleSafetyState(known=True, fresh=True, armed=True, in_air=False)
    )
    headers = _headers(client)
    response = _runtime_stop(client, headers)

    assert response["accepted"] is False
    assert response["message"] == "vehicle is armed"
    events = client.get("/events/recent", headers=headers).json()
    assert events[-1]["message"] == "command rejected: runtime.stop - vehicle is armed"


def test_in_flight_state_blocks_runtime_mutation():
    client = _client(
        VehicleSafetyState(known=True, fresh=True, armed=False, in_air=True)
    )
    response = _runtime_stop(client, _headers(client))

    assert response["accepted"] is False
    assert response["message"] == "vehicle is in flight"


def test_stale_state_fails_closed():
    client = _client(
        VehicleSafetyState(known=True, fresh=False, armed=False, in_air=False)
    )
    response = _runtime_stop(client, _headers(client))

    assert response["accepted"] is False
    assert response["message"] == "vehicle state stale"


def test_unknown_state_fails_closed_but_read_only_status_still_available():
    client = _client(
        VehicleSafetyState(known=False, fresh=False, reason="vehicle state unknown")
    )
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


def test_receiver_clock_gate_blocks_mutation_but_not_read_only_status(tmp_path):
    boot = tmp_path / "boot-id"
    state = tmp_path / "clock-state.json"
    boot.write_text("boot-a\n", encoding="ascii")
    state.write_text('{"boot_id":"boot-a","gate":"DEGRADED_CLOCK"}\n', encoding="utf-8")
    gate = RuntimeMutationGate(
        VehicleSafetyState(known=True, fresh=True, armed=False, in_air=False),
        clock_gate=ReceiverClockGate(state, boot),
    )
    # Inject through the handler registry by constructing the app with this gate.
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            system_adapter=RuntimeSystemAdapter(
                daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()
            ),
            mutation_gate=gate,
        )
    )
    headers = _headers(client)
    stop = _runtime_stop(client, headers)
    status = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "clock-status", "command_id": "runtime.status"},
    ).json()
    assert stop["accepted"] is False
    assert stop["message"].startswith("DEGRADED_CLOCK")
    assert status["accepted"] is True
    state.write_text('{"boot_id":"boot-a","gate":"OPERATIONAL"}\n', encoding="utf-8")
    opened = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "gate-opened", "command_id": "runtime.stop"},
    ).json()
    assert opened["accepted"] is True


def test_clock_gate_fails_closed_for_missing_stale_and_fault_state(tmp_path):
    boot = tmp_path / "boot-id"
    state = tmp_path / "clock-state.json"
    boot.write_text("boot-b\n", encoding="ascii")
    gate = ReceiverClockGate(state, boot)
    assert gate().startswith("DEGRADED_CLOCK")
    state.write_text('{"boot_id":"boot-a","gate":"OPERATIONAL"}\n', encoding="utf-8")
    assert "another boot" in gate()
    state.write_text(
        '{"boot_id":"boot-b","gate":"CLOCK_FAULT_ACTIVE"}\n', encoding="utf-8"
    )
    assert gate().startswith("CLOCK_FAULT_ACTIVE")


def test_degraded_clock_http_gate_blocks_every_runtime_mutation_surface():
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="clock-gate-test-runtime",
                runtime_name="Clock Gate Test Runtime",
                profile="sim",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            system_adapter=RuntimeSystemAdapter(
                daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()
            ),
            clock_gate_provider=lambda: (
                "DEGRADED_CLOCK: synchronize the aircraft clock"
            ),
        )
    )
    headers = _headers(client)
    assert client.get("/runtime/status", headers=headers).status_code == 200
    blocked = client.post(
        "/configuration/apply",
        headers=headers,
        json={},
    )
    assert blocked.status_code == 409
    assert blocked.json() == {
        "code": "DEGRADED_CLOCK",
        "detail": "DEGRADED_CLOCK: synchronize the aircraft clock",
        "mutation_allowed": False,
    }
    command = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "clock-block", "command_id": "runtime.status"},
    )
    assert command.status_code == 200
    assert command.json()["accepted"] is True
    mutation = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "clock-mutation", "command_id": "runtime.stop"},
    )
    assert mutation.status_code == 409
    assert mutation.json()["detail"]["code"] == "DEGRADED_CLOCK"
