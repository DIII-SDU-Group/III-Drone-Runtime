from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.safety import RuntimeMutationGate, VehicleSafetyState
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_runtime_commands import _FakeDaemonClient, _FakeSystemd


def _client(state: VehicleSafetyState) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(runtime_id="test-runtime"),
            system_adapter=RuntimeSystemAdapter(
                daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()
            ),
            mutation_gate=RuntimeMutationGate(state),
        )
    )


def _runtime_stop(client: TestClient) -> dict:
    return client.post(
        "/commands/actions/start",
        json={"request_id": "gate-1", "command_id": "runtime.stop"},
    ).json()


def test_disarmed_fresh_state_allows_runtime_mutation():
    assert _runtime_stop(
        _client(VehicleSafetyState(known=True, fresh=True, armed=False, in_air=False))
    )["accepted"] is True


def test_armed_or_airborne_state_blocks_runtime_mutation():
    armed = _runtime_stop(
        _client(VehicleSafetyState(known=True, fresh=True, armed=True, in_air=False))
    )
    airborne = _runtime_stop(
        _client(VehicleSafetyState(known=True, fresh=True, armed=False, in_air=True))
    )

    assert armed["message"] == "vehicle is armed"
    assert airborne["message"] == "vehicle is in flight"


def test_unknown_state_blocks_mutation_but_not_read_only_status():
    client = _client(VehicleSafetyState(known=False, fresh=False, reason="vehicle state unknown"))

    assert _runtime_stop(client)["accepted"] is False
    status = client.post(
        "/commands/actions/start",
        json={"request_id": "gate-2", "command_id": "runtime.status"},
    ).json()
    assert status["accepted"] is True
