from fastapi.testclient import TestClient
import pytest

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.safety import RuntimeMutationGate, VehicleSafetyState
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_runtime_commands import _FakeDaemonClient, _FakeSystemd


def _client(
    state: VehicleSafetyState, *, profile: str = "real", inject_gate: bool = True
) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(runtime_id="test-runtime", profile=profile),
            system_adapter=RuntimeSystemAdapter(
                daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()
            ),
            mutation_gate=(
                RuntimeMutationGate(state, profile=profile) if inject_gate else None
            ),
        )
    )


def _runtime_stop(client: TestClient) -> dict:
    return _runtime_command(client, "runtime.stop")


def _runtime_command(client: TestClient, command_id: str) -> dict:
    return client.post(
        "/commands/actions/start",
        json={"request_id": "gate-1", "command_id": command_id},
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


@pytest.mark.parametrize("profile", ["hil", "sim"])
@pytest.mark.parametrize("command_id", ["runtime.boot", "runtime.start", "runtime.stop", "runtime.shutdown"])
@pytest.mark.parametrize("state", [
    VehicleSafetyState(known=True, fresh=True, armed=True, in_air=True),
    VehicleSafetyState(known=False, fresh=False, reason="vehicle state unknown"),
])
def test_virtual_profiles_allow_runtime_lifecycle_without_vehicle_state(
    profile: str, command_id: str, state: VehicleSafetyState
):
    response = _runtime_command(
        _client(state, profile=profile),
        command_id,
    )

    assert response["accepted"] is True


@pytest.mark.parametrize("profile", ["real", "opti_track"])
def test_physical_profiles_still_block_stop_while_airborne(profile: str):
    response = _runtime_stop(
        _client(
            VehicleSafetyState(known=True, fresh=True, armed=True, in_air=True),
            profile=profile,
        )
    )

    assert response["accepted"] is False
    assert response["message"] == "vehicle is armed"


def test_sim_app_profile_retains_its_existing_no_runtime_mutation_gate_behavior():
    response = _runtime_stop(
        _client(
            VehicleSafetyState(known=False, fresh=False, reason="vehicle state unknown"),
            profile="sim",
            inject_gate=False,
        )
    )

    assert response["accepted"] is True
