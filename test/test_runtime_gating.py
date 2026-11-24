from fastapi.testclient import TestClient
import pytest

from iii_drone_contracts import TelemetryFieldState, VehicleDomainState
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.px4_state import FusedPx4StateProvider
from iii_drone_runtime.api.safety import (
    RuntimeMutationGate,
    VehicleSafetyState,
    vehicle_safety_state,
)
from iii_drone_runtime.api.system_adapter import RuntimeSystemAdapter
from test_px4_state import _FakeCommandAdapter, _command_status, _ros_cache
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


# The application's own gate follows the live fused PX4 state.


class _LiveVehicle:
    def __init__(self, state):
        self.vehicle = state

    def state(self):
        return self.vehicle

    def dangerous_command_rejection_reason(self):
        return None


def _evidence(value, *, freshness="fresh", disagreement=False):
    return TelemetryFieldState(
        value=value,
        source="PX4 ROS/uXRCE",
        freshness=freshness,
        source_availability="degraded" if disagreement else "available",
        disagreement=disagreement,
        detail="MAVSDK and ROS/uXRCE disagree" if disagreement else None,
    )


def _fused(*, armed=False, in_air=False, freshness="fresh", disagreement=False):
    return VehicleDomainState(
        freshness="fresh",
        source_availability="available",
        armed=armed,
        in_air=in_air,
        telemetry_fields={
            "armed": _evidence(armed, freshness=freshness, disagreement=disagreement),
            "in_air": _evidence(in_air, freshness=freshness),
        },
    )


def _wired_client(vehicle, *, profile="real") -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(runtime_id="test-runtime", profile=profile),
            system_adapter=RuntimeSystemAdapter(
                daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()
            ),
            px4_state_provider=vehicle,
        )
    )


@pytest.mark.parametrize("profile", ["real", "opti_track"])
def test_aircraft_runtime_gate_follows_the_live_vehicle_state(profile: str):
    vehicle = _LiveVehicle(_fused())
    client = _wired_client(vehicle, profile=profile)

    def command(request_id, command_id):
        return client.post(
            "/commands/actions/start",
            json={"request_id": request_id, "command_id": command_id},
        ).json()

    assert command("landed", "runtime.restart")["accepted"] is True

    vehicle.vehicle = _fused(armed=True, in_air=True)
    for command_id in ("runtime.stop", "runtime.restart", "runtime.shutdown"):
        response = command(f"flying-{command_id}", command_id)
        assert response["accepted"] is False
        assert response["message"] == "vehicle is armed"

    vehicle.vehicle = _fused(armed=False, in_air=True)
    assert command("airborne-disarmed", "runtime.stop")["message"] == "vehicle is in flight"


@pytest.mark.parametrize(
    ("state", "message"),
    [
        (VehicleDomainState(), "vehicle state unknown"),
        (_fused(freshness="stale"), "vehicle state stale"),
        (
            _fused(disagreement=True),
            "vehicle armed/in-air sources disagree: MAVSDK and ROS/uXRCE disagree",
        ),
    ],
)
def test_aircraft_runtime_gate_refuses_unknown_stale_or_disputed_state(state, message):
    response = _runtime_stop(_wired_client(_LiveVehicle(state), profile="opti_track"))

    assert response["accepted"] is False
    assert response["message"] == message


def test_aircraft_runtime_gate_refuses_before_any_px4_state_arrives():
    # The default fused provider: no MAVSDK or ROS/uXRCE sample yet.
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(runtime_id="test-runtime", profile="real"),
            system_adapter=RuntimeSystemAdapter(
                daemon_client=_FakeDaemonClient(), systemd=_FakeSystemd()
            ),
        )
    )

    response = _runtime_stop(client)

    assert response["accepted"] is False
    assert response["message"].startswith("vehicle state unknown")


@pytest.mark.parametrize("profile", ["hil", "sim"])
def test_virtual_profiles_keep_runtime_lifecycle_without_vehicle_state(profile: str):
    response = _runtime_stop(_wired_client(_LiveVehicle(VehicleDomainState()), profile=profile))

    assert response["accepted"] is True


def test_disarmed_landed_aircraft_stays_mutable_without_the_mavsdk_transport():
    ros = _ros_cache(armed=False, in_air=False)
    vehicle = FusedPx4StateProvider(
        command_adapter=_FakeCommandAdapter(
            _command_status(
                connected=False,
                source_availability="unavailable",
                degraded_reason="MAVLink endpoint silent",
                armed=None,
                in_air=None,
                nav_state=None,
                last_update_at=None,
            )
        ),
        ros_state=ros,
    ).state()

    safety = vehicle_safety_state(vehicle)

    assert vehicle.freshness == "stale"
    assert safety == VehicleSafetyState(known=True, fresh=True, armed=False, in_air=False)
    assert RuntimeMutationGate(profile="real", state_provider=lambda: safety).rejection_reason("runtime.stop") is None
