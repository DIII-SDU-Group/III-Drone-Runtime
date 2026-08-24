from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId, VehicleDomainState
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache


class _IntentService:
    def __init__(self):
        self.calls = []

    def set_intent(self, service_name, value):
        self.calls.append((service_name, value))
        return {"success": True, "message": "runtime intent enqueued seq=4", "value": value}


class _MissionVehicleProvider:
    def state(self):
        return VehicleDomainState(
            armed=True,
            in_air=True,
            nav_state="mission",
            flight_mode="mission",
            freshness="fresh",
            source_availability="available",
            latest={"command_transport": {"command_available": True}},
        )

    def dangerous_command_rejection_reason(self):
        return None


def _mission_status(active_mode):
    cache = MissionStatusCache()
    modes = [
        SimpleNamespace(
            mode_key=key,
            display_name=key,
            mode_id=index + 20,
            mode_id_valid=True,
            registered=True,
            active=key == active_mode,
            tree_running=key == active_mode,
            tree_finished=False,
            tree_success=False,
            degraded=False,
            degraded_reason="",
        )
        for index, key in enumerate(("inspection_demo", "reach_cable", "cable_charging", "leave_cable"))
    ]
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/mission/mission_specification.yaml",
            mission_active=True,
            mission_state_label="active",
            required_modes=[mode.mode_key for mode in modes],
            registered_modes=[mode.mode_key for mode in modes],
            modes=modes,
            owned_mode="inspection_demo",
            control_owner="mission",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
            intents=[],
        )
    )
    cache.set_system_running(True)
    return cache


def _client(active_mode, service):
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            mission_status=_mission_status(active_mode),
            mission_intent_service=service,
            px4_state_provider=_MissionVehicleProvider(),
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_recharge_now_is_phase_gated_and_uses_onboard_intent_service():
    service = _IntentService()
    client = _client("inspection_demo", service)
    response = client.post(
        "/commands/actions/start",
        headers=_headers(client),
        json={"request_id": "recharge", "command_id": CommandId.MISSION_RECHARGE_NOW.value},
    ).json()

    assert response["accepted"] is True
    assert service.calls == [("/mission/inspection_demo/trigger_recharge_now", True)]


def test_charging_intents_expose_stay_and_leave_semantics():
    service = _IntentService()
    client = _client("cable_charging", service)
    headers = _headers(client)
    stay = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "stay",
            "command_id": CommandId.MISSION_STAY_ON_CABLE.value,
            "parameters": {"value": True},
        },
    ).json()
    leave = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "leave", "command_id": CommandId.MISSION_LEAVE_CABLE_NOW.value},
    ).json()

    assert stay["accepted"] is True
    assert leave["accepted"] is True
    assert service.calls == [
        ("/mission/cable_charging/stay_on_cable", True),
        ("/mission/cable_charging/interrupt_recharging_now", True),
    ]


def test_intent_rejects_outside_its_semantic_phase():
    service = _IntentService()
    client = _client("inspection_demo", service)
    response = client.post(
        "/commands/actions/start",
        headers=_headers(client),
        json={"request_id": "leave", "command_id": CommandId.MISSION_LEAVE_CABLE_NOW.value},
    ).json()

    assert response["accepted"] is False
    assert "inspection_demo" in response["rejection"]["message"]
    assert service.calls == []
