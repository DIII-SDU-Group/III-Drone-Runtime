from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache


def _client(cache: MissionStatusCache) -> TestClient:
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            mission_status=cache,
        )
    )


def _headers(client: TestClient) -> dict[str, str]:
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_mission_status_cache_represents_missing_topic_explicitly():
    state = MissionStatusCache().state()

    assert state.source_availability == "unavailable"
    assert state.required_modes_registered is False
    assert "not been received" in state.degraded_reason


def test_mission_status_cache_exposes_activation_preconditions():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/mission.yaml",
            mission_active=False,
            mission_state_label="ready",
            required_modes=["first", "second"],
            registered_modes=["first"],
            owned_mode="executor",
            mode_id=77,
            control_owner="mission",
            ready=False,
            degraded=True,
            degraded_reasons=["second mode not registered"],
            required_modes_registered=False,
        )
    )

    state = cache.state()

    assert state.active_spec_id == "/missions/mission.yaml"
    assert state.required_modes_registered is False
    assert state.latest["registered_modes"] == ["first"]
    assert state.latest["mode_id"] == 77
    assert cache.mission_mode_id() == 77
    assert state.latest["activation_allowed"] is False
    assert "required mission modes are not registered" in state.latest["activation_rejections"]


def test_runtime_api_exposes_mission_status_domain():
    cache = MissionStatusCache()
    cache.set_system_running(True)
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/mission.yaml",
            mission_active=True,
            mission_state_label="active",
            required_modes=["executor"],
            registered_modes=["executor"],
            owned_mode="executor",
            control_owner="mission",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )
    client = _client(cache)

    response = client.get("/mission/status", headers=_headers(client))

    assert response.status_code == 200
    payload = response.json()
    assert payload["active_spec_id"] == "/missions/mission.yaml"
    assert payload["required_modes_registered"] is True
    assert payload["latest"]["activation_allowed"] is True
