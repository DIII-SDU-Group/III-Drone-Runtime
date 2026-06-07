from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId, VehicleDomainState
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache
from iii_drone_runtime.api.perception import PerceptionStatusCache


class _FakePLMapperService:
    def __init__(self):
        self.commands = []

    def command(self, command, *, reset=False):
        self.commands.append((command, reset))
        return {"success": True, "command": command, "reset": reset}


class _FakeOverviewService:
    def __init__(self):
        self.requests = []

    def update(self, *, timeout_s):
        self.requests.append(timeout_s)
        return {"success": True, "timeout_s": timeout_s}


class _VehicleStateProvider:
    def __init__(self, *, nav_state="hold"):
        self._state = VehicleDomainState(
            source_label="px4_fusion",
            freshness="fresh",
            source_availability="available",
            armed=True,
            in_air=True,
            nav_state=nav_state,
        )

    def state(self):
        return self._state

    def dangerous_command_rejection_reason(self):
        return None


def _mission_cache(*, active=False):
    cache = MissionStatusCache()
    cache.handle_message(
        SimpleNamespace(
            active_mission_specification="/missions/mission.yaml",
            mission_active=active,
            mission_state_label="active" if active else "ready",
            required_modes=["mission"],
            registered_modes=["mission"],
            owned_mode="Mission",
            control_owner="mission" if active else "",
            ready=True,
            degraded=False,
            degraded_reasons=[],
            required_modes_registered=True,
        )
    )
    return cache


def _operation_cache(*, active=False):
    cache = CustomOperationStatusCache()
    cache.handle_message(
        SimpleNamespace(
            operation_state_label="active" if active else "ready",
            operation_active=active,
            active_operation="hover" if active else "",
            custom_operation_modes_registered=True,
            required_modes=["CustomOperation"],
            registered_modes=["CustomOperation"],
            owned_mode="CustomOperation",
            control_owner="custom_operation" if active else "",
            cancel_available=active,
            degraded=False,
            degraded_reasons=[],
        )
    )
    return cache


def _perception_status():
    cache = PerceptionStatusCache()
    cache.handle_pl_mapper_state(SimpleNamespace(data="mapping"))
    cache.handle_pl_direction_status(SimpleNamespace(data="healthy"))
    cache.handle_hough_status(SimpleNamespace(data="ready"))
    cache.handle_stored_overview_status(SimpleNamespace(data="Powerline stored"))
    cache.handle_live_powerline(SimpleNamespace(lines=[object(), object()]))
    return cache


def _client(*, mission_active=False, operation_active=False, pl_service=None, overview_service=None, px4_state_provider=None):
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            mission_status=_mission_cache(active=mission_active),
            operation_status=_operation_cache(active=operation_active),
            perception_status=_perception_status(),
            pl_mapper_service=pl_service or _FakePLMapperService(),
            powerline_overview_service=overview_service or _FakeOverviewService(),
            px4_state_provider=px4_state_provider,
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_perception_and_powerline_status_include_current_gui_fields():
    client = _client()
    headers = _headers(client)

    perception = client.get("/perception/status", headers=headers)
    powerline = client.get("/powerline/status", headers=headers)

    assert perception.status_code == 200
    assert perception.json()["pl_mapper_state"] == "mapping"
    assert perception.json()["pl_direction_status"] == "healthy"
    assert perception.json()["hough_status"] == "ready"
    assert perception.json()["latest"]["permissions"]["mutating_commands_allowed"] is True
    assert powerline.json()["stored_overview_status"] == "Powerline stored"
    assert powerline.json()["live_perception_status"] == "available"
    assert powerline.json()["latest"]["live_powerline_line_count"] == 2
    assert powerline.json()["latest"]["live_powerline_publisher_available"] is True
    assert powerline.json()["latest"]["last_live_powerline_sample_at"] is not None


def test_powerline_status_timestamp_refreshes_while_live_topic_publisher_exists():
    cache = PerceptionStatusCache()
    cache.handle_live_powerline(SimpleNamespace(lines=[object(), object()]))
    old_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60)
    cache._last_powerline_update_at = old_timestamp

    class _Node:
        def count_publishers(self, topic):
            assert topic == "/perception/pl_mapper/powerline"
            return 1

    cache.refresh_graph_state(_Node())
    state = cache.powerline_state()

    assert state.source_timestamp > old_timestamp
    assert state.latest["live_powerline_line_count"] == 2
    assert state.latest["live_powerline_publisher_available"] is True


def test_powerline_status_timestamp_refreshes_each_state_read_when_live_topic_is_running():
    cache = PerceptionStatusCache()
    cache.handle_live_powerline(SimpleNamespace(lines=[object(), object()]))
    old_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60)
    cache._last_powerline_update_at = old_timestamp
    cache._live_powerline_publisher_available = True

    state = cache.powerline_state()

    assert state.source_timestamp > old_timestamp
    assert state.latest["last_live_powerline_sample_at"] is not None


def test_pl_mapper_commands_use_typed_service_adapter():
    pl_service = _FakePLMapperService()
    client = _client(pl_service=pl_service)
    headers = _headers(client)

    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "pl-start",
            "command_id": CommandId.PERCEPTION_PL_MAPPER_START.value,
            "parameters": {"reset": True},
        },
    )
    freeze = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "pl-freeze", "command_id": CommandId.PERCEPTION_PL_MAPPER_FREEZE.value},
    )

    assert response.json()["accepted"] is True
    assert freeze.json()["accepted"] is True
    assert pl_service.commands == [("start", True), ("freeze", False)]


def test_powerline_overview_update_uses_typed_service_adapter():
    overview_service = _FakeOverviewService()
    client = _client(overview_service=overview_service)
    headers = _headers(client)

    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "overview-update",
            "command_id": CommandId.POWERLINE_OVERVIEW_UPDATE.value,
            "parameters": {"timeout_s": 7},
        },
    )

    assert response.json()["accepted"] is True
    assert overview_service.requests == [7]


def test_perception_commands_reject_in_mission_mode_and_custom_operation_active():
    pl_service = _FakePLMapperService()
    mission_client = _client(mission_active=True, pl_service=pl_service)
    mission_headers = _headers(mission_client)

    mission_response = mission_client.post(
        "/commands/actions/start",
        headers=mission_headers,
        json={"request_id": "pl-stop", "command_id": CommandId.PERCEPTION_PL_MAPPER_STOP.value},
    )

    operation_client = _client(operation_active=True, pl_service=pl_service)
    operation_headers = _headers(operation_client)
    operation_response = operation_client.post(
        "/commands/actions/start",
        headers=operation_headers,
        json={"request_id": "pl-pause", "command_id": CommandId.PERCEPTION_PL_MAPPER_PAUSE.value},
    )

    assert mission_response.json()["accepted"] is False
    assert "Mission mode" in mission_response.json()["rejection"]["message"]
    assert operation_response.json()["accepted"] is False
    assert "custom operation action is active" in operation_response.json()["rejection"]["message"]
    assert pl_service.commands == []


def test_perception_permissions_reconcile_stale_mission_active_when_px4_is_hold():
    pl_service = _FakePLMapperService()
    client = _client(mission_active=True, pl_service=pl_service, px4_state_provider=_VehicleStateProvider(nav_state="hold"))
    headers = _headers(client)

    status = client.get("/perception/status", headers=headers)
    response = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "pl-start-after-hold",
            "command_id": CommandId.PERCEPTION_PL_MAPPER_START.value,
            "parameters": {"reset": False},
        },
    )

    assert status.json()["latest"]["permissions"]["mutating_commands_allowed"] is True
    assert status.json()["latest"]["permissions"]["mutation_rejections"] == []
    assert response.json()["accepted"] is True
    assert pl_service.commands == [("start", False)]
