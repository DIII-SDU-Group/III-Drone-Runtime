from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from threading import Thread
import time

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId, VehicleDomainState
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache
from iii_drone_runtime.api.perception import PerceptionStatusCache, RosPowerlineOverviewServiceAdapter


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


class _FakePylonService:
    def __init__(self):
        self.captures = []
        self.clear_count = 0

    def capture_current(self, *, pylon_id, replace_existing):
        self.captures.append((pylon_id, replace_existing))
        return {
            "success": True,
            "captured_pylon": {"id": pylon_id, "x": 1.0, "y": 2.0},
            "stored_pylon_overview": {"frame_id": "world", "pylons": []},
            "persistence_source": "operator_capture_gnss",
        }

    def clear(self):
        self.clear_count += 1
        return {"success": True, "stored_pylon_overview": {"frame_id": "world", "pylons": []}}


class _FakeRosbagAdapter:
    def __init__(self):
        self.recording = False

    def status(self):
        return {"recording": self.recording, "owner": "inspection" if self.recording else "unknown", "free_space_bytes": 10 << 30}

    def start(self, request):
        self.recording = True
        return {"success": True, "recording": True, "owner": request.get("owner")}

    def stop(self, request):
        self.recording = False
        return {"success": True, "was_running": True}

    def list_recordings(self):
        return []

    def download(self, recording_id):
        return {"recording_id": recording_id}


def test_ros_service_adapter_waits_for_existing_executor_without_spinning_node():
    class _Request:
        timeout_s = 0

    class _Service:
        Request = _Request

    class _Future:
        callback = None

        def add_done_callback(self, callback):
            self.callback = callback

        def result(self):
            return "stored"

    future = _Future()

    class _Client:
        def wait_for_service(self, *, timeout_sec):
            return True

        def call_async(self, request):
            assert request.timeout_s == 5

            def complete():
                time.sleep(0.01)
                future.callback(future)

            Thread(target=complete).start()
            return future

    class _Node:
        def create_client(self, service_type, service_name):
            assert service_type is _Service
            assert service_name == "/overview"
            return _Client()

    adapter = RosPowerlineOverviewServiceAdapter(node_provider=lambda: _Node())

    assert adapter._call(_Service, "/overview", timeout_sec=0.2, timeout_s=5) == "stored"


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
            active_catalog_id="inspection-production",
            catalog_hash="sha256:" + "a" * 64,
            active_entry_hash="sha256:" + "b" * 64,
            default_catalog_id="inspection-production",
            configuration_profile="sim",
            classification="production",
            compatible_profiles=["real", "opti_track", "sim"],
            temporary_override=False,
            experimental=False,
            experimental_warning="",
            catalog_ready=True,
            catalog_error="",
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
    cache.handle_pl_mapper_state(SimpleNamespace(data="running"))
    cache.handle_pl_direction_status(SimpleNamespace(data="healthy"))
    cache.handle_hough_status(SimpleNamespace(data="ready"))
    cache.handle_stored_overview_status(SimpleNamespace(data="Powerline stored"))
    cache.handle_stored_pylon_status(SimpleNamespace(data="Pylon overview stored"))
    cache.handle_live_powerline(SimpleNamespace(lines=[object(), object(), object(), object()]))
    return cache


def _client(*, mission_active=False, operation_active=False, perception_status=None, pl_service=None, overview_service=None, pylon_service=None, px4_state_provider=None, rosbag_adapter=None):
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
            perception_status=perception_status or _perception_status(),
            pl_mapper_service=pl_service or _FakePLMapperService(),
            powerline_overview_service=overview_service or _FakeOverviewService(),
            pylon_overview_service=pylon_service or _FakePylonService(),
            px4_state_provider=px4_state_provider,
            rosbag_adapter=rosbag_adapter or _FakeRosbagAdapter(),
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
    assert perception.json()["pl_mapper_state"] == "running"
    assert perception.json()["pl_direction_status"] == "healthy"
    assert perception.json()["hough_status"] == "ready"
    assert perception.json()["latest"]["permissions"]["mutating_commands_allowed"] is True
    assert powerline.json()["stored_overview_status"] == "Powerline stored"
    assert powerline.json()["latest"]["stored_pylon_status"] == "Pylon overview stored"
    assert powerline.json()["latest"]["mission_overview_rejections"] == []
    assert powerline.json()["live_perception_status"] == "available"
    assert powerline.json()["latest"]["live_powerline_line_count"] == 4
    assert powerline.json()["latest"]["live_powerline_publisher_available"] is True
    assert powerline.json()["latest"]["last_live_powerline_sample_at"] is not None


def test_mission_overview_rejections_identify_missing_invalid_and_stale_inputs():
    now = datetime.now(timezone.utc)
    cache = PerceptionStatusCache(overview_stale_after_seconds=3.0)

    assert cache.mission_overview_rejections(now=now) == [
        "stored powerline overview status has not been received",
        "stored pylon overview status has not been received",
    ]

    cache.handle_stored_overview_status(SimpleNamespace(data="No powerline stored"))
    cache.handle_stored_pylon_status(SimpleNamespace(data="No valid pylon overview stored"))
    assert cache.mission_overview_rejections() == [
        "No powerline stored",
        "No valid pylon overview stored",
    ]

    cache.handle_stored_overview_status(SimpleNamespace(data="Powerline stored on disk (GNSS)"))
    cache.handle_stored_pylon_status(SimpleNamespace(data="Pylon overview stored on disk (GNSS)"))
    assert cache.mission_overview_rejections() == []

    stale_time = datetime.now(timezone.utc) + timedelta(seconds=4)
    assert cache.mission_overview_rejections(now=stale_time) == [
        "stored powerline overview status is stale",
        "stored pylon overview status is stale",
    ]


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
    pl_service = _FakePLMapperService()
    client = _client(overview_service=overview_service, pl_service=pl_service)
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
    assert pl_service.commands == [("freeze", False)]


def test_powerline_overview_readiness_rejection_is_retryable():
    status = _perception_status()
    status.handle_live_powerline(SimpleNamespace(lines=[object(), object(), object()]))
    client = _client(perception_status=status)

    response = client.post(
        "/commands/actions/start",
        headers=_headers(client),
        json={"request_id": "overview-not-ready", "command_id": CommandId.POWERLINE_OVERVIEW_UPDATE.value},
    ).json()

    assert response["accepted"] is False
    assert response["rejection"]["code"] == "degraded_state"
    assert response["rejection"]["retryable"] is True
    assert "at least 4 live powerline lines" in response["rejection"]["message"]


def test_overview_storage_fails_closed_when_recording_storage_is_critical():
    adapter = _FakeRosbagAdapter()
    adapter.status = lambda: {"recording": False, "owner": "unknown", "free_space_bytes": 100}
    overview_service = _FakeOverviewService()
    client = _client(overview_service=overview_service, rosbag_adapter=adapter)

    response = client.post(
        "/commands/actions/start",
        headers=_headers(client),
        json={"request_id": "overview-low-storage", "command_id": CommandId.POWERLINE_OVERVIEW_UPDATE.value},
    ).json()

    assert response["accepted"] is False
    assert "critically low" in response["rejection"]["message"]
    assert overview_service.requests == []


def test_pylon_capture_and_clear_use_onboard_service_without_browser_coordinates():
    pylon_service = _FakePylonService()
    client = _client(pylon_service=pylon_service)
    headers = _headers(client)

    captured = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "capture-pylon-1",
            "command_id": CommandId.PYLON_CAPTURE_CURRENT.value,
            "parameters": {"pylon_id": 1, "replace_existing": True, "x": 999, "y": 999},
        },
    ).json()
    cleared = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "clear-pylons", "command_id": CommandId.PYLON_OVERVIEW_CLEAR.value},
    ).json()

    assert captured["accepted"] is True
    assert pylon_service.captures == [(1, True)]
    assert captured["result"]["result"]["captured_pylon"]["id"] == 1
    assert cleared["accepted"] is True
    assert cleared["result"]["result"]["stored_pylon_overview"]["pylons"] == []
    assert pylon_service.clear_count == 1


def test_pylon_provider_rejection_preserves_exact_reason_and_final_state():
    class _RejectingPylonService(_FakePylonService):
        def capture_current(self, *, pylon_id, replace_existing):
            del pylon_id, replace_existing
            return {
                "success": False,
                "message": "horizontal speed must remain below the capture threshold for the full dwell time",
                "stored_pylon_overview": {"frame_id": "world", "pylons": []},
            }

    client = _client(pylon_service=_RejectingPylonService())
    response = client.post(
        "/commands/actions/start",
        headers=_headers(client),
        json={
            "request_id": "capture-pylon-moving",
            "command_id": CommandId.PYLON_CAPTURE_CURRENT.value,
            "parameters": {"pylon_id": 1},
        },
    ).json()

    assert response["accepted"] is False
    assert response["rejection"]["code"] == "degraded_state"
    assert "horizontal speed" in response["rejection"]["message"]
    assert response["result"]["result"]["stored_pylon_overview"]["pylons"] == []


def test_typed_pylon_status_distinguishes_partial_complete_and_gnss_only():
    cache = PerceptionStatusCache()
    stamp = SimpleNamespace(sec=1_800_000_000, nanosec=0)

    cache.handle_pylon_overview_status(
        SimpleNamespace(
            stamp=stamp,
            valid=False,
            pylon_count=1,
            pylon_ids=[2],
            overview_in_frame=True,
            overview_gnss_only=False,
            overview_source="operator_capture_memory_world",
            persistence_file_present=True,
            degraded_reason="pylon overview requires exactly two captured endpoints",
            overview=SimpleNamespace(frame_id="world", pylons=[SimpleNamespace(id=2, x=1.0, y=2.0)]),
        )
    )
    partial = cache.powerline_state().pylon_overview
    assert partial.valid is False
    assert partial.pylon_count == 1
    assert partial.pylon_ids == [2]
    assert partial.frame_id == "world"

    cache.handle_pylon_overview_status(
        SimpleNamespace(
            stamp=stamp,
            valid=True,
            pylon_count=2,
            pylon_ids=[1, 2],
            overview_in_frame=True,
            overview_gnss_only=False,
            overview_source="loaded_gnss_to_world",
            persistence_file_present=True,
            degraded_reason="",
            overview=SimpleNamespace(
                frame_id="world",
                pylons=[SimpleNamespace(id=1, x=0.0, y=0.0), SimpleNamespace(id=2, x=10.0, y=0.0)],
            ),
        )
    )
    complete = cache.powerline_state().pylon_overview
    assert complete.valid is True
    assert complete.overview_source == "loaded_gnss_to_world"

    cache.handle_pylon_overview_status(
        SimpleNamespace(
            stamp=stamp,
            valid=False,
            pylon_count=0,
            pylon_ids=[],
            overview_in_frame=False,
            overview_gnss_only=True,
            overview_source="gnss_only_unavailable",
            persistence_file_present=True,
            degraded_reason="GNSS pylon data cannot be reprojected into the active world frame",
            overview=SimpleNamespace(frame_id="world", pylons=[]),
        )
    )
    gnss_only = cache.powerline_state().pylon_overview
    assert gnss_only.overview_gnss_only is True
    assert gnss_only.degraded_reason.startswith("GNSS pylon data")


def test_typed_pylon_status_marks_unreceived_and_aged_data_explicitly():
    cache = PerceptionStatusCache(overview_stale_after_seconds=3.0)

    empty = cache.powerline_state().pylon_overview
    assert empty.freshness == "unknown"
    assert "has not been received" in empty.degraded_reason

    cache.handle_pylon_overview_status(
        SimpleNamespace(
            stamp=SimpleNamespace(sec=1_800_000_000, nanosec=0),
            valid=True,
            pylon_count=2,
            pylon_ids=[1, 2],
            overview_in_frame=True,
            overview_gnss_only=False,
            overview_source="loaded_gnss_to_world",
            persistence_file_present=True,
            degraded_reason="",
            overview=SimpleNamespace(
                frame_id="world",
                pylons=[SimpleNamespace(id=1, x=0.0, y=0.0), SimpleNamespace(id=2, x=10.0, y=0.0)],
            ),
        )
    )
    cache._last_pylon_update_at = datetime.now(timezone.utc) - timedelta(seconds=4)

    stale = cache.powerline_state().pylon_overview
    assert stale.freshness == "stale"
    assert stale.degraded_reason == "stored pylon overview status is stale"


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
