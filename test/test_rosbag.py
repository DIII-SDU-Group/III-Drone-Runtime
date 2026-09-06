from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.mission_status import MissionStatusCache


class _FakeRosbagAdapter:
    def __init__(self):
        self.state = {
            "recording": False,
            "recording_id": None,
            "output_dir": None,
            "owner": "unknown",
            "size_bytes": 0,
            "free_space_bytes": 10 << 30,
            "started_at": "2026-08-12T12:00:00+00:00",
        }
        self.started = []
        self.stopped = []
        self.recordings = [{"recording_id": "bag-1", "path": "/tmp/iii_drone/rosbags/bag-1", "size_bytes": 123}]

    def status(self):
        return dict(self.state)

    def start(self, request):
        self.started.append(request)
        self.state.update(
            recording=True,
            recording_id=request.get("recording_id") or "manual-bag",
            output_dir=request.get("output_dir") or "/tmp/iii_drone/rosbags/manual-bag",
            owner=request.get("owner", "manual"),
            size_bytes=0,
        )
        return {"success": True, **self.status()}

    def stop(self, request):
        self.stopped.append(request)
        old = self.status()
        self.state.update(recording=False, owner="unknown")
        return {"success": True, "was_running": old["recording"], **old}

    def list_recordings(self):
        return list(self.recordings)

    def download(self, recording_id):
        return {"recording_id": recording_id, "path": f"/tmp/iii_drone/rosbags/{recording_id}", "download_supported": True}


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


def _client(adapter, *, mission_active=False):
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            mission_status=_mission_cache(active=mission_active),
            rosbag_adapter=adapter,
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_rosbag_status_lists_owner_and_available_recordings():
    adapter = _FakeRosbagAdapter()
    adapter.state.update(recording=True, recording_id="mission-bag", output_dir="/bags/mission-bag", owner="mission")
    client = _client(adapter)
    headers = _headers(client)

    status = client.get("/rosbag/status", headers=headers)
    listing = client.get("/rosbags", headers=headers)

    assert status.status_code == 200
    assert status.json()["recording"] is True
    assert status.json()["owner"] == "mission"
    assert status.json()["recording_id"] == "mission-bag"
    assert listing.json()["recordings"][0]["recording_id"] == "bag-1"


def test_manual_start_stop_and_download_use_recorder_adapter():
    adapter = _FakeRosbagAdapter()
    client = _client(adapter)
    headers = _headers(client)

    start = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "rosbag-start",
            "command_id": CommandId.ROSBAG_START.value,
            "parameters": {"recording_id": "manual-1", "all_topics": True},
        },
    )
    stop = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "rosbag-stop", "command_id": CommandId.ROSBAG_STOP.value},
    )
    download = client.get("/rosbags/manual-1/download", headers=headers)

    assert start.json()["accepted"] is True
    assert stop.json()["accepted"] is True
    assert adapter.started[0]["recording_id"] == "manual-1"
    assert adapter.stopped[0]["timeout_sec"] == 5.0
    assert download.json()["download_supported"] is True


def test_mission_mode_requires_press_hold_for_rosbag_control():
    adapter = _FakeRosbagAdapter()
    client = _client(adapter, mission_active=True)
    headers = _headers(client)

    rejected = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "rosbag-start", "command_id": CommandId.ROSBAG_START.value},
    )
    accepted = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "rosbag-start-confirmed",
            "command_id": CommandId.ROSBAG_START.value,
            "parameters": {"hold_confirmed": True},
        },
    )

    assert rejected.json()["accepted"] is False
    assert "press-and-hold" in rejected.json()["rejection"]["message"]
    assert accepted.json()["accepted"] is True


def test_stopping_mission_owned_recording_requires_press_hold_warning():
    adapter = _FakeRosbagAdapter()
    adapter.state.update(recording=True, recording_id="mission-bag", output_dir="/bags/mission-bag", owner="mission")
    client = _client(adapter)
    headers = _headers(client)

    rejected = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "rosbag-stop", "command_id": CommandId.ROSBAG_STOP.value},
    )
    accepted = client.post(
        "/commands/actions/start",
        headers=headers,
        json={
            "request_id": "rosbag-stop-confirmed",
            "command_id": CommandId.ROSBAG_STOP.value,
            "parameters": {"hold_confirmed": True},
        },
    )

    assert rejected.json()["accepted"] is False
    assert "mission-owned" in rejected.json()["rejection"]["message"]
    assert accepted.json()["accepted"] is True


def test_inspection_recording_is_idempotent_and_rejects_critical_storage():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    controller = RosbagController(adapter=adapter)
    first = controller.ensure_inspection_recording()
    second = controller.ensure_inspection_recording()

    assert first["recording"] is True
    assert second["recording"] is True
    assert len(adapter.started) == 1
    assert adapter.started[0]["owner"] == "inspection"
    assert adapter.started[0]["all_topics"] is False
    assert "/fmu/out/vehicle_odometry" in adapter.started[0]["topics"]
    assert "/mission/status" in adapter.started[0]["topics"]
    assert "/depth_camera/points" not in adapter.started[0]["topics"]
    assert "/sensor/mmwave/points" in adapter.started[0]["topics"]
    assert "/sensor/mmwave/points_full" in adapter.started[0]["topics"]
    assert "/perception/pl_mapper/projected_points" in adapter.started[0]["topics"]
    assert "/perception/pl_mapper/points_est" in adapter.started[0]["topics"]
    assert "/perception/pl_mapper/transformed_points" in adapter.started[0]["topics"]
    assert "/sensor/cable_camera/image_raw" not in adapter.started[0]["topics"]

    adapter.state["free_space_bytes"] = 100
    try:
        controller.ensure_inspection_recording()
    except RuntimeError as exc:
        assert "critically low" in str(exc)
    else:
        raise AssertionError("critical storage must reject inspection recording")


def test_inspection_recording_waits_for_asynchronous_recorder_activation():
    from iii_drone_runtime.api.rosbag import RosbagController

    class DelayedAdapter(_FakeRosbagAdapter):
        def __init__(self):
            super().__init__()
            self.pending = False
            self.polls_after_start = 0

        def start(self, request):
            result = super().start(request)
            self.pending = True
            self.state.update(recording=False, owner="unknown")
            return result

        def status(self):
            if self.pending:
                self.polls_after_start += 1
                if self.polls_after_start >= 3:
                    self.state.update(recording=True, owner="inspection")
                    self.pending = False
            return super().status()

    adapter = DelayedAdapter()
    clock = [0.0]
    sleeps = []

    def wait(duration):
        sleeps.append(duration)
        clock[0] += duration

    controller = RosbagController(
        adapter=adapter,
        recording_start_timeout_seconds=1.0,
        recording_start_poll_interval_seconds=0.1,
        monotonic_clock=lambda: clock[0],
        sleep=wait,
    )

    status = controller.ensure_inspection_recording()

    assert status["recording"] is True
    assert status["owner"] == "inspection"
    assert sleeps == [0.1, 0.1]


def test_inspection_recording_reports_true_activation_timeout():
    from iii_drone_runtime.api.rosbag import RosbagController

    class NeverActiveAdapter(_FakeRosbagAdapter):
        def start(self, request):
            self.started.append(request)
            return {"success": True}

    adapter = NeverActiveAdapter()
    clock = [0.0]

    def wait(duration):
        clock[0] += duration

    controller = RosbagController(
        adapter=adapter,
        recording_start_timeout_seconds=0.2,
        recording_start_poll_interval_seconds=0.1,
        monotonic_clock=lambda: clock[0],
        sleep=wait,
    )

    try:
        controller.ensure_inspection_recording()
    except RuntimeError as exc:
        assert "did not become active within 0.2s" in str(exc)
    else:
        raise AssertionError("inactive recorder must time out")


def test_inspection_recording_rejects_active_manual_recording():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    adapter.state.update(recording=True, recording_id="manual", owner="manual")
    controller = RosbagController(adapter=adapter)

    try:
        controller.ensure_inspection_recording()
    except RuntimeError as exc:
        assert "manual-owned rosbag recording is active" in str(exc)
    else:
        raise AssertionError("inspection must not reuse a manual all-topic recording")

    assert adapter.started == []
    assert adapter.state["recording"] is True


def test_inspection_recording_stops_as_soon_as_mission_ownership_ends():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    controller = RosbagController(adapter=adapter)
    controller.ensure_inspection_recording()
    controller.reconcile(mission_active=True, nav_mode="mission", failsafe=False)
    controller.reconcile(mission_active=False, nav_mode="land", failsafe=False)
    assert adapter.state["recording"] is False
    assert len(adapter.stopped) == 1


def test_inspection_recording_survives_executor_transitions_and_runtime_reconnect():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    first_controller = RosbagController(adapter=adapter)
    first_controller.ensure_inspection_recording()
    first_controller.reconcile(
        mission_active=True,
        nav_mode="mission",
        failsafe=False,
        control_owner="mission",
        armed=True,
        in_air=True,
    )

    reconnected_controller = RosbagController(adapter=adapter)
    reconnected_controller.reconcile(
        mission_active=True,
        nav_mode="reach_cable",
        failsafe=False,
        control_owner="mission",
        armed=True,
        in_air=True,
    )
    reconnected_controller.reconcile(
        mission_active=True,
        nav_mode="cable_charging",
        failsafe=False,
        control_owner="mission",
        armed=True,
        in_air=True,
    )
    reconnected_controller.reconcile(
        mission_active=False,
        nav_mode="hold",
        failsafe=False,
        control_owner="px4",
        armed=True,
        in_air=True,
    )

    assert adapter.state["recording"] is False
    assert len(adapter.started) == 1
    assert len(adapter.stopped) == 1


def test_executor_initiated_landing_records_until_executor_ownership_ends():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    controller = RosbagController(adapter=adapter)
    controller.ensure_inspection_recording()
    controller.reconcile(
        mission_active=True,
        nav_mode="land",
        failsafe=False,
        control_owner="mission",
        armed=True,
        in_air=True,
    )
    assert adapter.state["recording"] is True
    assert adapter.stopped == []

    controller.reconcile(
        mission_active=False,
        nav_mode="hold",
        failsafe=False,
        control_owner="px4",
        armed=False,
        in_air=False,
    )

    assert adapter.state["recording"] is False
    assert len(adapter.stopped) == 1


def test_airborne_failsafe_stops_recording_when_executor_ownership_ends():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    controller = RosbagController(adapter=adapter)
    controller.ensure_inspection_recording()
    controller.reconcile(mission_active=True, nav_mode="mission", failsafe=False, in_air=True)
    controller.reconcile(mission_active=False, nav_mode="return", failsafe=True, in_air=True)
    assert adapter.state["recording"] is False


def test_px4_hold_stops_recording_even_when_mission_status_is_stale_active():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    controller = RosbagController(adapter=adapter)
    controller.ensure_inspection_recording()

    controller.reconcile(
        mission_active=True,
        nav_mode="hold",
        failsafe=False,
        control_owner="mission",
        armed=True,
        in_air=True,
    )

    assert adapter.state["recording"] is False
    assert len(adapter.stopped) == 1


def test_orphaned_mission_recording_is_stopped_after_runtime_restart():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    adapter.state.update(recording=True, recording_id="orphan", owner="inspection")

    RosbagController(adapter=adapter).reconcile(
        mission_active=False,
        nav_mode="hold",
        failsafe=False,
        control_owner="px4",
        armed=True,
        in_air=True,
    )

    assert adapter.state["recording"] is False
    assert len(adapter.stopped) == 1


def test_failed_activation_recording_is_stopped_after_bounded_grace():
    from iii_drone_runtime.api.rosbag import RosbagController

    now = [100.0]
    adapter = _FakeRosbagAdapter()
    controller = RosbagController(
        adapter=adapter,
        activation_grace_seconds=10.0,
        monotonic_clock=lambda: now[0],
    )
    controller.ensure_inspection_recording()

    controller.reconcile(mission_active=False, nav_mode="unknown", failsafe=False)
    assert adapter.state["recording"] is True

    now[0] = 110.0
    controller.reconcile(mission_active=False, nav_mode="unknown", failsafe=False)
    assert adapter.state["recording"] is False


def test_behavior_tree_owned_recording_is_recovered_when_mode_stops():
    from iii_drone_runtime.api.rosbag import RosbagController

    adapter = _FakeRosbagAdapter()
    adapter.state.update(recording=True, recording_id="reach", owner="reach_cable")
    controller = RosbagController(adapter=adapter)
    controller.reconcile(
        mission_active=True,
        nav_mode="reach_cable",
        failsafe=False,
        control_owner="mission",
    )
    controller.reconcile(
        mission_active=False,
        nav_mode="hold",
        failsafe=False,
        control_owner="px4",
    )

    assert adapter.state["recording"] is False
    assert len(adapter.stopped) == 1


def test_rosbag_status_failure_becomes_actionable_degraded_state():
    from iii_drone_runtime.api.rosbag import RosbagController

    class _UnavailableAdapter(_FakeRosbagAdapter):
        def status(self):
            raise RuntimeError("recorder transport lost")

    state = RosbagController(adapter=_UnavailableAdapter()).state()

    assert state.recording is False
    assert state.source_availability == "unavailable"
    assert state.freshness == "stale"
    assert "recorder transport lost" in state.recording_error
