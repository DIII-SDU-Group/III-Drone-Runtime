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
