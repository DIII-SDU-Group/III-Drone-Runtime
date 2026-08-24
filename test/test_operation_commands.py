from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.operation_status import CustomOperationStatusCache
from iii_drone_runtime.api.px4_state import FusedPx4StateProvider
from iii_drone_runtime.ros_lifecycle import RuntimeRosExecutor
from test_px4_state import _FakeCommandAdapter, _command_status


class _FakeGoal:
    accepted = True

    def __init__(self):
        self.cancelled = False

    def cancel(self):
        self.cancelled = True
        return True


class _FakeTransport:
    def __init__(self):
        self.started = []
        self.feedback_callbacks = {}
        self.result_callbacks = {}
        self.goals = {}

    def start(self, *, operation, arguments, request_id, feedback_callback, result_callback):
        goal = _FakeGoal()
        self.started.append((operation, arguments, request_id))
        self.feedback_callbacks[request_id] = feedback_callback
        self.result_callbacks[request_id] = result_callback
        self.goals[request_id] = goal
        return goal


def _operation_status_ready():
    cache = CustomOperationStatusCache()
    cache.handle_message(
        SimpleNamespace(
            operation_state_label="ready",
            operation_active=False,
            active_operation="",
            custom_operation_modes_registered=True,
            required_modes=["CustomOperation"],
            registered_modes=["CustomOperation"],
            owned_mode="CustomOperation",
            control_owner="custom_operation",
            cancel_available=False,
            degraded=False,
            degraded_reasons=[],
        )
    )
    return cache


def _client(transport):
    return TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
                lease_timeout_seconds=60.0,
                px4_command_transport_enabled=False,
            ),
            operation_status=_operation_status_ready(),
            custom_operation_transport=transport,
            ros_executor=RuntimeRosExecutor(rclpy_module=None),
            px4_state_provider=FusedPx4StateProvider(
                command_adapter=_FakeCommandAdapter(
                    _command_status(flight_mode=None, nav_state=None),
                )
            ),
        )
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}, token


def test_runtime_operation_validate_start_status_and_cancel():
    transport = _FakeTransport()
    with _client(transport) as client:
        headers, _token = _headers(client)

        validation = client.post(
            "/commands/actions/start",
            headers=headers,
            json={
                "request_id": "validate-1",
                "command_id": CommandId.CUSTOM_OPERATION_VALIDATE.value,
                "parameters": {"operation": "hover", "arguments": {"duration_s": 2.0}},
            },
        )
        missing_hold = client.post(
            "/commands/actions/start",
            headers=headers,
            json={
                "request_id": "start-1",
                "command_id": CommandId.CUSTOM_OPERATION_HOVER_START.value,
                "parameters": {"duration_s": 2.0},
            },
        )
        started = client.post(
            "/commands/actions/start",
            headers=headers,
            json={
                "request_id": "start-2",
                "command_id": CommandId.CUSTOM_OPERATION_HOVER_START.value,
                "parameters": {"hold_confirmed": True, "duration_s": 2.0},
            },
        )
        status = client.get("/operations/status", headers=headers)
        cancel = client.post(
            "/commands/actions/start",
            headers=headers,
            json={"request_id": "cancel-1", "command_id": CommandId.CUSTOM_OPERATION_CANCEL.value},
        )
        status_after_cancel = client.get("/operations/status", headers=headers)

    assert validation.json()["result"]["validation"]["ok"] is True
    assert missing_hold.json()["accepted"] is False
    assert missing_hold.json()["rejection"]["code"] == "forbidden"
    assert started.json()["accepted"] is True
    assert started.json()["started"] is True
    assert transport.started == [("hover", {"duration_s": 2.0, "sustain_duration_s": 0.0}, "start-2")]
    assert status.json()["active_operation_type"] == "hover"
    assert status.json()["latest"]["runtime_operation"]["status"] == "running"
    assert cancel.json()["accepted"] is True
    assert cancel.json()["result"]["operation"]["status"] == "cancelled"
    assert transport.goals["start-2"].cancelled is True
    assert status_after_cancel.json()["active_operation_id"] is None
    assert status_after_cancel.json()["active_operation_type"] is None
    assert status_after_cancel.json()["latest"]["operation_active"] is False
    assert status_after_cancel.json()["latest"]["runtime_operation"]["status"] == "cancelled"


def test_runtime_operation_start_rejects_validation_reasons_without_raw_json_path():
    transport = _FakeTransport()
    with _client(transport) as client:
        headers, _token = _headers(client)
        rejected = client.post(
            "/commands/actions/start",
            headers=headers,
            json={
                "request_id": "start-invalid",
                "command_id": CommandId.CUSTOM_OPERATION_FLY_TO_POSITION_START.value,
                "parameters": {"hold_confirmed": True, "frame_id": "", "x": 0, "y": 0, "z": 1, "yaw": 0},
            },
        )

    assert rejected.json()["accepted"] is False
    assert "frame_id is required" in rejected.json()["rejection"]["message"]
    assert transport.started == []


def test_operation_feedback_and_result_emit_websocket_command_results():
    transport = _FakeTransport()
    with _client(transport) as client:
        headers, token = _headers(client)
        with client.websocket_connect(f"/ws?token={token}") as websocket:
            assert websocket.receive_json()["message_type"] == "snapshot"
            started = client.post(
                "/commands/actions/start",
                headers=headers,
                json={
                    "request_id": "start-ws",
                    "command_id": CommandId.CUSTOM_OPERATION_HOVER_START.value,
                    "parameters": {"hold_confirmed": True, "duration_s": 2.0},
                },
            )
            start_event = _next_operation_event(websocket, "started")
            transport.feedback_callbacks["start-ws"]({"progress": 0.5})
            feedback_event = _next_operation_event(websocket, "feedback")
            transport.result_callbacks["start-ws"]({"success": True})
            result_event = _next_operation_event(websocket, "result")

    assert started.json()["accepted"] is True
    assert start_event["message_type"] == "command_result"
    assert start_event["payload"]["result"]["event_type"] == "started"
    assert feedback_event["payload"]["result"]["event_type"] == "feedback"
    assert result_event["payload"]["result"]["event_type"] == "result"
    assert start_event["payload"]["request_id"] == "start-ws"
    assert feedback_event["payload"]["request_id"] == "start-ws"
    assert result_event["payload"]["request_id"] == "start-ws"


def _next_operation_event(websocket, event_type: str):
    # Periodic authoritative state patches may be interleaved with command
    # results; keep the assertion focused on the event contract, not scheduler
    # timing on a busy ROS development host.
    for _ in range(100):
        message = websocket.receive_json()
        if message["message_type"] != "command_result":
            continue
        result = message.get("payload", {}).get("result", {})
        if isinstance(result, dict) and result.get("event_type") == event_type:
            return message
    raise AssertionError(f"websocket did not emit {event_type} command_result message")
