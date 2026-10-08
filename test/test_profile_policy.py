"""opti_track flies flight basics: payload, perception, overview, cable-intent
and cable/target custom-operation commands are rejected with
PROFILE_RESTRICTED before any handler runs; other profiles are unchanged."""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId, CommandRequest
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.profile_policy import (
    FLIGHT_BASICS_COMMANDS,
    FLIGHT_BASICS_CUSTOM_OPERATIONS,
    RuntimeProfilePolicy,
)
from test_simulation_controls import _FakeSimulationTools, _client as _simulation_client


class _Recorder:
    """Fake ROS service adapters that record every call they receive."""

    def __init__(self):
        self.calls = []

    def command(self, *args, **kwargs):
        self.calls.append(("command", args, kwargs))
        return {"success": True}

    def update(self, **kwargs):
        self.calls.append(("update", kwargs))
        return {"success": True}

    def capture_current(self, **kwargs):
        self.calls.append(("capture_current", kwargs))
        return {"success": True}

    def clear(self):
        self.calls.append(("clear",))
        return {"success": True}

    def set_intent(self, service_name, value):
        self.calls.append(("set_intent", service_name, value))
        return {"success": True, "message": "", "service_name": service_name, "value": value}


class _OperationTransport:
    def __init__(self):
        self.started = []

    def start(self, *, operation, arguments, request_id, feedback_callback, result_callback):
        del arguments, request_id, feedback_callback, result_callback
        self.started.append(operation)
        return SimpleNamespace(accepted=True, cancel=lambda: True)


def _app(profile, recorder=None, transport=None):
    recorder = recorder or _Recorder()
    return create_app(
        settings=RuntimeApiSettings(runtime_id="iii-runtime", runtime_name="III Runtime", profile=profile),
        gripper_service=recorder,
        pl_mapper_service=recorder,
        powerline_overview_service=recorder,
        pylon_overview_service=recorder,
        mission_intent_service=recorder,
        custom_operation_transport=transport or _OperationTransport(),
    )


def _start(client, command_id, parameters=None, request_id=None):
    return client.post(
        "/commands/actions/start",
        json={
            "request_id": request_id or f"req-{command_id}",
            "command_id": command_id,
            "parameters": parameters or {},
        },
    ).json()


RESTRICTED = [
    (CommandId.PAYLOAD_GRIPPER_OPEN.value, {}, "payload control"),
    (CommandId.PAYLOAD_GRIPPER_CLOSE.value, {}, "payload control"),
    (CommandId.PERCEPTION_PL_MAPPER_START.value, {}, "powerline perception"),
    (CommandId.PERCEPTION_PL_MAPPER_STOP.value, {}, "powerline perception"),
    (CommandId.PERCEPTION_PL_MAPPER_FREEZE.value, {}, "powerline perception"),
    (CommandId.PERCEPTION_PL_MAPPER_PAUSE.value, {}, "powerline perception"),
    (CommandId.POWERLINE_OVERVIEW_UPDATE.value, {"timeout_s": 5}, "overview capture"),
    (CommandId.PYLON_CAPTURE_CURRENT.value, {"pylon_id": 1}, "overview capture"),
    (CommandId.PYLON_OVERVIEW_CLEAR.value, {}, "overview capture"),
    (CommandId.MISSION_RECHARGE_NOW.value, {"value": True}, "cable intent mission.recharge_now"),
    (CommandId.MISSION_STAY_ON_CABLE.value, {"value": True}, "cable intent mission.stay_on_cable"),
    (CommandId.MISSION_LEAVE_CABLE_NOW.value, {"value": True}, "cable intent mission.leave_cable_now"),
    *(
        (command_id, {"hold_confirmed": True, "arguments": {}}, f"custom operation {operation}")
        for command_id, operation in (
            (CommandId.CUSTOM_OPERATION_CABLE_AWARE_FLY_TO_POSITION_START.value, "cable_aware_fly_to_position"),
            (CommandId.CUSTOM_OPERATION_FLY_TO_OBJECT_START.value, "fly_to_object"),
            (CommandId.CUSTOM_OPERATION_HOVER_BY_OBJECT_START.value, "hover_by_object"),
            (CommandId.CUSTOM_OPERATION_HOVER_ON_CABLE_START.value, "hover_on_cable"),
            (CommandId.CUSTOM_OPERATION_CABLE_LANDING_START.value, "cable_landing"),
            (CommandId.CUSTOM_OPERATION_CABLE_TAKEOFF_START.value, "cable_takeoff"),
        )
    ),
    (
        CommandId.CUSTOM_OPERATION_VALIDATE.value,
        {"operation": "cable_landing", "arguments": {"target_cable_id": 0}},
        "custom operation cable_landing",
    ),
]


@pytest.mark.parametrize(("command_id", "parameters", "thing"), RESTRICTED)
def test_opti_track_rejects_unavailable_commands_before_their_handler(command_id, parameters, thing):
    recorder = _Recorder()
    transport = _OperationTransport()
    client = TestClient(_app("opti_track", recorder, transport))

    response = _start(client, command_id, parameters)

    assert response["accepted"] is False
    assert response["rejection"]["code"] == "profile_restricted"
    assert response["rejection"]["retryable"] is False
    assert response["message"] == f"{thing} is not available in the opti_track profile"
    assert response["rejection"]["message"] == response["message"]
    assert recorder.calls == []
    assert transport.started == []


def test_opti_track_rejection_is_logged_and_replayed_for_the_same_request():
    client = TestClient(_app("opti_track"))

    first = _start(client, CommandId.PAYLOAD_GRIPPER_OPEN.value, request_id="gripper-1")
    second = _start(client, CommandId.PAYLOAD_GRIPPER_OPEN.value, request_id="gripper-1")

    assert first == second
    decisions = [
        event
        for event in client.get("/events/recent").json()
        if event["category"] == "command_decision" and event["request_id"] == "gripper-1"
    ]
    assert decisions[-1]["details"]["accepted"] is False
    assert "not available in the opti_track profile" in decisions[-1]["message"]


def test_cli_commands_pass_the_same_profile_allowlist():
    client = TestClient(_app("opti_track"))

    response = client.post(
        "/cli/commands",
        json={"request_id": "cli-1", "command_id": CommandId.PERCEPTION_PL_MAPPER_START.value},
    ).json()

    assert response["accepted"] is False
    assert response["rejection"]["code"] == "profile_restricted"


def test_opti_track_still_dispatches_its_flight_basics_commands():
    recorder = _Recorder()
    client = TestClient(_app("opti_track", recorder))

    validate = _start(
        client,
        CommandId.CUSTOM_OPERATION_VALIDATE.value,
        {"operation": "hover", "arguments": {"duration_s": 5}},
    )
    hover = _start(client, CommandId.CUSTOM_OPERATION_HOVER_START.value, {"arguments": {"duration_s": 5}})
    fly = _start(
        client,
        CommandId.CUSTOM_OPERATION_FLY_TO_POSITION_START.value,
        {"arguments": {"frame_id": "map", "x": 0, "y": 0, "z": 1, "yaw": 0}},
    )
    rosbags = _start(client, CommandId.ROSBAG_LIST.value)

    assert validate["accepted"] is True
    assert validate["result"]["validation"]["operation"] == "hover"
    # The operation handlers ran and applied their own rules.
    assert hover["rejection"]["code"] == "forbidden"
    assert "press-and-hold" in hover["message"]
    assert fly["rejection"]["code"] == "forbidden"
    assert rosbags["accepted"] is True


@pytest.mark.parametrize("profile", ["real", "hil", "sim"])
def test_other_profiles_keep_payload_perception_and_cable_commands(profile):
    recorder = _Recorder()
    client = TestClient(_app(profile, recorder))

    gripper = _start(client, CommandId.PAYLOAD_GRIPPER_CLOSE.value)
    mapper = _start(client, CommandId.PERCEPTION_PL_MAPPER_START.value)
    intent = _start(client, CommandId.MISSION_LEAVE_CABLE_NOW.value, {"value": True})
    validate = _start(
        client,
        CommandId.CUSTOM_OPERATION_VALIDATE.value,
        {"operation": "cable_landing", "arguments": {"target_cable_id": 0}},
    )

    assert gripper["accepted"] is True
    assert mapper["accepted"] is True
    assert ("command", ("close",), {}) in recorder.calls
    # The intent reaches its handler, which judges the mission phase.
    assert intent["rejection"]["code"] != "profile_restricted"
    assert validate["accepted"] is True


def test_identity_and_system_state_advertise_the_profile_capabilities():
    opti_track = TestClient(_app("opti_track"))
    sim = TestClient(_app("sim"))

    restricted = opti_track.get("/identity").json()["capabilities"]
    system = opti_track.get("/system/health").json()["capabilities"]
    unrestricted = sim.get("/identity").json()["capabilities"]

    assert restricted == system
    assert restricted["profile"] == "opti_track"
    assert restricted["payload_available"] is False
    assert restricted["perception_available"] is False
    assert restricted["overviews_available"] is False
    assert restricted["cable_intents_available"] is False
    assert restricted["simulation_available"] is False
    # follow_waypoint_path is allowed but this runtime cannot start it yet.
    assert restricted["custom_operations"] == ["fly_to_position", "hover"]
    assert restricted["disarmed_mission_activation"] is True
    assert unrestricted["payload_available"] is True
    assert unrestricted["cable_intents_available"] is True
    assert unrestricted["simulation_available"] is True
    assert unrestricted["custom_operations"] is None
    assert unrestricted["disarmed_mission_activation"] is False
    assert TestClient(_app("real")).get("/identity").json()["capabilities"]["simulation_available"] is False


def test_opti_track_payload_and_perception_permissions_show_the_restriction():
    client = TestClient(_app("opti_track"))

    payload = client.get("/payload/status").json()["latest"]["permissions"]
    perception = client.get("/perception/status").json()["latest"]["permissions"]

    assert payload["gripper_commands_allowed"] is False
    assert payload["gripper_command_rejections"] == ["payload control is not available in the opti_track profile"]
    assert perception["mutating_commands_allowed"] is False
    assert perception["mutation_rejections"] == ["powerline perception is not available in the opti_track profile"]


def test_opti_track_simulation_controls_stay_disabled_without_calling_tools():
    tools = _FakeSimulationTools()
    client = _simulation_client("opti_track", tools)

    start = client.post("/simulation/backend/start").json()

    assert start["result"]["ok"] is False
    assert start["result"]["disabled_reason"]
    assert tools.calls == []


def test_flight_basics_allowlist_and_capabilities():
    policy = RuntimeProfilePolicy("opti_track")

    def request(command_id, **parameters):
        return CommandRequest(request_id="r", command_id=command_id, parameters=parameters)

    assert policy.flight_basics is True
    assert FLIGHT_BASICS_CUSTOM_OPERATIONS == {"fly_to_position", "follow_waypoint_path", "hover"}
    assert CommandId.CUSTOM_OPERATION_HOVER_START.value in FLIGHT_BASICS_COMMANDS
    assert CommandId.CUSTOM_OPERATION_CABLE_LANDING_START.value not in FLIGHT_BASICS_COMMANDS
    assert policy.command_rejection(request(CommandId.RUNTIME_STOP.value)) is None
    assert CommandId.MISSION_PROCEED.value in FLIGHT_BASICS_COMMANDS
    assert policy.command_rejection(request(CommandId.MISSION_PROCEED.value)) is None
    assert policy.command_rejection(request(CommandId.CUSTOM_OPERATION_VALIDATE.value, operation="follow_waypoint_path")) is None
    # A command added later is not available until it is allowlisted.
    assert policy.command_rejection(request("payload.winch.lower")) == (
        "payload control is not available in the opti_track profile"
    )
    assert policy.command_rejection(request("future.command")) == (
        "command future.command is not available in the opti_track profile"
    )
    assert policy.rejection(request(CommandId.PX4_ARM.value)) is None
    for profile in ("real", "hil", "sim", None):
        unrestricted = RuntimeProfilePolicy(profile)
        assert unrestricted.command_rejection(request(CommandId.PAYLOAD_GRIPPER_OPEN.value)) is None
        assert unrestricted.restriction("payload control") is None
        assert unrestricted.capabilities().custom_operations is None

