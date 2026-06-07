from iii_drone_runtime.api.custom_operations import (
    NonblockingCustomOperationClient,
    OperationReadinessContext,
    validate_operation_request,
)


class _FakeGoal:
    accepted = True

    def __init__(self):
        self.cancelled = False

    def cancel(self):
        self.cancelled = True
        return True


class _FakeTransport:
    def __init__(self, *, accepted=True):
        self.accepted = accepted
        self.started = []
        self.feedback_callbacks = {}
        self.result_callbacks = {}
        self.goals = {}

    def start(self, *, operation, arguments, request_id, feedback_callback, result_callback):
        self.started.append((operation, arguments, request_id))
        if not self.accepted:
            goal = _FakeGoal()
            goal.accepted = False
            goal.reason = "transport rejected"
            return goal
        goal = _FakeGoal()
        self.goals[request_id] = goal
        self.feedback_callbacks[request_id] = feedback_callback
        self.result_callbacks[request_id] = result_callback
        return goal


def _ready_context(**overrides):
    values = {
        "custom_operation_mode_registered": True,
        "custom_operation_mode_active": True,
        "active_operation_id": None,
        "available_frames": {"map", "drone"},
        "available_target_ids": {7, 9},
        "available_cable_ids": {1, 2},
    }
    values.update(overrides)
    return OperationReadinessContext(**values)


def test_validate_only_checks_mode_active_operation_frames_targets_and_ranges():
    ok, reasons = validate_operation_request(
        "fly_to_position",
        {"frame_id": "map", "x": "1", "y": 2, "z": 3, "yaw": 0},
        context=_ready_context(),
    )
    assert reasons == []
    assert ok["x"] == 1.0

    invalid, reasons = validate_operation_request(
        "hover_on_cable",
        {"target_cable_id": 99, "duration_s": -1.0},
        context=_ready_context(
            custom_operation_mode_active=False,
            active_operation_id="active-1",
        ),
    )

    assert invalid["target_cable_id"] == 99
    assert "CustomOperation mode is not active" in reasons
    assert "operation already active: active-1" in reasons
    assert "target cable is unavailable: 99" in reasons
    assert "duration_s must be greater than zero" in reasons


def test_nonblocking_start_returns_after_goal_acceptance_and_streams_feedback_and_result():
    events = []
    transport = _FakeTransport()
    client = NonblockingCustomOperationClient(
        transport=transport,
        readiness_provider=_ready_context,
        event_sink=events.append,
    )

    record = client.start(
        "hover",
        {"duration_s": 2.0},
        request_id="req-hover",
    )

    assert record.accepted is True
    assert record.status == "running"
    assert transport.started == [("hover", {"duration_s": 2.0, "sustain_duration_s": 0.0}, "req-hover")]
    assert events[-1].event_type == "started"

    transport.feedback_callbacks["req-hover"]({"progress": 0.5})
    status = client.status(record.operation_id)

    assert status.feedback_count == 1
    assert status.last_feedback == {"progress": 0.5}
    assert events[-1].event_type == "feedback"

    transport.result_callbacks["req-hover"]({"success": True, "summary": "done"})
    result = client.result(record.operation_id)

    assert result.status == "succeeded"
    assert result.result == {"success": True, "summary": "done"}
    assert events[-1].event_type == "result"


def test_one_active_operation_at_a_time_and_rejection_reasons_surface():
    transport = _FakeTransport()
    client = NonblockingCustomOperationClient(transport=transport, readiness_provider=_ready_context)

    active = client.start("hover", {"duration_s": 2.0}, request_id="req-1")
    rejected = client.start("fly_to_position", {"frame_id": "map", "x": 0, "y": 0, "z": 0, "yaw": 0}, request_id="req-2")

    assert active.status == "running"
    assert rejected.status == "rejected"
    assert rejected.accepted is False
    assert rejected.rejection_reasons == [f"operation already active: {active.operation_id}"]
    assert len(transport.started) == 1


def test_cancel_active_has_nonblocking_cancel_semantics():
    transport = _FakeTransport()
    client = NonblockingCustomOperationClient(transport=transport, readiness_provider=_ready_context)
    active = client.start("hover", {"duration_s": 2.0}, request_id="req-cancel")

    cancelled = client.cancel_active()

    assert cancelled.operation_id == active.operation_id
    assert cancelled.status == "cancelled"
    assert transport.goals["req-cancel"].cancelled is True


def test_all_supported_operation_helpers_validate_with_expected_arguments():
    operation_arguments = {
        "fly_to_position": {"frame_id": "map", "x": 0, "y": 0, "z": 1, "yaw": 0},
        "cable_aware_fly_to_position": {"frame_id": "map", "x": 0, "y": 0, "z": 1, "yaw": 0},
        "fly_to_object": {"target_id": 7},
        "cable_landing": {"target_cable_id": 1},
        "cable_takeoff": {"target_cable_id": 1, "target_cable_distance": 0.5},
        "hover": {"duration_s": 1.0},
        "hover_by_object": {"target_id": 7, "duration_s": 1.0},
        "hover_on_cable": {"target_cable_id": 1, "duration_s": 1.0},
    }

    for operation, arguments in operation_arguments.items():
        validation = validate_operation_request(operation, arguments, context=_ready_context())
        assert validation[1] == [], operation
