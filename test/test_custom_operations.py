from iii_drone_runtime.api.custom_operations import (
    NonblockingCustomOperationClient,
    OperationReadinessContext,
    RosCustomOperationTransport,
    _message_to_dict,
    validate_operation_request,
)


def test_ros_slot_message_feedback_serializes_without_vars():
    class Feedback:
        __slots__ = ("operation", "state", "feedback_json")

        def __init__(self):
            self.operation = "fly_to_position"
            self.state = "running"
            self.feedback_json = "{}"

        @staticmethod
        def get_fields_and_field_types():
            return {"operation": "string", "state": "string", "feedback_json": "string"}

    assert _message_to_dict(Feedback()) == {
        "operation": "fly_to_position",
        "state": "running",
        "feedback_json": "{}",
    }


def test_ros_action_transport_uses_runtime_reentrant_callback_group(monkeypatch):
    import sys
    from types import ModuleType

    captured = {}

    class ActionClient:
        def __init__(self, node, action_type, name, *, callback_group):
            captured.update(
                node=node,
                action_type=action_type,
                name=name,
                callback_group=callback_group,
            )

        def wait_for_server(self, *, timeout_sec):
            return False

    action_module = ModuleType("rclpy.action")
    action_module.ActionClient = ActionClient
    monkeypatch.setitem(sys.modules, "rclpy.action", action_module)
    interfaces_module = ModuleType("iii_drone_interfaces")
    interfaces_action_module = ModuleType("iii_drone_interfaces.action")
    interfaces_action_module.CustomOperation = type("CustomOperation", (), {})
    interfaces_module.action = interfaces_action_module
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces", interfaces_module)
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces.action", interfaces_action_module)
    monkeypatch.setattr(
        "iii_drone_runtime.api.custom_operations.runtime_reentrant_callback_group",
        lambda node: "runtime-reentrant-group",
    )
    node = object()
    transport = RosCustomOperationTransport(node=node)

    goal = transport.start(
        operation="hover",
        arguments={"duration_s": 1.0},
        request_id="request",
        feedback_callback=lambda _feedback: None,
        result_callback=lambda _result: None,
    )

    assert goal.accepted is False
    assert captured["node"] is node
    assert captured["name"] == "/mission/custom_operation/run_operation"
    assert captured["callback_group"] == "runtime-reentrant-group"


def test_ros_action_transport_waits_for_goal_acceptance_and_forwards_result(monkeypatch):
    import sys
    from types import ModuleType, SimpleNamespace

    class Future:
        def __init__(self, result):
            self._result = result

        def add_done_callback(self, callback):
            callback(self)

        def result(self):
            return self._result

    class GoalHandle:
        accepted = True

        def __init__(self):
            self.cancelled = False

        def get_result_async(self):
            result = SimpleNamespace(success=True, error="")
            return Future(SimpleNamespace(status=4, result=result))

        def cancel_goal_async(self):
            self.cancelled = True

    goal_handle = GoalHandle()

    class ActionClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def wait_for_server(self, *, timeout_sec):
            return True

        def send_goal_async(self, _goal, *, feedback_callback):
            feedback_callback(SimpleNamespace(feedback={"progress": 0.5}))
            return Future(goal_handle)

    action_module = ModuleType("rclpy.action")
    action_module.ActionClient = ActionClient
    monkeypatch.setitem(sys.modules, "rclpy.action", action_module)
    action_msgs_module = ModuleType("action_msgs")
    action_msgs_msg_module = ModuleType("action_msgs.msg")
    action_msgs_msg_module.GoalStatus = SimpleNamespace(STATUS_SUCCEEDED=4, STATUS_CANCELED=5)
    action_msgs_module.msg = action_msgs_msg_module
    monkeypatch.setitem(sys.modules, "action_msgs", action_msgs_module)
    monkeypatch.setitem(sys.modules, "action_msgs.msg", action_msgs_msg_module)
    interfaces_module = ModuleType("iii_drone_interfaces")
    interfaces_action_module = ModuleType("iii_drone_interfaces.action")

    class Goal:
        pass

    interfaces_action_module.CustomOperation = SimpleNamespace(Goal=Goal)
    interfaces_module.action = interfaces_action_module
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces", interfaces_module)
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces.action", interfaces_action_module)
    monkeypatch.setattr(
        "iii_drone_runtime.api.custom_operations.runtime_reentrant_callback_group",
        lambda _node: "group",
    )
    feedback = []
    results = []
    transport = RosCustomOperationTransport(node=object())

    goal = transport.start(
        operation="hover",
        arguments={"duration_s": 1.0},
        request_id="request",
        feedback_callback=feedback.append,
        result_callback=results.append,
    )

    assert goal.accepted is True
    assert feedback == [{"progress": 0.5}]
    assert results == [{"success": True, "cancelled": False, "status": 4, "error": ""}]
    assert goal._result_future is not None
    assert goal.cancel() is True
    assert goal_handle.cancelled is True


def test_ros_action_transport_rejects_goal_response_timeout_and_cancels_late_acceptance(monkeypatch):
    import sys
    from types import ModuleType, SimpleNamespace

    callbacks = []

    class Future:
        def add_done_callback(self, callback):
            callbacks.append(callback)

        def result(self):
            return goal_handle

    class GoalHandle:
        accepted = True

        def __init__(self):
            self.cancelled = False

        def cancel_goal_async(self):
            self.cancelled = True

    goal_handle = GoalHandle()

    class ActionClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def wait_for_server(self, *, timeout_sec):
            return True

        def send_goal_async(self, _goal, *, feedback_callback):
            del feedback_callback
            return Future()

    action_module = ModuleType("rclpy.action")
    action_module.ActionClient = ActionClient
    monkeypatch.setitem(sys.modules, "rclpy.action", action_module)
    action_msgs_module = ModuleType("action_msgs")
    action_msgs_msg_module = ModuleType("action_msgs.msg")
    action_msgs_msg_module.GoalStatus = SimpleNamespace(STATUS_SUCCEEDED=4, STATUS_CANCELED=5)
    action_msgs_module.msg = action_msgs_msg_module
    monkeypatch.setitem(sys.modules, "action_msgs", action_msgs_module)
    monkeypatch.setitem(sys.modules, "action_msgs.msg", action_msgs_msg_module)
    interfaces_module = ModuleType("iii_drone_interfaces")
    interfaces_action_module = ModuleType("iii_drone_interfaces.action")

    class Goal:
        pass

    interfaces_action_module.CustomOperation = SimpleNamespace(Goal=Goal)
    interfaces_module.action = interfaces_action_module
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces", interfaces_module)
    monkeypatch.setitem(sys.modules, "iii_drone_interfaces.action", interfaces_action_module)
    monkeypatch.setattr(
        "iii_drone_runtime.api.custom_operations.runtime_reentrant_callback_group",
        lambda _node: "group",
    )
    transport = RosCustomOperationTransport(node=object(), goal_response_timeout_s=0.001)

    goal = transport.start(
        operation="hover",
        arguments={"duration_s": 1.0},
        request_id="request",
        feedback_callback=lambda _feedback: None,
        result_callback=lambda _result: None,
    )

    assert goal.accepted is False
    assert goal.reason == "timed out waiting for hover goal response"
    assert len(callbacks) == 2
    callbacks[1](Future())
    assert goal_handle.cancelled is True


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


def test_immediate_transport_result_is_not_overwritten_as_running():
    class ImmediateTransport:
        def start(self, *, operation, arguments, request_id, feedback_callback, result_callback):
            del operation, arguments, request_id, feedback_callback
            result_callback({"success": True})
            return _FakeGoal()

    client = NonblockingCustomOperationClient(
        transport=ImmediateTransport(),
        readiness_provider=_ready_context,
    )

    record = client.start("hover", {"duration_s": 1.0}, request_id="immediate")

    assert record.status == "succeeded"
    assert record.terminal is True


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


def test_ros_transport_fails_closed_until_runtime_ros_node_exists():
    transport = RosCustomOperationTransport(node_provider=lambda: None)

    goal = transport.start(
        operation="hover",
        arguments={"duration_s": 1.0},
        request_id="missing-node",
        feedback_callback=lambda _feedback: None,
        result_callback=lambda _result: None,
    )

    assert goal.accepted is False
    assert goal.reason == "runtime ROS node is unavailable for CustomOperation"
