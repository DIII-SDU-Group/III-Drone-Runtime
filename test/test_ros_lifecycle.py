import time

from iii_drone_runtime.api.events import RuntimeEventLog
from iii_drone_runtime.ros_lifecycle import RuntimeRosExecutor


class _FakeNode:
    def __init__(self):
        self.destroyed = False
        self.subscriptions = []

    def create_subscription(self, message_type, topic, callback, queue_size):
        subscription = (message_type, topic, callback, queue_size)
        self.subscriptions.append(subscription)
        return subscription

    def destroy_node(self):
        self.destroyed = True


class _FakeExecutor:
    def __init__(self):
        self.nodes = []
        self.spin_count = 0

    def add_node(self, node):
        self.nodes.append(node)

    def remove_node(self, node):
        self.nodes.remove(node)

    def spin_once(self, timeout_sec):
        del timeout_sec
        self.spin_count += 1
        time.sleep(0.001)


class _FakeExecutors:
    def __init__(self, executor):
        self._executor = executor

    def SingleThreadedExecutor(self):
        return self._executor


class _FakeRclpy:
    def __init__(self):
        self.executor = _FakeExecutor()
        self.executors = _FakeExecutors(self.executor)
        self.node = _FakeNode()
        self.init_called = False
        self.shutdown_called = False

    def init(self, args=None):
        del args
        self.init_called = True

    def create_node(self, name):
        assert name == "iii_runtime_api"
        return self.node

    def shutdown(self):
        self.shutdown_called = True


def test_ros_lifecycle_degrades_when_rclpy_unavailable():
    lifecycle = RuntimeRosExecutor(rclpy_module=None)

    status = lifecycle.start()

    assert status.available is False
    assert status.running is False
    assert status.degraded_reason == "rclpy unavailable"


def test_ros_executor_starts_and_stops_cleanly_with_fake_rclpy():
    fake = _FakeRclpy()
    lifecycle = RuntimeRosExecutor(rclpy_module=fake)

    status = lifecycle.start()
    time.sleep(0.01)

    assert status.available is True
    assert lifecycle.status().running is True
    assert fake.init_called is True
    assert fake.executor.spin_count > 0

    stopped = lifecycle.stop()

    assert stopped.running is False
    assert fake.node.destroyed is True
    assert fake.shutdown_called is True


def test_ros_executor_registers_subscriptions_before_spinning():
    fake = _FakeRclpy()
    lifecycle = RuntimeRosExecutor(rclpy_module=fake)

    status = lifecycle.start([lambda node: node.create_subscription(object, "/topic", lambda msg: msg, 10)])
    lifecycle.stop()

    assert status.available is True
    assert fake.node.subscriptions[0][1] == "/topic"
    assert fake.executor.nodes == []


def test_callback_handoff_queue_is_thread_safe_and_drainable():
    lifecycle = RuntimeRosExecutor(rclpy_module=None)

    lifecycle.enqueue_callback_update("/health", {"ready": True})
    lifecycle.enqueue_callback_update("/vehicle", {"armed": False})

    assert lifecycle.drain_updates() == [
        ("/health", {"ready": True}),
        ("/vehicle", {"armed": False}),
    ]
    assert lifecycle.drain_updates() == []


def test_availability_changes_produce_runtime_events():
    event_log = RuntimeEventLog()
    lifecycle = RuntimeRosExecutor(rclpy_module=None, event_log=event_log)

    lifecycle.record_availability_change("mission_action", False, "server missing")

    event = event_log.recent()[-1]
    assert event.category == "availability"
    assert event.details["label"] == "mission_action"
    assert event.details["available"] is False
