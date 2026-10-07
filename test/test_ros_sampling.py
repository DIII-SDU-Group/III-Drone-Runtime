import os
import threading
import time

import pytest

from iii_drone_runtime.ros_sampling import (
    TopicSampler,
    create_batched_subscription,
    create_periodic,
    create_sampled_subscription,
    register_sampler,
)


class _PlainNode:
    def __init__(self):
        self.created = []

    def create_subscription(self, msg_type, topic, callback, qos):
        self.created.append((msg_type, topic, callback, qos))
        return self.created[-1]

    def create_timer(self, period, callback):
        self.created.append(("timer", period, callback))
        return self.created[-1]


def test_without_a_sampler_the_subscription_is_plain():
    node = _PlainNode()

    def callback(message=None):
        del message

    assert create_sampled_subscription(node, int, "/state", callback, 10) == (int, "/state", callback, 10)
    assert create_batched_subscription(node, int, "/tf", callback, 100) == (int, "/tf", callback, 100)
    assert create_periodic(node, 1.0, callback) == ("timer", 1.0, callback)


@pytest.fixture
def ros():
    rclpy = pytest.importorskip("rclpy")
    context = rclpy.context.Context()
    rclpy.init(context=context, domain_id=87)
    nodes = []
    samplers = []

    def node(name):
        created = rclpy.create_node(f"{name}_{os.getpid()}", context=context)
        nodes.append(created)
        return created

    def sampler(owner, rate_hz=10.0):
        created = TopicSampler(rclpy, owner, rate_hz=rate_hz)
        register_sampler(owner, created)
        samplers.append(created)
        return created

    yield rclpy, node, sampler
    for created in samplers:
        created.destroy()
    for created in nodes:
        created.destroy_node()
    rclpy.shutdown(context=context)


def _wait_for_match(publisher, timeout=5.0):
    deadline = time.monotonic() + timeout
    while publisher.get_subscription_count() == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert publisher.get_subscription_count() > 0


def test_sampler_delivers_only_the_newest_message_at_its_rate(ros):
    rclpy, node, sampler = ros
    from rclpy.qos import ReliabilityPolicy
    from std_msgs.msg import Int32

    owner = node("sampler_test")
    publisher_node = node("sampler_test_publisher")
    sampler(owner)
    topic = f"/sampler_test_{os.getpid()}"
    published = [0]
    received = []
    lags = []
    threads = set()

    def callback(message):
        received.append(message.data)
        # How many messages newer than this one had been published.
        lags.append(published[0] - message.data)
        threads.add(threading.current_thread().name)

    subscription = create_sampled_subscription(owner, Int32, topic, callback, 10)
    # The subscription lives on a side node no executor waits on, and the
    # delivery needs no executor or timer on the runtime node.
    assert list(owner.subscriptions) == [] and list(owner.timers) == []
    # Newest-only state is read best-effort.
    assert subscription.qos_profile.reliability == ReliabilityPolicy.BEST_EFFORT

    publisher = publisher_node.create_publisher(Int32, topic, 10)
    _wait_for_match(publisher)
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        published[0] += 1
        publisher.publish(Int32(data=published[0]))
        time.sleep(0.002)
    time.sleep(0.2)

    # About one delivery per 0.1 s tick, each the newest message: the ones
    # published in between (about 40 per tick) are dropped.
    assert 3 <= len(received) <= 18, received
    assert published[0] > 5 * len(received)
    assert received == sorted(set(received)), received
    assert max(lags) <= 20, lags
    assert threads == {"iii-runtime-api-delivery"}


def test_batched_subscription_delivers_every_queued_message_in_order(ros):
    rclpy, node, sampler = ros
    from std_msgs.msg import Int32

    owner = node("batched_test")
    publisher_node = node("batched_test_publisher")
    sampler(owner)
    topic = f"/batched_test_{os.getpid()}"
    received = []
    deliveries = []

    def callback(message):
        received.append(message.data)
        deliveries.append(time.monotonic())

    create_batched_subscription(owner, Int32, topic, callback, 100)
    assert list(owner.subscriptions) == []
    publisher = publisher_node.create_publisher(Int32, topic, 100)
    _wait_for_match(publisher)
    for value in range(1, 61):
        publisher.publish(Int32(data=value))
        time.sleep(0.002)
    deadline = time.monotonic() + 2.0
    while len(received) < 60 and time.monotonic() < deadline:
        time.sleep(0.05)

    assert received == list(range(1, 61))
    # Handed over in a few delivery ticks, not one wakeup per message.
    ticks = 1 + sum(1 for earlier, later in zip(deliveries, deliveries[1:]) if later - earlier > 0.05)
    assert ticks <= 4, ticks


def test_subscribers_of_one_topic_share_one_reader(ros):
    rclpy, node, sampler = ros
    from std_msgs.msg import Int32

    owner = node("shared_test")
    publisher_node = node("shared_test_publisher")
    topic_sampler = sampler(owner)
    topic = f"/shared_test_{os.getpid()}"
    first, second = [], []

    one = create_sampled_subscription(owner, Int32, topic, lambda message: first.append(message.data), 10)
    two = create_sampled_subscription(owner, Int32, topic, lambda message: second.append(message.data), 10)
    batched = create_batched_subscription(owner, Int32, topic, lambda message: None, 10)

    assert one is two
    assert batched is not one
    assert len(list(topic_sampler._side_node.subscriptions)) == 2

    publisher = publisher_node.create_publisher(Int32, topic, 10)
    _wait_for_match(publisher)
    publisher.publish(Int32(data=7))
    deadline = time.monotonic() + 2.0
    while not (first and second) and time.monotonic() < deadline:
        time.sleep(0.05)

    assert first == second == [7]


def test_periodic_work_runs_on_the_delivery_thread_without_a_timer(ros):
    rclpy, node, sampler = ros

    owner = node("periodic_test")
    sampler(owner, rate_hz=20.0)
    calls = []

    create_periodic(owner, 0.2, lambda: calls.append(threading.current_thread().name))
    time.sleep(1.1)

    assert list(owner.timers) == []
    assert 4 <= len(calls) <= 6, calls
    assert set(calls) == {"iii-runtime-api-delivery"}


def test_a_failing_callback_does_not_stop_delivery(ros):
    rclpy, node, sampler = ros
    from std_msgs.msg import Int32

    owner = node("failing_test")
    publisher_node = node("failing_test_publisher")
    sampler(owner)
    topic = f"/failing_test_{os.getpid()}"
    received = []
    periodic_calls = []

    def broken(message):
        raise RuntimeError(f"broken callback {message.data}")

    def broken_periodic():
        periodic_calls.append(time.monotonic())
        raise RuntimeError("broken periodic")

    create_batched_subscription(owner, Int32, topic, broken, 10)
    create_batched_subscription(owner, Int32, topic, lambda message: received.append(message.data), 10)
    create_periodic(owner, 0.1, broken_periodic)
    publisher = publisher_node.create_publisher(Int32, topic, 10)
    _wait_for_match(publisher)
    for value in (1, 2, 3):
        publisher.publish(Int32(data=value))
        time.sleep(0.15)
    deadline = time.monotonic() + 2.0
    while len(received) < 3 and time.monotonic() < deadline:
        time.sleep(0.05)

    assert received == [1, 2, 3]
    assert len(periodic_calls) >= 3


def test_a_lower_rate_drops_messages_that_arrive_within_the_period():
    from iii_drone_runtime.ros_sampling import at_most

    now = [0.0]
    seen = []
    limited = at_most(2.0, seen.append, clock=lambda: now[0])

    for stamp, message in ((0.0, 1), (0.1, 2), (0.2, 3), (0.5, 4), (0.6, 5), (1.0, 6)):
        now[0] = stamp
        limited(message)

    assert seen == [1, 4, 6]
