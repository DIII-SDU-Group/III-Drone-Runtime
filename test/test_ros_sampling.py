import os
import threading
import time

import pytest

from iii_drone_runtime.ros_sampling import TopicSampler, create_sampled_subscription, register_sampler


class _PlainNode:
    def __init__(self):
        self.created = []

    def create_subscription(self, msg_type, topic, callback, qos):
        self.created.append((msg_type, topic, callback, qos))
        return self.created[-1]


def test_without_a_sampler_the_subscription_is_plain():
    node = _PlainNode()

    def callback(message):
        del message

    assert create_sampled_subscription(node, int, "/state", callback, 10) == (int, "/state", callback, 10)


def test_sampler_delivers_only_the_newest_message_at_its_rate():
    rclpy = pytest.importorskip("rclpy")
    from rclpy.executors import SingleThreadedExecutor
    from std_msgs.msg import Int32

    context = rclpy.context.Context()
    rclpy.init(context=context, domain_id=87)
    topic = f"/sampler_test_{os.getpid()}"
    node = rclpy.create_node("sampler_test", context=context)
    publisher_node = rclpy.create_node("sampler_test_publisher", context=context)
    sampler = TopicSampler(rclpy, node, rate_hz=10.0)
    stop = threading.Event()
    publisher_thread = None
    try:
        register_sampler(node, sampler)
        received = []
        create_sampled_subscription(node, Int32, topic, lambda message: received.append(message.data), 10)
        # The sampled subscription lives on a side node the executor never waits on.
        assert list(node.subscriptions) == []

        publisher = publisher_node.create_publisher(Int32, topic, 10)
        published = [0]

        def publish():
            while not stop.is_set():
                published[0] += 1
                publisher.publish(Int32(data=published[0]))
                time.sleep(0.002)

        publisher_thread = threading.Thread(target=publish)
        publisher_thread.start()
        executor = SingleThreadedExecutor(context=context)
        executor.add_node(node)
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline:
            executor.spin_once(timeout_sec=0.05)
        stop.set()
        publisher_thread.join()

        assert 3 <= len(received) <= 16, received
        assert published[0] > 5 * len(received)
        # Each delivery is the newest message; the ones in between are dropped.
        assert all(later - earlier > 5 for earlier, later in zip(received, received[1:])), received
    finally:
        stop.set()
        if publisher_thread is not None:
            publisher_thread.join()
        sampler.destroy()
        node.destroy_node()
        publisher_node.destroy_node()
        rclpy.shutdown(context=context)
