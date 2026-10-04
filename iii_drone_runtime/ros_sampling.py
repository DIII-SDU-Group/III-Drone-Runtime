"""Fixed-rate, newest-message delivery for high-rate ROS state topics."""

from __future__ import annotations

import copy
import threading
import weakref
from typing import Any, Callable

DEFAULT_SAMPLE_RATE_HZ = 10.0

_SAMPLERS: "weakref.WeakKeyDictionary[Any, TopicSampler]" = weakref.WeakKeyDictionary()


class TopicSampler:
    """Hands high-rate state topics to their callbacks at a fixed rate.

    PX4 and payload state arrive at 50-100 Hz per topic. Delivering every
    message through the rclpy executor kept the runtime API on most of a Pi
    core and delayed its service calls, although its caches only need the
    newest value. Sampled subscriptions live on a side node that no executor
    waits on and keep only their newest message (history depth 1); a timer on
    the executor's node takes that message and passes it to the callback.
    The side node and timer are created with the first sampled subscription.
    """

    def __init__(self, rclpy_module: Any, node: Any, *, rate_hz: float = DEFAULT_SAMPLE_RATE_HZ):
        self._rclpy = rclpy_module
        self._node = node
        self._rate_hz = rate_hz
        self._lock = threading.Lock()
        self._entries: list[tuple[Any, Callable[[Any], Any]]] = []
        self._side_node: Any | None = None
        self._timer: Any | None = None

    def create_subscription(self, msg_type: Any, topic: str, callback: Callable[[Any], Any], qos: Any) -> Any:
        with self._lock:
            if self._side_node is None:
                self._side_node = self._rclpy.create_node(
                    f"{self._node.get_name()}_sampled",
                    context=self._node.context,
                    start_parameter_services=False,
                )
                self._timer = self._node.create_timer(1.0 / self._rate_hz, self.deliver_newest)
            subscription = self._side_node.create_subscription(msg_type, topic, _unused_callback, _newest_only(qos))
            self._entries.append((subscription, callback))
        return subscription

    def deliver_newest(self) -> None:
        with self._lock:
            entries = list(self._entries)
        for subscription, callback in entries:
            message = _take_newest(subscription)
            if message is not None:
                callback(message)

    def destroy(self) -> None:
        with self._lock:
            self._entries.clear()
            side_node, self._side_node = self._side_node, None
            timer, self._timer = self._timer, None
        if timer is not None:
            self._node.destroy_timer(timer)
        if side_node is not None:
            side_node.destroy_node()


def register_sampler(node: Any, sampler: TopicSampler) -> None:
    _SAMPLERS[node] = sampler


def create_sampled_subscription(
    node: Any,
    msg_type: Any,
    topic: str,
    callback: Callable[[Any], Any],
    qos: Any,
) -> Any:
    """Subscribe to a state topic whose callback only needs the newest message.

    Without a sampler registered for the node (as in unit tests), this is a
    plain subscription.
    """
    sampler = _SAMPLERS.get(node)
    if sampler is None:
        return node.create_subscription(msg_type, topic, callback, qos)
    return sampler.create_subscription(msg_type, topic, callback, qos)


def _unused_callback(message: Any) -> None:
    del message


def _newest_only(qos: Any) -> Any:
    from rclpy.qos import HistoryPolicy, QoSProfile

    profile = QoSProfile(depth=1) if isinstance(qos, int) else copy.copy(qos)
    profile.history = HistoryPolicy.KEEP_LAST
    profile.depth = 1
    return profile


def _take_newest(subscription: Any) -> Any | None:
    from rclpy.exceptions import InvalidHandle

    try:
        with subscription.handle:
            taken = subscription.handle.take_message(subscription.msg_type, subscription.raw)
    except InvalidHandle:
        return None
    return None if taken is None else taken[0]
