"""Topic delivery and periodic work for the runtime API, off the rclpy executor."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
import logging
import threading
import time
import weakref
from typing import Any, Callable

DEFAULT_SAMPLE_RATE_HZ = 10.0
CALLBACK_ERROR_LOG_PERIOD_SECONDS = 60.0

_LOGGER = logging.getLogger("iii_drone_runtime.ros_sampling")
_SAMPLERS: "weakref.WeakKeyDictionary[Any, TopicSampler]" = weakref.WeakKeyDictionary()


@dataclass
class _Reader:
    subscription: Any
    per_tick: int  # messages taken per tick: 1 = newest only
    callbacks: list[Callable[[Any], Any]] = field(default_factory=list)


@dataclass
class Periodic:
    period_seconds: float
    callback: Callable[[], Any]
    next_due: float
    cancelled: bool = False

    def cancel(self) -> None:
        self.cancelled = True


class TopicSampler:
    """Delivers the runtime API's topics and periodic work on one thread.

    Every callback through the rclpy executor cost a Python wait-set rebuild
    (about 0.6 ms on the Pi, mostly the QoS event handlers) plus a thread-pool
    hand-off and two guard-condition wakeups: at 172 executor wakeups per
    second that was a third of the runtime API's CPU. Subscriptions therefore
    live on a side node that no executor waits on, and one delivery thread
    takes their messages at a fixed rate and hands them to the callbacks in
    order:

    - newest-only subscriptions (state topics at 50-100 Hz) keep one message
      (history depth 1). They are read best-effort unless they are latched: a
      newer sample replaces a lost one within milliseconds, and best-effort
      spares both sides the reliable protocol's acknowledgements;
    - batched subscriptions keep their queue (the QoS depth) and hand over
      every queued message in order (status topics, the TF buffer).

    Periodic callbacks (graph checks, slow refreshes) run on the same thread,
    so the executor only wakes for service and action responses. Subscribers
    of the same topic, type and profile share one DDS reader. A callback that
    raises is logged and does not stop delivery.
    """

    def __init__(self, rclpy_module: Any, node: Any, *, rate_hz: float = DEFAULT_SAMPLE_RATE_HZ):
        self._rclpy = rclpy_module
        self._node = node
        self._period = 1.0 / rate_hz
        self._lock = threading.Lock()
        self._readers: dict[tuple, _Reader] = {}
        self._periodics: list[Periodic] = []
        self._side_node: Any | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._error_logged_at: dict[int, float] = {}

    def create_subscription(
        self,
        msg_type: Any,
        topic: str,
        callback: Callable[[Any], Any],
        qos: Any,
        *,
        batched: bool = False,
        rate_hz: float | None = None,
    ) -> Any:
        profile = _queued(qos) if batched else _newest_only(qos)
        if rate_hz is not None:
            callback = at_most(rate_hz, callback)
        key = (topic, msg_type, batched, _profile_key(profile))
        with self._lock:
            reader = self._readers.get(key)
            if reader is None:
                if self._side_node is None:
                    self._side_node = self._rclpy.create_node(
                        f"{self._node.get_name()}_sampled",
                        context=self._node.context,
                        start_parameter_services=False,
                    )
                subscription = self._side_node.create_subscription(msg_type, topic, _unused_callback, profile)
                reader = _Reader(subscription=subscription, per_tick=max(1, profile.depth))
                self._readers[key] = reader
            reader.callbacks.append(callback)
            self._ensure_started_locked()
        return reader.subscription

    def create_periodic(self, period_seconds: float, callback: Callable[[], Any]) -> Periodic:
        periodic = Periodic(period_seconds, callback, next_due=time.monotonic() + period_seconds)
        with self._lock:
            self._periodics.append(periodic)
            self._ensure_started_locked()
        return periodic

    def deliver(self) -> None:
        with self._lock:
            readers = [(reader.subscription, reader.per_tick, list(reader.callbacks)) for reader in self._readers.values()]
        for subscription, per_tick, callbacks in readers:
            for _ in range(per_tick):
                message = self._take(subscription)
                if message is None:
                    break
                for callback in callbacks:
                    self._call(callback, message)

    def run_due_periodics(self, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        with self._lock:
            self._periodics = [periodic for periodic in self._periodics if not periodic.cancelled]
            due = [periodic for periodic in self._periodics if periodic.next_due <= now]
        for periodic in due:
            # The next run is one period after this one, not a catch-up burst.
            periodic.next_due = max(periodic.next_due + periodic.period_seconds, now)
            self._call(periodic.callback)

    def destroy(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        with self._lock:
            self._readers.clear()
            self._periodics.clear()
            side_node, self._side_node = self._side_node, None
            self._thread = None
        if side_node is not None:
            side_node.destroy_node()

    def _ensure_started_locked(self) -> None:
        if self._thread is None and not self._stop.is_set():
            self._thread = threading.Thread(target=self._run, name="iii-runtime-api-delivery", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        next_tick = time.monotonic() + self._period
        while not self._stop.wait(max(0.0, next_tick - time.monotonic())):
            self.deliver()
            self.run_due_periodics()
            next_tick += self._period
            now = time.monotonic()
            if next_tick <= now:
                # Ticks missed while delivery ran late are skipped, not run
                # back to back.
                next_tick = now + self._period

    def _take(self, subscription: Any) -> Any | None:
        try:
            return _take(subscription)
        except Exception as exc:
            self._log_error(id(subscription), f"taking from {getattr(subscription, 'topic_name', subscription)}", exc)
            return None

    def _call(self, callback: Callable[..., Any], *args: Any) -> None:
        try:
            callback(*args)
        except Exception as exc:
            self._log_error(id(callback), f"callback {getattr(callback, '__qualname__', callback)}", exc)

    def _log_error(self, source: int, label: str, exc: Exception) -> None:
        now = time.monotonic()
        last = self._error_logged_at.get(source)
        if last is not None and now - last < CALLBACK_ERROR_LOG_PERIOD_SECONDS:
            return
        self._error_logged_at[source] = now
        _LOGGER.error("runtime topic delivery: %s failed: %s", label, exc, exc_info=exc)


def register_sampler(node: Any, sampler: TopicSampler) -> None:
    _SAMPLERS[node] = sampler


def create_sampled_subscription(
    node: Any,
    msg_type: Any,
    topic: str,
    callback: Callable[[Any], Any],
    qos: Any,
    *,
    rate_hz: float | None = None,
) -> Any:
    """Subscribe to a state topic whose callback only needs the newest message.

    ``rate_hz`` lowers the delivery rate below the sampler's own. Without a
    sampler registered for the node (as in unit tests), this is a plain
    subscription.
    """
    sampler = _SAMPLERS.get(node)
    if sampler is None:
        return node.create_subscription(msg_type, topic, callback, qos)
    return sampler.create_subscription(msg_type, topic, callback, qos, rate_hz=rate_hz)


def at_most(rate_hz: float, callback: Callable[[Any], Any], *, clock: Callable[[], float] = time.monotonic) -> Callable[[Any], None]:
    """Pass a message to the callback at most ``rate_hz`` times per second."""
    # A tenth of a period of slack so a delivery tick that lands slightly early still counts.
    period = 1.0 / rate_hz
    last: list[float | None] = [None]

    def limited(message: Any) -> None:
        now = clock()
        if last[0] is not None and now - last[0] < period * 0.9:
            return
        last[0] = now
        callback(message)

    return limited


def create_batched_subscription(
    node: Any,
    msg_type: Any,
    topic: str,
    callback: Callable[[Any], Any],
    qos: Any,
) -> Any:
    """Subscribe to a topic whose callback needs every message, in order.

    Messages queue up to the QoS depth and are handed over within one
    delivery period. Without a sampler registered for the node, this is a
    plain subscription.
    """
    sampler = _SAMPLERS.get(node)
    if sampler is None:
        return node.create_subscription(msg_type, topic, callback, qos)
    return sampler.create_subscription(msg_type, topic, callback, qos, batched=True)


def create_periodic(node: Any, period_seconds: float, callback: Callable[[], Any]) -> Any:
    """Run a callback periodically on the delivery thread instead of an executor timer.

    Without a sampler registered for the node, this is a plain timer.
    """
    sampler = _SAMPLERS.get(node)
    if sampler is None:
        return node.create_timer(period_seconds, callback)
    return sampler.create_periodic(period_seconds, callback)


def _unused_callback(message: Any) -> None:
    del message


def _newest_only(qos: Any) -> Any:
    from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy

    profile = QoSProfile(depth=1) if isinstance(qos, int) else copy.copy(qos)
    profile.history = HistoryPolicy.KEEP_LAST
    profile.depth = 1
    if profile.durability != DurabilityPolicy.TRANSIENT_LOCAL:
        profile.reliability = ReliabilityPolicy.BEST_EFFORT
    return profile


def _queued(qos: Any) -> Any:
    from rclpy.qos import HistoryPolicy, QoSProfile

    profile = QoSProfile(depth=qos) if isinstance(qos, int) else copy.copy(qos)
    profile.history = HistoryPolicy.KEEP_LAST
    return profile


def _profile_key(profile: Any) -> tuple:
    return (profile.history, profile.depth, profile.reliability, profile.durability)


def _take(subscription: Any) -> Any | None:
    from rclpy.exceptions import InvalidHandle

    try:
        with subscription.handle:
            taken = subscription.handle.take_message(subscription.msg_type, subscription.raw)
    except InvalidHandle:
        return None
    return None if taken is None else taken[0]
