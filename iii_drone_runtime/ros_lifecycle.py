"""Optional rclpy executor lifecycle for iii-runtime-api."""

from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any, Callable

from iii_drone_contracts import EventSource

from .api.events import RuntimeEventLog


@dataclass(frozen=True)
class RosLifecycleStatus:
    available: bool
    running: bool
    degraded_reason: str | None = None


class RuntimeRosExecutor:
    def __init__(self, *, rclpy_module: Any | None = None, event_log: RuntimeEventLog | None = None):
        self._rclpy = rclpy_module
        self._event_log = event_log or RuntimeEventLog()
        self._queue: Queue[tuple[str, dict]] = Queue()
        self._stop = Event()
        self._thread: Thread | None = None
        self._executor = None
        self._node = None
        self._subscriptions: list[Any] = []
        self._degraded_reason: str | None = None

    def status(self) -> RosLifecycleStatus:
        return RosLifecycleStatus(
            available=self._rclpy is not None and self._degraded_reason is None,
            running=self._thread is not None and self._thread.is_alive(),
            degraded_reason=self._degraded_reason,
        )

    @property
    def node(self) -> Any | None:
        return self._node

    def start(self, subscription_registrars: list[Callable[[Any], Any]] | None = None) -> RosLifecycleStatus:
        if self._rclpy is None:
            self._degraded_reason = "rclpy unavailable"
            return self.status()
        if self._thread is not None and self._thread.is_alive():
            return self.status()

        try:
            if hasattr(self._rclpy, "init"):
                self._rclpy.init(args=None)
            self._executor = self._rclpy.executors.SingleThreadedExecutor()
            self._node = self._rclpy.create_node("iii_runtime_api")
            self._subscriptions = self._create_subscriptions(subscription_registrars or [])
            self._executor.add_node(self._node)
        except Exception as exc:
            self._degraded_reason = str(exc)
            return self.status()

        self._stop.clear()
        self._thread = Thread(target=self._spin, name="iii-runtime-api-rclpy", daemon=True)
        self._thread.start()
        self._event_log.record_availability_change(label="ros_executor", available=True)
        return self.status()

    def _create_subscriptions(self, registrars: list[Callable[[Any], Any]]) -> list[Any]:
        subscriptions: list[Any] = []
        for registrar in registrars:
            created = registrar(self._node)
            if created is None:
                continue
            if isinstance(created, list):
                subscriptions.extend(created)
            else:
                subscriptions.append(created)
        return subscriptions

    def _spin(self) -> None:
        while not self._stop.is_set():
            self._executor.spin_once(timeout_sec=0.1)

    def stop(self) -> RosLifecycleStatus:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        if self._executor is not None and self._node is not None:
            try:
                self._executor.remove_node(self._node)
            except Exception:
                pass
        if self._node is not None and hasattr(self._node, "destroy_node"):
            self._node.destroy_node()
        if self._rclpy is not None and hasattr(self._rclpy, "shutdown"):
            try:
                self._rclpy.shutdown()
            except Exception:
                pass
        self._thread = None
        self._executor = None
        self._node = None
        self._subscriptions = []
        self._event_log.record_availability_change(label="ros_executor", available=False, reason="stopped")
        return self.status()

    def enqueue_callback_update(self, topic: str, payload: dict) -> None:
        self._queue.put((topic, payload))

    def drain_updates(self) -> list[tuple[str, dict]]:
        updates: list[tuple[str, dict]] = []
        while True:
            try:
                updates.append(self._queue.get_nowait())
            except Empty:
                return updates

    def record_availability_change(self, label: str, available: bool, reason: str | None = None) -> None:
        self._event_log.record_availability_change(label=label, available=available, reason=reason)
