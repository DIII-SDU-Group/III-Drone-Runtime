"""Thread-safe helpers for synchronous calls through the runtime ROS executor."""

from __future__ import annotations

from inspect import signature
from threading import Event, Lock
from typing import Any


_CALLBACK_GROUP_ATTRIBUTE = "_iii_runtime_service_callback_group"
_callback_group_lock = Lock()


def runtime_reentrant_callback_group(node: Any) -> Any:
    """Return the callback group shared by runtime service and action clients."""

    group = getattr(node, _CALLBACK_GROUP_ATTRIBUTE, None)
    if group is None:
        with _callback_group_lock:
            group = getattr(node, _CALLBACK_GROUP_ATTRIBUTE, None)
            if group is None:
                from rclpy.callback_groups import ReentrantCallbackGroup

                group = ReentrantCallbackGroup()
                setattr(node, _CALLBACK_GROUP_ATTRIBUTE, group)
    return group


def create_reentrant_client(node: Any, service_type: Any, service_name: str) -> Any:
    """Create a service client without coupling it to subscription callbacks.

    Runtime service calls originate on HTTP worker threads while the node is
    already owned by ``RuntimeRosExecutor``. A reentrant callback group lets the
    executor complete service futures even while another node callback is
    active. Lightweight test nodes without callback-group support keep using
    their normal ``create_client`` signature.
    """

    create_client = node.create_client
    try:
        supports_callback_group = "callback_group" in signature(create_client).parameters
    except (TypeError, ValueError):
        supports_callback_group = False
    if not supports_callback_group:
        return create_client(service_type, service_name)

    return create_client(
        service_type,
        service_name,
        callback_group=runtime_reentrant_callback_group(node),
    )


def wait_for_service_response(client: Any, request: Any, *, timeout_sec: float, label: str) -> Any:
    """Wait for a future completed by the node's existing executor.

    Never call ``rclpy.spin_until_future_complete`` here: spinning a node from a
    second executor can detach it from ``RuntimeRosExecutor`` and strand every
    subsequent subscription and service response.
    """

    future = client.call_async(request)
    completed = Event()
    future.add_done_callback(lambda _future: completed.set())
    if not completed.wait(timeout=timeout_sec):
        raise TimeoutError(f"timed out waiting for {label}")
    exception_getter = getattr(future, "exception", None)
    exception = exception_getter() if exception_getter is not None else None
    if exception is not None:
        raise RuntimeError(f"ROS service {label} failed: {exception}") from exception
    return future.result()
