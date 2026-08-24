from threading import Event, Thread
from types import SimpleNamespace

import pytest

from iii_drone_runtime.ros_services import (
    create_reentrant_client,
    runtime_reentrant_callback_group,
    wait_for_service_response,
)


class _Future:
    def __init__(self):
        self._callback = None
        self._callback_ready = Event()
        self._result = None
        self._exception = None

    def add_done_callback(self, callback):
        self._callback = callback
        self._callback_ready.set()

    def complete(self, *, result=None, exception=None):
        self._result = result
        self._exception = exception
        self._callback(self)

    def result(self):
        return self._result

    def exception(self):
        return self._exception


class _Client:
    def __init__(self, future):
        self.future = future

    def call_async(self, request):
        self.request = request
        return self.future


def test_wait_for_service_response_uses_existing_executor_completion():
    future = _Future()
    client = _Client(future)
    response = SimpleNamespace(success=True)

    worker = Thread(target=lambda: (future._callback_ready.wait(), future.complete(result=response)))
    worker.start()
    assert wait_for_service_response(client, object(), timeout_sec=1.0, label="test") is response
    worker.join()


def test_wait_for_service_response_reports_timeout_and_future_errors():
    with pytest.raises(TimeoutError, match="timed out waiting for test"):
        wait_for_service_response(_Client(_Future()), object(), timeout_sec=0.001, label="test")

    future = _Future()
    client = _Client(future)
    worker = Thread(
        target=lambda: (
            future._callback_ready.wait(),
            future.complete(exception=ValueError("bad response")),
        )
    )
    worker.start()
    with pytest.raises(RuntimeError, match="ROS service test failed: bad response"):
        wait_for_service_response(client, object(), timeout_sec=1.0, label="test")
    worker.join()


def test_create_reentrant_client_preserves_simple_test_node_signature():
    class Node:
        def create_client(self, service_type, service_name):
            return service_type, service_name

    assert create_reentrant_client(Node(), "type", "/service") == ("type", "/service")


def test_runtime_callback_group_is_reused_by_all_transports(monkeypatch):
    class Group:
        pass

    monkeypatch.setattr("rclpy.callback_groups.ReentrantCallbackGroup", Group)
    node = SimpleNamespace()

    first = runtime_reentrant_callback_group(node)
    second = runtime_reentrant_callback_group(node)

    assert first is second
    assert isinstance(first, Group)
