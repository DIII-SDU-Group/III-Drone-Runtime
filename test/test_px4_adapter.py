import asyncio
from dataclasses import dataclass
import subprocess
import sys
import time
from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.px4_adapter import (
    PersistentPx4CommandAdapter,
    default_mavsdk_system_factory,
)
from iii_drone_runtime.api.state_bus import RuntimeStateBus
from iii_drone_runtime.ros_lifecycle import RuntimeRosExecutor


@dataclass
class _ConnectionState:
    is_connected: bool


class _FakeCore:
    def __init__(self):
        self.disconnect = asyncio.Event()
        self.calls = 0

    async def connection_state(self):
        self.calls += 1
        yield _ConnectionState(True)
        if self.calls > 1:
            await self.disconnect.wait()
            yield _ConnectionState(False)


class _FakeTelemetry:
    def __init__(self, system):
        self.system = system

    async def armed(self):
        async for value in self._changes(lambda: self.system.armed):
            yield value

    async def flight_mode(self):
        async for value in self._changes(lambda: self.system.flight_mode):
            yield value

    async def in_air(self):
        async for value in self._changes(lambda: self.system.in_air):
            yield value

    async def health(self):
        async for value in self._changes(
            lambda: self.system.arming_checks_passed
        ):
            yield type("Health", (), {"is_armable": value})()

    async def _changes(self, value_provider):
        missing = object()
        previous = missing
        while True:
            current = value_provider()
            if current != previous:
                previous = current
                yield current
            await asyncio.sleep(0.005)


class _FakeAction:
    def __init__(self, system):
        self.system = system

    async def arm(self):
        self.system.assert_action_loop()
        self.system.commands.append("arm")
        self.system.armed = True

    async def takeoff(self):
        self.system.assert_action_loop()
        self.system.commands.append("takeoff")
        self.system.in_air = True
        self.system.flight_mode = "TAKEOFF"

    async def land(self):
        self.system.assert_action_loop()
        self.system.commands.append("land")
        self.system.in_air = False
        self.system.flight_mode = "LAND"

    async def hold(self):
        self.system.assert_action_loop()
        self.system.commands.append("hold")
        self.system.flight_mode = "HOLD"


class _FakeSystem:
    def __init__(self):
        self.core = _FakeCore()
        self.telemetry = _FakeTelemetry(self)
        self.action = _FakeAction(self)
        self.armed = False
        self.flight_mode = "POSITION"
        self.in_air = False
        self.arming_checks_passed = True
        self.commands = []
        self.closed = False
        self.expected_action_loop = None

    def assert_action_loop(self):
        if self.expected_action_loop is not None:
            assert asyncio.get_running_loop() is self.expected_action_loop

    def close(self):
        self.closed = True

    def disconnect(self):
        self.core.disconnect.set()


class _NeverConnectCore:
    async def connection_state(self):
        await asyncio.Event().wait()
        yield _ConnectionState(False)


class _NeverConnectSystem:
    def __init__(self):
        self.core = _NeverConnectCore()
        self.closed = False

    def close(self):
        self.closed = True


class _ServerOwnedSystemWithoutClose(_FakeSystem):
    close = None

    def __init__(self, process):
        super().__init__()
        self._server_process = process
        self.stop_server_calls = 0

    def _stop_mavsdk_server(self):
        self.stop_server_calls += 1
        process = self._server_process
        if process is not None:
            if process.poll() is None:
                process.kill()
            self._server_process = None


async def _wait_for(predicate, timeout=1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not reached before timeout")


class _CaptureWebSocket:
    def __init__(self):
        self.messages = []

    async def send_json(self, message):
        self.messages.append(message)


def test_px4_adapter_connects_persistently_and_executes_commands():
    async def scenario():
        system = _FakeSystem()
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test",
            system_factory=lambda endpoint: system,
            reconnect_backoff_seconds=0.01,
        )

        await adapter.start()
        await _wait_for(lambda: adapter.status().command_available)

        await adapter.arm()
        await adapter.takeoff()
        await adapter.hold()
        await adapter.land()

        assert system.commands == ["arm", "takeoff", "hold", "land"]
        await adapter.stop()

    asyncio.run(scenario())


def test_px4_adapter_reconnects_after_link_loss():
    async def scenario():
        systems = [_FakeSystem(), _FakeSystem()]
        first = systems[0]

        async def factory(endpoint):
            return systems.pop(0)

        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test",
            system_factory=factory,
            reconnect_backoff_seconds=0.01,
        )

        await adapter.start()
        await _wait_for(lambda: adapter.status().command_available)
        first.disconnect()
        await _wait_for(lambda: first.closed and len(systems) == 0 and adapter.status().command_available)

        assert first.closed is True
        assert adapter.status().reconnect_attempts >= 2
        await adapter.stop()

    asyncio.run(scenario())


def test_px4_adapter_bounds_never_connected_attempt_and_retries():
    async def scenario():
        systems = []
        calls = 0

        def factory(endpoint):
            nonlocal calls
            calls += 1
            system = _NeverConnectSystem()
            systems.append(system)
            return system

        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test",
            system_factory=factory,
            connection_timeout_seconds=0.03,
            reconnect_backoff_seconds=0.01,
        )
        await adapter.start()
        await _wait_for(
            lambda: calls >= 2 and all(system.closed for system in systems),
            timeout=0.5,
        )
        assert adapter.status().command_available is False
        assert adapter.status().reconnect_attempts >= 2
        assert all(system.closed for system in systems)
        assert "timed out" in (adapter.status().last_error or "")
        await adapter.stop()

    asyncio.run(scenario())


def test_px4_adapter_recovers_with_fresh_system_after_bounded_attempt_timeout():
    async def scenario():
        stale_system = _NeverConnectSystem()
        healthy_system = _FakeSystem()
        systems = [stale_system, healthy_system]

        def factory(endpoint):
            return systems.pop(0)

        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test",
            system_factory=factory,
            connection_timeout_seconds=0.03,
            reconnect_backoff_seconds=0.01,
        )
        await adapter.start()
        await _wait_for(lambda: adapter.status().command_available)
        assert stale_system.closed is True
        assert healthy_system.commands == []
        await adapter.arm()
        assert healthy_system.commands == ["arm"]
        await adapter.stop()

    asyncio.run(scenario())


def test_px4_adapter_stops_and_reaps_owned_mavsdk_server_without_public_close():
    async def scenario():
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )
        system = _ServerOwnedSystemWithoutClose(process)
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test", system_factory=lambda endpoint: system
        )
        adapter._system = system
        try:
            await adapter._close_system()
            assert system.stop_server_calls == 1
            assert process.poll() is not None
            assert system._server_process is None
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()

    asyncio.run(scenario())


def test_owned_mavsdk_server_cleanup_timeout_is_bounded_after_kill():
    class StuckProcess:
        def __init__(self):
            self.wait_timeouts = []
            self.killed = False

        def poll(self):
            return None

        def terminate(self):
            pass

        def kill(self):
            self.killed = True

        def wait(self, timeout=None):
            self.wait_timeouts.append(timeout)
            raise subprocess.TimeoutExpired("mavsdk_server", timeout)

    async def scenario():
        process = StuckProcess()
        system = SimpleNamespace(
            _server_process=process,
            _stop_mavsdk_server=lambda: None,
        )
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test", system_factory=lambda endpoint: system
        )
        adapter._system = system
        try:
            await adapter._close_system()
        except TimeoutError as exc:
            assert "did not exit after kill" in str(exc)
        else:
            raise AssertionError("stuck owned server cleanup must report timeout")

        assert process.wait_timeouts == [2.0, 2.0]
        assert process.killed is True
        assert system._server_process is process

    asyncio.run(scenario())


def test_default_factory_cancellation_during_connect_cleans_owned_server(monkeypatch):
    async def scenario():
        started = asyncio.Event()
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )

        class BlockingSystem:
            def __init__(self, **_kwargs):
                self._server_process = process
                self.stop_server_calls = 0

            async def connect(self, **_kwargs):
                started.set()
                await asyncio.Event().wait()

            def _stop_mavsdk_server(self):
                self.stop_server_calls += 1
                if process.poll() is None:
                    process.kill()
                self._server_process = None

        instance = BlockingSystem()
        monkeypatch.setitem(sys.modules, "mavsdk", SimpleNamespace(System=lambda **kwargs: instance))
        task = asyncio.create_task(default_mavsdk_system_factory("udp://test"))
        try:
            await asyncio.wait_for(started.wait(), timeout=1.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("factory cancellation was not propagated")
            assert instance.stop_server_calls == 1
            assert process.poll() is not None
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if process.poll() is None:
                process.kill()
            process.wait()

    asyncio.run(scenario())


def test_adapter_bounds_default_factory_connect_and_recovers_fresh_system(monkeypatch):
    async def scenario():
        started = asyncio.Event()
        first_process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )
        second_process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )

        class ConnectableSystem(_FakeSystem):
            def __init__(self, process, *, block_connect):
                super().__init__()
                self._server_process = process
                self.block_connect = block_connect
                self.stop_server_calls = 0

            async def connect(self, **_kwargs):
                if self.block_connect:
                    started.set()
                    await asyncio.Event().wait()

            def _stop_mavsdk_server(self):
                self.stop_server_calls += 1
                process = self._server_process
                if process is not None:
                    if process.poll() is None:
                        process.kill()
                    self._server_process = None

        first = ConnectableSystem(first_process, block_connect=True)
        second = ConnectableSystem(second_process, block_connect=False)
        systems = [first, second]
        monkeypatch.setitem(
            sys.modules,
            "mavsdk",
            SimpleNamespace(System=lambda **_kwargs: systems.pop(0)),
        )
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test",
            system_factory=default_mavsdk_system_factory,
            connection_timeout_seconds=0.05,
            reconnect_backoff_seconds=0.01,
        )
        try:
            await adapter.start()
            await asyncio.wait_for(started.wait(), timeout=1.0)
            await _wait_for(lambda: adapter.status().command_available, timeout=1.0)
            assert first.stop_server_calls == 1
            assert first_process.poll() is not None
            assert adapter.status().reconnect_attempts >= 2
            await adapter.stop()
            assert second.stop_server_calls == 1
            assert second_process.poll() is not None
        finally:
            await adapter.stop()
            for process in (first_process, second_process):
                if process.poll() is None:
                    process.kill()
                process.wait()


def test_default_factory_connect_failure_reaps_owned_server_and_reraises(monkeypatch):
    async def scenario():
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )

        class FailingSystem:
            def __init__(self, **_kwargs):
                self._server_process = process
                self.stop_server_calls = 0

            async def connect(self, **_kwargs):
                raise RuntimeError("connect failed")

            def _stop_mavsdk_server(self):
                self.stop_server_calls += 1
                if process.poll() is None:
                    process.kill()
                self._server_process = None

        system = FailingSystem()
        monkeypatch.setitem(sys.modules, "mavsdk", SimpleNamespace(System=lambda **_kwargs: system))
        try:
            try:
                await default_mavsdk_system_factory("udp://test")
            except RuntimeError as exc:
                assert str(exc) == "connect failed"
            else:
                raise AssertionError("factory failure was not propagated")
            assert system.stop_server_calls == 1
            assert process.poll() is not None
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()

    asyncio.run(scenario())


def test_owned_cleanup_does_not_stop_external_server_without_owned_process():
    async def scenario():
        system = _FakeSystem()
        system._server_process = None
        system.stop_server_calls = 0
        system._stop_mavsdk_server = lambda: setattr(
            system, "stop_server_calls", system.stop_server_calls + 1
        )
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://external", system_factory=lambda endpoint: system
        )
        adapter._system = system
        await adapter._close_system()
        assert system.closed is True
        assert system.stop_server_calls == 0

    asyncio.run(scenario())


def test_px4_adapter_reports_degraded_when_mavlink_unavailable():
    async def failing_factory(endpoint):
        raise RuntimeError("no MAVLink heartbeat")

    async def scenario():
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://missing",
            system_factory=failing_factory,
            reconnect_backoff_seconds=0.01,
        )

        await adapter.start()
        await _wait_for(lambda: adapter.status().last_error == "no MAVLink heartbeat")

        status = adapter.status()
        assert status.command_available is False
        assert status.source_availability == "degraded"
        assert status.degraded_reason == "PX4 MAVSDK command transport unavailable"
        await adapter.stop()

    asyncio.run(scenario())


def test_px4_adapter_sync_dispatch_runs_on_adapter_loop_from_other_thread():
    async def scenario():
        system = _FakeSystem()
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test",
            system_factory=lambda endpoint: system,
            reconnect_backoff_seconds=0.01,
        )

        await adapter.start()
        await _wait_for(lambda: adapter.status().command_available)
        system.expected_action_loop = asyncio.get_running_loop()
        result_holder = {}

        def dispatch():
            result_holder["telemetry"] = adapter.run_blocking(lambda px4: px4.arm())

        await asyncio.wait_for(asyncio.to_thread(dispatch), timeout=1.0)

        assert system.commands == ["arm"]
        assert result_holder["telemetry"].armed is not None
        await adapter.stop()

    asyncio.run(scenario())


def test_runtime_api_exposes_px4_status_and_px4_command_dispatch():
    system = _FakeSystem()
    adapter = PersistentPx4CommandAdapter(
        endpoint="udp://test",
        system_factory=lambda endpoint: system,
        reconnect_backoff_seconds=0.01,
    )
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        px4_adapter=adapter,
    )

    with TestClient(app) as client:
        token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
        headers = {"Authorization": f"Bearer {token}"}

        status = client.get("/px4/status", headers=headers)
        command = client.post(
            "/commands/actions/start",
            headers=headers,
            json={"request_id": "px4-1", "command_id": CommandId.PX4_HOLD.value},
        )

    assert status.status_code == 200
    assert status.json()["latest"]["command_transport"]["command_available"] is True
    assert command.status_code == 200
    assert command.json()["accepted"] is True
    assert system.commands == ["hold"]


def test_runtime_api_returns_px4_command_without_waiting_for_domain_refresh(monkeypatch):
    system = _FakeSystem()
    adapter = PersistentPx4CommandAdapter(
        endpoint="udp://test",
        system_factory=lambda endpoint: system,
        reconnect_backoff_seconds=0.01,
    )
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        px4_adapter=adapter,
    )

    # This is the expensive reconciliation path that used to execute inline
    # after every accepted command.
    monkeypatch.setattr(
        "iii_drone_runtime.api.app.RuntimeMapAggregator.state",
        lambda self, force=False: (time.sleep(0.25), self._state)[1],
    )

    with TestClient(app) as client:
        token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
        started_at = time.monotonic()
        response = client.post(
            "/commands/actions/start",
            headers={"Authorization": f"Bearer {token}"},
            json={"request_id": "px4-fast-arm", "command_id": CommandId.PX4_ARM.value},
        )
        elapsed = time.monotonic() - started_at

    assert response.json()["accepted"] is True
    assert elapsed < 0.2


def test_px4_commands_do_not_wait_for_fresh_telemetry_subscriptions():
    async def scenario():
        system = _FakeSystem()
        adapter = PersistentPx4CommandAdapter(
            endpoint="udp://test",
            system_factory=lambda endpoint: system,
            reconnect_backoff_seconds=0.01,
        )

        await adapter.start()
        await _wait_for(lambda: adapter.status().command_available)

        async def blocked_snapshot():
            await asyncio.Event().wait()

        adapter.telemetry_snapshot = blocked_snapshot
        await asyncio.wait_for(adapter.arm(), timeout=0.1)
        await asyncio.wait_for(adapter.takeoff(), timeout=0.1)

        assert system.commands == ["arm", "takeoff"]
        await adapter.stop()

    asyncio.run(scenario())


def test_runtime_api_publishes_vehicle_and_control_patches_after_px4_command():
    system = _FakeSystem()
    adapter = PersistentPx4CommandAdapter(
        endpoint="udp://test",
        system_factory=lambda endpoint: system,
        reconnect_backoff_seconds=0.01,
    )
    state_bus = RuntimeStateBus()
    capture = _CaptureWebSocket()
    state_bus.active_websocket = capture
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        px4_adapter=adapter,
        state_bus=state_bus,
        ros_executor=RuntimeRosExecutor(rclpy_module=None),
    )

    with TestClient(app) as client:
        token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
        headers = {"Authorization": f"Bearer {token}"}
        command = client.post(
            "/commands/actions/start",
            headers=headers,
            json={"request_id": "px4-arm-1", "command_id": CommandId.PX4_ARM.value},
        )
        assert command.status_code == 200
        assert command.json()["accepted"] is True

        async def patches_published():
            await _wait_for(
                lambda: any(
                    message["message_type"] == "patch"
                    and message["payload"]["domain"] == "vehicle"
                    and message["payload"]["state"]["armed"] is True
                    for message in capture.messages
                )
                and any(
                    message["message_type"] == "patch" and message["payload"]["domain"] == "control"
                    for message in capture.messages
                ),
                timeout=10.0,
            )

        asyncio.run(patches_published())

    vehicle_patch = next(
        message for message in capture.messages
        if message["message_type"] == "patch"
        and message["payload"]["domain"] == "vehicle"
        and message["payload"]["state"]["armed"] is True
    )
    control_patch = next(
        message
        for message in capture.messages
        if message["message_type"] == "patch"
        and message["payload"]["domain"] == "control"
        and message["payload"]["state"]["latest"]["command_permissions"][CommandId.PX4_TAKEOFF.value] == []
    )
    assert vehicle_patch["payload"]["state"]["armed"] is True
    assert control_patch["payload"]["state"]["latest"]["command_permissions"][CommandId.PX4_TAKEOFF.value] == []


def test_runtime_api_px4_command_rejects_with_frontend_visible_reason_when_unavailable():
    adapter = PersistentPx4CommandAdapter(endpoint="udp://disabled", enabled=False)
    app = create_app(
        settings=RuntimeApiSettings(
            runtime_id="test-runtime",
            runtime_name="Test Runtime",
            browser_password="secret",
            cli_token="cli-secret",
        ),
        px4_adapter=adapter,
    )

    with TestClient(app) as client:
        token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
        headers = {"Authorization": f"Bearer {token}"}
        command = client.post(
            "/commands/actions/start",
            headers=headers,
            json={"request_id": "px4-2", "command_id": CommandId.PX4_ARM.value},
        )

    payload = command.json()
    assert payload["accepted"] is False
    assert payload["rejection"]["code"] == "degraded_state"
    assert "disabled by configuration" in payload["rejection"]["message"]
    assert payload["result"]["transport"]["command_available"] is False
