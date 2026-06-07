import asyncio
from dataclasses import dataclass

from fastapi.testclient import TestClient

from iii_drone_contracts import CommandId
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.px4_adapter import PersistentPx4CommandAdapter
from iii_drone_runtime.api.state_bus import RuntimeStateBus


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
        yield self.system.armed
        await asyncio.Event().wait()

    async def flight_mode(self):
        yield self.system.flight_mode
        await asyncio.Event().wait()

    async def in_air(self):
        yield self.system.in_air
        await asyncio.Event().wait()


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

        armed = await adapter.arm()
        takeoff = await adapter.takeoff()
        hold = await adapter.hold()
        land = await adapter.land()

        assert system.commands == ["arm", "takeoff", "hold", "land"]
        assert armed.armed is True
        assert takeoff.in_air is True
        assert hold.nav_state == "hold"
        assert land.in_air is False
        assert adapter.status().armed is True
        assert adapter.status().flight_mode == "LAND"
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
        assert result_holder["telemetry"].armed is True
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
    assert command.json()["result"]["telemetry"]["nav_state"] == "hold"


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
                    message["message_type"] == "patch" and message["payload"]["domain"] == "vehicle"
                    for message in capture.messages
                )
                and any(
                    message["message_type"] == "patch" and message["payload"]["domain"] == "control"
                    for message in capture.messages
                )
            )

        asyncio.run(patches_published())

    vehicle_patch = next(
        message for message in capture.messages if message["message_type"] == "patch" and message["payload"]["domain"] == "vehicle"
    )
    control_patch = next(
        message for message in capture.messages if message["message_type"] == "patch" and message["payload"]["domain"] == "control"
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
