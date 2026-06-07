import asyncio

from iii_drone_contracts import CommandResultMessage, DomainName, GenericDomainState, OperatorEvent, OperatorStatePatch
from iii_drone_contracts.envelopes import EventSource
from iii_drone_runtime.api.state_bus import RuntimeStateBus, system_patch


class _FakeWebSocket:
    def __init__(self):
        self.accepted = False
        self.closed = None
        self.messages = []

    async def accept(self):
        self.accepted = True

    async def close(self, code):
        self.closed = code

    async def send_json(self, payload):
        self.messages.append(payload)


class _DisconnectingWebSocket(_FakeWebSocket):
    async def send_json(self, payload):
        del payload
        raise RuntimeError('Cannot call "send" once a close message has been sent.')


def test_state_bus_sends_full_snapshot_on_connect_and_cleans_up():
    bus = RuntimeStateBus()
    websocket = _FakeWebSocket()

    assert asyncio.run(bus.connect(websocket))
    assert websocket.accepted
    assert websocket.messages[0]["message_type"] == "snapshot"

    bus.disconnect(websocket)
    assert bus.active_websocket is None


def test_state_bus_rejects_second_active_websocket():
    bus = RuntimeStateBus()
    first = _FakeWebSocket()
    second = _FakeWebSocket()

    assert asyncio.run(bus.connect(first))
    assert not asyncio.run(bus.connect(second))
    assert second.closed == 1008


def test_state_bus_clears_active_socket_when_initial_snapshot_send_fails():
    bus = RuntimeStateBus()
    websocket = _DisconnectingWebSocket()

    assert not asyncio.run(bus.connect(websocket))
    assert websocket.accepted
    assert bus.active_websocket is None


def test_state_bus_clears_active_socket_when_patch_send_fails():
    bus = RuntimeStateBus()
    websocket = _FakeWebSocket()
    assert asyncio.run(bus.connect(websocket))
    websocket.send_json = _DisconnectingWebSocket().send_json

    asyncio.run(bus.send_patch(system_patch({"api": "up"})))

    assert bus.active_websocket is None


def test_state_bus_sends_patch_event_and_command_result_messages():
    bus = RuntimeStateBus()
    websocket = _FakeWebSocket()
    asyncio.run(bus.connect(websocket))

    patch = system_patch({"api": "up"}, patch_id="patch-1")
    event = OperatorEvent(
        event_id="event-1",
        source=EventSource.RUNTIME,
        category="health",
        message="api up",
    )
    result = CommandResultMessage(request_id="req-1", command_id="px4.hold", status="succeeded")

    asyncio.run(bus.send_patch(patch))
    asyncio.run(bus.send_event(event))
    asyncio.run(bus.send_command_result(result))

    assert [message["message_type"] for message in websocket.messages] == [
        "snapshot",
        "patch",
        "event",
        "command_result",
    ]


def test_state_bus_coalesces_domain_patches():
    bus = RuntimeStateBus()
    websocket = _FakeWebSocket()
    asyncio.run(bus.connect(websocket))

    bus.coalesce_patch(
        system_patch({"api": "starting"}, patch_id="system-old")
    )
    bus.coalesce_patch(
        system_patch({"api": "up"}, patch_id="system-new")
    )
    bus.coalesce_patch(
        # Use another domain so the flush emits one per domain.
        patch=OperatorStatePatch(
            domain=DomainName.VEHICLE,
            state=GenericDomainState(source_label="runtime_api", freshness="fresh", value={"armed": False}),
            patch_id="vehicle",
        )
    )
    asyncio.run(bus.flush_patches())

    patch_messages = [message for message in websocket.messages if message["message_type"] == "patch"]
    assert [message["message_id"] for message in patch_messages] == ["system-new", "vehicle"]
