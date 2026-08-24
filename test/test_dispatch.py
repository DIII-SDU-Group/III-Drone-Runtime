from fastapi.testclient import TestClient

from iii_drone_contracts import ActionStartResponse, ServiceCallResponse
from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.dispatch import DispatchRegistry


def test_dispatch_registry_routes_fake_action_and_service_handlers():
    registry = DispatchRegistry.empty()

    registry.register_action(
        "px4.hold",
        lambda request: ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=True,
            action_id="action-1",
            result={"queued": True},
        ),
    )
    registry.register_service(
        "logs",
        "logs.list_sources",
        lambda request: ServiceCallResponse(
            request_id=request.request_id,
            service_type=request.service_type,
            service_name=request.service_name,
            ok=True,
            result={"sources": ["daemon"]},
        ),
    )

    action_response, command_result = registry.start_action(
        type("Request", (), {"request_id": "req-1", "command_id": "px4.hold"})()
    )
    service_response = registry.call_service(
        type("Request", (), {"request_id": "req-2", "service_type": "logs", "service_name": "logs.list_sources"})()
    )

    assert action_response.accepted is True
    assert command_result.status == "accepted"
    assert service_response.ok is True
    assert service_response.result == {"sources": ["daemon"]}


def test_dispatch_registry_rejects_unknown_action_and_service():
    registry = DispatchRegistry.empty()

    action_response, command_result = registry.start_action(
        type("Request", (), {"request_id": "req-3", "command_id": "unknown"})()
    )
    service_response = registry.call_service(
        type("Request", (), {"request_id": "req-4", "service_type": "ros", "service_name": "arbitrary"})()
    )

    assert action_response.accepted is False
    assert action_response.rejection.code == "handler_unavailable"
    assert command_result is None
    assert service_response.ok is False
    assert service_response.error.code == "handler_unavailable"


def test_dispatch_registry_replays_identical_request_without_duplicate_side_effect():
    registry = DispatchRegistry.empty()
    calls = []

    def handler(request):
        calls.append(request.request_id)
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=True,
            action_id="action-idempotent",
        )

    registry.register_action("px4.hold", handler)
    request = type(
        "Request",
        (),
        {"request_id": "same-request", "command_id": "px4.hold", "parameters": {"reason": "operator"}},
    )()

    first = registry.start_action(request)
    second = registry.start_action(request)

    assert first == second
    assert calls == ["same-request"]


def test_dispatch_registry_rejects_request_id_reuse_with_different_payload():
    registry = DispatchRegistry.empty()
    registry.register_action(
        "px4.hold",
        lambda request: ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
        ),
    )

    first = type("Request", (), {"request_id": "reused", "command_id": "px4.hold", "parameters": {}})()
    changed = type(
        "Request",
        (),
        {"request_id": "reused", "command_id": "px4.hold", "parameters": {"changed": True}},
    )()

    assert registry.start_action(first)[0].accepted is True
    response, result = registry.start_action(changed)
    assert response.accepted is False
    assert response.rejection.code == "conflict"
    assert result is None


def test_dispatch_registry_marks_synchronous_actions_succeeded():
    registry = DispatchRegistry.empty()
    registry.register_action(
        "payload.release",
        lambda request: ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=False,
        ),
    )
    request = type("Request", (), {"request_id": "sync", "command_id": "payload.release", "parameters": {}})()

    _response, result = registry.start_action(request)

    assert result.status == "succeeded"


def test_runtime_api_uses_explicit_dispatch_registry():
    registry = DispatchRegistry.empty()
    registry.register_action(
        "px4.hold",
        lambda request: ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=True,
            action_id="action-2",
        ),
    )
    registry.register_service(
        "logs",
        "logs.list_sources",
        lambda request: ServiceCallResponse(
            request_id=request.request_id,
            service_type=request.service_type,
            service_name=request.service_name,
            ok=True,
            result={"sources": ["runtime-api"]},
        ),
    )
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            dispatch_registry=registry,
        )
    )
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    headers = {"Authorization": f"Bearer {token}"}

    action = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "req-5", "command_id": "px4.hold"},
    )
    service = client.post(
        "/commands/services/call",
        headers=headers,
        json={"request_id": "req-6", "service_type": "logs", "service_name": "logs.list_sources"},
    )
    rejected = client.post(
        "/commands/actions/start",
        headers=headers,
        json={"request_id": "req-7", "command_id": "ros.anything"},
    )

    assert action.json()["accepted"] is True
    assert service.json()["ok"] is True
    assert rejected.json()["accepted"] is False
    assert rejected.json()["rejection"]["code"] == "handler_unavailable"
