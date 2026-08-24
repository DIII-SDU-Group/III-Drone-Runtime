"""Contract-typed WebSocket state bus."""

from __future__ import annotations

import uuid

from fastapi import WebSocket, status
from starlette.websockets import WebSocketDisconnect

from iii_drone_contracts import (
    CommandResultMessage,
    DomainName,
    GenericDomainState,
    OperatorEvent,
    OperatorStatePatch,
    OperatorStateSnapshot,
    WebSocketMessage,
)


class RuntimeStateBus:
    def __init__(self, snapshot: OperatorStateSnapshot | None = None):
        self.snapshot = snapshot or OperatorStateSnapshot()
        self.active_websocket: WebSocket | None = None
        self.pending_patches: dict[DomainName, OperatorStatePatch] = {}

    async def connect(self, websocket: WebSocket) -> bool:
        if self.active_websocket is not None:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return False
        await websocket.accept()
        self.active_websocket = websocket
        try:
            await self.send_snapshot()
        except (RuntimeError, WebSocketDisconnect):
            self.disconnect(websocket)
            return False
        return self.active_websocket is websocket

    def disconnect(self, websocket: WebSocket) -> None:
        if self.active_websocket is websocket:
            self.active_websocket = None

    def coalesce_patch(self, patch: OperatorStatePatch) -> None:
        self.pending_patches[patch.domain] = patch

    async def flush_patches(self) -> None:
        patches = list(self.pending_patches.values())
        self.pending_patches.clear()
        for patch in patches:
            await self.send_patch(patch)

    async def send_snapshot(self) -> None:
        await self._send(
            WebSocketMessage(
                message_type="snapshot",
                message_id="initial-snapshot",
                payload=self.snapshot,
            )
        )

    async def send_patch(self, patch: OperatorStatePatch) -> None:
        await self._send(
            WebSocketMessage(
                message_type="patch",
                message_id=patch.patch_id or str(uuid.uuid4()),
                payload=patch,
            )
        )

    async def send_event(self, event: OperatorEvent) -> None:
        await self._send(
            WebSocketMessage(
                message_type="event",
                message_id=event.event_id,
                payload=event,
            )
        )

    async def send_command_result(self, result: CommandResultMessage) -> None:
        self.snapshot.command_results = [
            item for item in self.snapshot.command_results if item.request_id != result.request_id
        ]
        self.snapshot.command_results.append(result)
        del self.snapshot.command_results[:-100]
        await self._send(
            WebSocketMessage(
                message_type="command_result",
                message_id=str(uuid.uuid4()),
                payload=result,
            )
        )

    async def _send(self, message: WebSocketMessage) -> None:
        websocket = self.active_websocket
        if websocket is None:
            return
        try:
            await websocket.send_json(message.model_dump(mode="json"))
        except (RuntimeError, WebSocketDisconnect):
            self.disconnect(websocket)


def system_patch(value: dict, *, patch_id: str | None = None) -> OperatorStatePatch:
    return OperatorStatePatch(
        domain=DomainName.SYSTEM,
        state=GenericDomainState(source_label="runtime_api", freshness="fresh", value=value),
        patch_id=patch_id,
    )
