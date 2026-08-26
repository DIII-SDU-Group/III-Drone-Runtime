"""Bounded runtime event collection."""

from __future__ import annotations

from collections import deque
import logging
from typing import Any, Deque, Mapping
import uuid

from iii_drone_contracts import CommandResultMessage, EventSource, OperatorEvent


LOGGER = logging.getLogger("iii_drone_runtime.events")
LOGGER.setLevel(logging.INFO)


class RuntimeEventLog:
    def __init__(self, max_events: int = 200):
        self._events: Deque[OperatorEvent] = deque(maxlen=max_events)

    def append(self, event: OperatorEvent) -> OperatorEvent:
        self._events.append(event)
        return event

    def record_command_request(
        self,
        *,
        command_id: str,
        request_id: str,
        source: EventSource,
        client_label: str | None = None,
        mutating: bool = False,
    ) -> OperatorEvent:
        event = self.append(
            OperatorEvent(
                event_id=str(uuid.uuid4()),
                source=source,
                category="command_request",
                severity="info",
                message=f"command requested: {command_id}",
                request_id=request_id,
                command_id=command_id,
                details={"client_label": client_label, "mutating": mutating},
            )
        )
        if mutating:
            LOGGER.info("mutating command requested", extra={"command_id": command_id, "request_id": request_id})
        return event

    def record_command_decision(
        self,
        *,
        command_id: str,
        request_id: str,
        accepted: bool,
        reason: str | None,
        source: EventSource = EventSource.RUNTIME,
        client_label: str | None = None,
        mutating: bool = True,
        details: Mapping[str, Any] | None = None,
    ) -> OperatorEvent:
        severity = "info" if accepted else "warning"
        message = f"command {'accepted' if accepted else 'rejected'}: {command_id}"
        if reason:
            message = f"{message} - {reason}"
        event = self.append(
            OperatorEvent(
                event_id=str(uuid.uuid4()),
                source=source,
                category="command_decision",
                severity=severity,
                message=message,
                request_id=request_id,
                command_id=command_id,
                details={
                    "client_label": client_label,
                    "accepted": accepted,
                    "reason": reason,
                    **dict(details or {}),
                },
            )
        )
        if mutating:
            LOGGER.log(
                logging.INFO if accepted else logging.WARNING,
                message,
                extra={"command_id": command_id, "request_id": request_id},
            )
        return event

    def record_command_result(self, result: CommandResultMessage) -> OperatorEvent:
        return self.append(
            OperatorEvent(
                event_id=str(uuid.uuid4()),
                source=EventSource.RUNTIME,
                category="command_result",
                severity="info" if result.status == "succeeded" else "warning",
                message=f"command result: {result.command_id} {result.status}",
                request_id=result.request_id,
                command_id=result.command_id,
                details=result.model_dump(mode="json"),
            )
        )

    def record_command_progress(
        self,
        *,
        command_id: str,
        request_id: str,
        stage: str,
        status: str,
        detail: str,
        result: dict | None = None,
    ) -> OperatorEvent:
        return self.append(
            OperatorEvent(
                event_id=str(uuid.uuid4()),
                source=EventSource.RUNTIME,
                category="command_progress",
                severity="warning" if status in {"degraded", "failed"} else "info",
                message=f"{stage}: {detail}",
                request_id=request_id,
                command_id=command_id,
                details={"stage": stage, "status": status, "result": result or {}},
            )
        )

    def record_availability_change(self, *, label: str, available: bool, reason: str | None = None) -> OperatorEvent:
        return self.append(
            OperatorEvent(
                event_id=str(uuid.uuid4()),
                source=EventSource.RUNTIME,
                category="availability",
                severity="info" if available else "warning",
                message=f"{label} {'available' if available else 'unavailable'}",
                details={"label": label, "available": available, "reason": reason},
            )
        )

    def record_runtime_validation_failure(self, *, message: str, request_id: str | None = None) -> OperatorEvent:
        return self.append(
            OperatorEvent(
                event_id=str(uuid.uuid4()),
                source=EventSource.RUNTIME,
                category="validation_failure",
                severity="warning",
                message=message,
                request_id=request_id,
            )
        )

    def record_cli_rejection(
        self,
        *,
        command_id: str,
        request_id: str,
        client_label: str | None,
        reason: str,
    ) -> OperatorEvent:
        event = self.record_command_decision(
            command_id=command_id,
            request_id=request_id,
            accepted=False,
            reason=reason,
            source=EventSource.CLI,
            client_label=client_label,
            mutating=True,
        )
        event.category = "remote_cli_conflict"
        return event

    def recent(self) -> list[OperatorEvent]:
        return list(self._events)
