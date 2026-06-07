"""Persistent MAVSDK-backed PX4 command transport."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import inspect
import threading
from typing import Any

from iii_drone_contracts import (
    ActionStartResponse,
    CommandId,
    CommandRejection,
    CommandRequest,
    ErrorCode,
    EventSource,
    HandlerPermission,
    SourceAvailability,
    VehicleDomainState,
)

from .dispatch import DispatchRegistry
from .events import RuntimeEventLog


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Px4CommandTelemetry:
    armed: bool | None = None
    flight_mode: str | None = None
    nav_state: str | None = None
    in_air: bool | None = None


@dataclass(frozen=True)
class Px4CommandTransportStatus:
    enabled: bool
    endpoint: str
    connected: bool
    source_availability: str
    degraded_reason: str | None = None
    last_heartbeat_at: datetime | None = None
    last_update_at: datetime | None = None
    armed: bool | None = None
    flight_mode: str | None = None
    nav_state: str | None = None
    in_air: bool | None = None
    reconnect_attempts: int = 0
    last_error: str | None = None

    @property
    def command_available(self) -> bool:
        return self.enabled and self.connected and self.degraded_reason is None

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "endpoint": self.endpoint,
            "connected": self.connected,
            "source_availability": self.source_availability,
            "degraded_reason": self.degraded_reason,
            "last_heartbeat_at": self.last_heartbeat_at.isoformat() if self.last_heartbeat_at else None,
            "last_update_at": self.last_update_at.isoformat() if self.last_update_at else None,
            "armed": self.armed,
            "flight_mode": self.flight_mode,
            "nav_state": self.nav_state,
            "in_air": self.in_air,
            "reconnect_attempts": self.reconnect_attempts,
            "last_error": self.last_error,
            "command_available": self.command_available,
        }

    def as_vehicle_state(self) -> VehicleDomainState:
        freshness = "fresh" if self.command_available else "unknown"
        if self.enabled and self.last_update_at and self.degraded_reason:
            freshness = "stale"
        return VehicleDomainState(
            source_label="mavsdk",
            source_timestamp=self.last_update_at,
            freshness=freshness,
            source_availability=self.source_availability,
            degraded_reason=self.degraded_reason,
            latest={"command_transport": self.as_dict()},
            armed=self.armed,
            in_air=self.in_air,
            nav_state=self.nav_state,
            flight_mode=self.flight_mode,
        )


MavsdkSystemFactory = Callable[[str], Awaitable[Any] | Any]


class MavsdkSystemFactoryUnavailable(RuntimeError):
    pass


async def default_mavsdk_system_factory(endpoint: str) -> Any:
    try:
        from mavsdk import System
    except ImportError as exc:
        raise MavsdkSystemFactoryUnavailable("mavsdk Python package is not installed") from exc

    system = System()
    await system.connect(system_address=endpoint)
    return system


class PersistentPx4CommandAdapter:
    """Maintains a MAVSDK connection and exposes PX4 command operations.

    The adapter never imports MAVSDK at module import time. Tests can inject a
    factory that returns a fake MAVSDK-like object with core, telemetry, and
    action attributes.
    """

    def __init__(
        self,
        *,
        endpoint: str = "udpin://0.0.0.0:14540",
        enabled: bool = True,
        system_factory: MavsdkSystemFactory | None = None,
        reconnect_backoff_seconds: float = 1.0,
        stale_after_seconds: float = 3.0,
    ):
        self.endpoint = endpoint
        self.enabled = enabled
        self.system_factory = system_factory or default_mavsdk_system_factory
        self.reconnect_backoff_seconds = reconnect_backoff_seconds
        self.stale_after_seconds = stale_after_seconds
        self._system: Any | None = None
        self._monitor_task: asyncio.Task | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stopped = asyncio.Event()
        self._lock = threading.RLock()
        self._status = Px4CommandTransportStatus(
            enabled=enabled,
            endpoint=endpoint,
            connected=False,
            source_availability=SourceAvailability.DEGRADED.value,
            degraded_reason=(
                "PX4 MAVSDK command transport has not connected yet"
                if enabled
                else "PX4 MAVSDK command transport is disabled by configuration"
            ),
        )

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        if not self.enabled:
            self._set_status(
                connected=False,
                source_availability=SourceAvailability.DEGRADED.value,
                degraded_reason="PX4 MAVSDK command transport is disabled by configuration",
            )
            return
        if self._monitor_task is None or self._monitor_task.done():
            self._stopped = asyncio.Event()
            self._monitor_task = asyncio.create_task(self._monitor(), name="iii-px4-command-adapter")

    async def stop(self) -> None:
        self._stopped.set()
        task = self._monitor_task
        self._monitor_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        await self._close_system()
        self._loop = None
        self._set_status(connected=False)

    def run_blocking(self, operation: Callable[["PersistentPx4CommandAdapter"], Awaitable[Px4CommandTelemetry]]) -> Px4CommandTelemetry:
        loop = self._loop
        if loop is None or loop.is_closed():
            return _run_async_blocking(operation(self))
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is loop:
            raise RuntimeError("PX4 command dispatch cannot block the adapter event loop")
        return asyncio.run_coroutine_threadsafe(operation(self), loop).result()

    def status(self) -> Px4CommandTransportStatus:
        with self._lock:
            status = self._status
        if (
            status.enabled
            and status.connected
            and status.last_update_at is not None
            and (datetime.now(timezone.utc) - status.last_update_at).total_seconds() > self.stale_after_seconds
        ):
            return self._status_with(
                status,
                connected=False,
                source_availability=SourceAvailability.DEGRADED.value,
                degraded_reason="PX4 MAVSDK telemetry is stale",
            )
        return status

    async def arm(self) -> Px4CommandTelemetry:
        await self._require_connected().action.arm()
        return await self.telemetry_snapshot()

    async def takeoff(self) -> Px4CommandTelemetry:
        await self._require_connected().action.takeoff()
        return await self.telemetry_snapshot()

    async def land(self) -> Px4CommandTelemetry:
        await self._require_connected().action.land()
        return await self.telemetry_snapshot()

    async def hold(self) -> Px4CommandTelemetry:
        await self._require_connected().action.hold()
        return await self.telemetry_snapshot()

    async def telemetry_snapshot(self) -> Px4CommandTelemetry:
        system = self._require_connected()
        armed = await self._first_or_none(system.telemetry.armed())
        flight_mode = await self._first_or_none(system.telemetry.flight_mode())
        in_air = await self._first_or_none(system.telemetry.in_air())
        snapshot = Px4CommandTelemetry(
            armed=None if armed is None else bool(armed),
            flight_mode=None if flight_mode is None else str(flight_mode),
            nav_state=None if flight_mode is None else self._normalise_nav_state(str(flight_mode)),
            in_air=None if in_air is None else bool(in_air),
        )
        self._set_telemetry(snapshot)
        return snapshot

    async def _monitor(self) -> None:
        while not self._stopped.is_set():
            try:
                self._set_status(
                    connected=False,
                    source_availability=SourceAvailability.UNAVAILABLE.value,
                    degraded_reason="connecting to PX4 MAVSDK command transport",
                    last_error=None,
                    increment_reconnect=True,
                )
                self._system = await self._create_system()
                connected = await self._wait_connected(self._system)
                if not connected:
                    raise TimeoutError("PX4 MAVSDK connection timed out")
                now = _utc_now()
                self._set_status(
                    connected=True,
                    source_availability=SourceAvailability.AVAILABLE.value,
                    degraded_reason=None,
                    last_heartbeat_at=now,
                    last_update_at=now,
                    last_error=None,
                )
                await self._stream_telemetry_until_disconnect(self._system)
                await self._close_system()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_status(
                    connected=False,
                    source_availability=SourceAvailability.DEGRADED.value,
                    degraded_reason="PX4 MAVSDK command transport unavailable",
                    last_error=str(exc),
                )
                await self._close_system()
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=self.reconnect_backoff_seconds)
                except TimeoutError:
                    pass

    async def _create_system(self) -> Any:
        created = self.system_factory(self.endpoint)
        if inspect.isawaitable(created):
            return await created
        return created

    async def _wait_connected(self, system: Any) -> bool:
        async for state in system.core.connection_state():
            if bool(getattr(state, "is_connected", False)):
                return True
            if self._stopped.is_set():
                return False
        return False

    async def _stream_telemetry_until_disconnect(self, system: Any) -> None:
        connection_task = asyncio.create_task(self._watch_connection_state(system))
        telemetry_tasks = [
            asyncio.create_task(self._watch_telemetry(system.telemetry.armed(), "armed")),
            asyncio.create_task(self._watch_telemetry(system.telemetry.flight_mode(), "flight_mode")),
            asyncio.create_task(self._watch_telemetry(system.telemetry.in_air(), "in_air")),
        ]
        done, pending = await asyncio.wait([connection_task, *telemetry_tasks], return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc

    async def _watch_connection_state(self, system: Any) -> None:
        async for state in system.core.connection_state():
            if bool(getattr(state, "is_connected", False)):
                self._set_status(
                    connected=True,
                    source_availability=SourceAvailability.AVAILABLE.value,
                    degraded_reason=None,
                    last_heartbeat_at=_utc_now(),
                    last_update_at=_utc_now(),
                )
                continue
            self._set_status(
                connected=False,
                source_availability=SourceAvailability.DEGRADED.value,
                degraded_reason="PX4 MAVSDK connection lost",
            )
            return

    async def _watch_telemetry(self, stream: Any, field_name: str) -> None:
        async for value in stream:
            with self._lock:
                current = self._status
                fields = {
                    "armed": current.armed,
                    "flight_mode": current.flight_mode,
                    "nav_state": current.nav_state,
                    "in_air": current.in_air,
                }
                if field_name == "armed":
                    fields["armed"] = bool(value)
                elif field_name == "flight_mode":
                    fields["flight_mode"] = str(value)
                    fields["nav_state"] = self._normalise_nav_state(str(value))
                elif field_name == "in_air":
                    fields["in_air"] = bool(value)
                self._status = self._status_with(
                    current,
                    connected=True,
                    source_availability=SourceAvailability.AVAILABLE.value,
                    degraded_reason=None,
                    last_update_at=_utc_now(),
                    **fields,
                )

    async def _close_system(self) -> None:
        system = self._system
        self._system = None
        if system is None:
            return
        close = getattr(system, "close", None)
        if close is None:
            return
        result = close()
        if inspect.isawaitable(result):
            await result

    def _require_connected(self) -> Any:
        status = self.status()
        if not status.enabled:
            raise RuntimeError(status.degraded_reason or "PX4 command transport is disabled")
        if not status.command_available or self._system is None:
            raise RuntimeError(status.degraded_reason or "PX4 command transport is not connected")
        return self._system

    async def _first_or_none(self, stream: Any) -> Any:
        async for value in stream:
            return value
        return None

    def _set_telemetry(self, telemetry: Px4CommandTelemetry) -> None:
        self._set_status(
            connected=True,
            source_availability=SourceAvailability.AVAILABLE.value,
            degraded_reason=None,
            armed=telemetry.armed,
            flight_mode=telemetry.flight_mode,
            nav_state=telemetry.nav_state,
            in_air=telemetry.in_air,
            last_update_at=_utc_now(),
        )

    def _set_status(self, *, increment_reconnect: bool = False, **changes: Any) -> None:
        with self._lock:
            current = self._status
            reconnect_attempts = current.reconnect_attempts + 1 if increment_reconnect else current.reconnect_attempts
            if "reconnect_attempts" not in changes:
                changes["reconnect_attempts"] = reconnect_attempts
            self._status = self._status_with(current, **changes)

    def _status_with(self, status: Px4CommandTransportStatus, **changes: Any) -> Px4CommandTransportStatus:
        values = status.as_dict()
        values.update(changes)
        values["last_heartbeat_at"] = self._parse_dt(values["last_heartbeat_at"])
        values["last_update_at"] = self._parse_dt(values["last_update_at"])
        return Px4CommandTransportStatus(
            enabled=bool(values["enabled"]),
            endpoint=str(values["endpoint"]),
            connected=bool(values["connected"]),
            source_availability=str(values["source_availability"]),
            degraded_reason=values["degraded_reason"],
            last_heartbeat_at=values["last_heartbeat_at"],
            last_update_at=values["last_update_at"],
            armed=values["armed"],
            flight_mode=values["flight_mode"],
            nav_state=values["nav_state"],
            in_air=values["in_air"],
            reconnect_attempts=int(values["reconnect_attempts"]),
            last_error=values["last_error"],
        )

    def _parse_dt(self, value: Any) -> datetime | None:
        if value is None or isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise TypeError(f"unsupported datetime value: {value!r}")

    def _normalise_nav_state(self, flight_mode: str) -> str:
        key = flight_mode.strip().lower()
        if "hold" in key or "loiter" in key:
            return "hold"
        if "takeoff" in key:
            return "takeoff"
        if "land" in key:
            return "land"
        if "mission" in key:
            return "mission"
        if "offboard" in key:
            return "offboard"
        if "position" in key or "posctl" in key:
            return "position"
        if "manual" in key:
            return "manual"
        if "fail" in key:
            return "failsafe"
        return key or "unknown"


class Px4CommandHandlers:
    def __init__(
        self,
        *,
        adapter: PersistentPx4CommandAdapter,
        event_log: RuntimeEventLog,
        vehicle_state_provider: Any | None = None,
        command_gate: Any | None = None,
        transition_tracker: Any | None = None,
        hold_reconciler: Any | None = None,
    ):
        self.adapter = adapter
        self.event_log = event_log
        self.vehicle_state_provider = vehicle_state_provider
        self.command_gate = command_gate
        self.transition_tracker = transition_tracker
        self.hold_reconciler = hold_reconciler

    def register(self, registry: DispatchRegistry) -> None:
        for command_id in PX4_COMMANDS:
            registry.register_action(
                command_id,
                self.handle,
                permission=HandlerPermission.FLIGHT_CRITICAL,
                transport="mavsdk",
                summary=f"PX4 command {command_id}",
            )

    def handle(self, request: CommandRequest) -> ActionStartResponse:
        self.event_log.record_command_request(
            command_id=request.command_id,
            request_id=request.request_id,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=True,
        )
        command = PX4_COMMANDS.get(request.command_id)
        if command is None:
            return self._reject(request, "unsupported PX4 command", ErrorCode.HANDLER_UNAVAILABLE)

        status = self.adapter.status()
        if not status.command_available:
            return self._reject(
                request,
                status.degraded_reason or "PX4 command transport is not available",
                ErrorCode.DEGRADED_STATE,
                details=status.as_dict(),
            )
        if self.command_gate is not None:
            rejection_reason = self.command_gate.rejection_reason(request.command_id)
            if rejection_reason is not None:
                return self._reject(
                    request,
                    rejection_reason,
                    ErrorCode.DEGRADED_STATE,
                    details={"control": self.command_gate.control_state().model_dump(mode="json")},
                )
        elif request.command_id in DANGEROUS_PX4_COMMANDS and self.vehicle_state_provider is not None:
            rejection_reason = self.vehicle_state_provider.dangerous_command_rejection_reason()
            if rejection_reason is not None:
                return self._reject(
                    request,
                    rejection_reason,
                    ErrorCode.DEGRADED_STATE,
                    details={"vehicle": self.vehicle_state_provider.state().model_dump(mode="json")},
                )

        try:
            telemetry = self.adapter.run_blocking(command)
        except Exception as exc:
            return self._reject(request, str(exc), ErrorCode.INTERNAL_ERROR)

        self.event_log.record_command_decision(
            command_id=request.command_id,
            request_id=request.request_id,
            accepted=True,
            reason=None,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=True,
        )
        transition = None
        if self.transition_tracker is not None and request.command_id in PX4_TRANSITION_TARGETS:
            transition = self.transition_tracker.start(
                command_id=request.command_id,
                request_id=request.request_id,
                target=PX4_TRANSITION_TARGETS[request.command_id],
            )
        if self.hold_reconciler is not None and request.command_id == CommandId.PX4_HOLD.value:
            self.hold_reconciler.record_hold(
                request_id=request.request_id,
                command_id=request.command_id,
            )
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=True,
            started=transition is not None,
            result={
                "transport": self.adapter.status().as_dict(),
                "telemetry": {
                    "armed": telemetry.armed,
                    "flight_mode": telemetry.flight_mode,
                    "nav_state": telemetry.nav_state,
                    "in_air": telemetry.in_air,
                },
                "transition": transition.as_dict() if transition else None,
            },
        )

    def _reject(
        self,
        request: CommandRequest,
        reason: str,
        code: ErrorCode,
        *,
        details: dict[str, Any] | None = None,
    ) -> ActionStartResponse:
        self.event_log.record_command_decision(
            command_id=request.command_id,
            request_id=request.request_id,
            accepted=False,
            reason=reason,
            source=EventSource.RUNTIME,
            client_label=request.client_label,
            mutating=True,
        )
        return ActionStartResponse(
            request_id=request.request_id,
            command_id=request.command_id,
            accepted=False,
            started=False,
            message=reason,
            rejection=CommandRejection(
                code=code,
                message=reason,
                request_id=request.request_id,
                command_id=request.command_id,
                details=details or {},
                retryable=True,
                degraded_reason=reason if code == ErrorCode.DEGRADED_STATE else None,
            ),
            result={"transport": self.adapter.status().as_dict()},
        )


def _run_async_blocking(awaitable: Awaitable[Px4CommandTelemetry]) -> Px4CommandTelemetry:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    raise RuntimeError("PX4 command dispatch cannot run inside an active event loop")


PX4_COMMANDS: dict[str, Callable[[PersistentPx4CommandAdapter], Awaitable[Px4CommandTelemetry]]] = {
    CommandId.PX4_ARM.value: lambda adapter: adapter.arm(),
    CommandId.PX4_TAKEOFF.value: lambda adapter: adapter.takeoff(),
    CommandId.PX4_LAND.value: lambda adapter: adapter.land(),
    CommandId.PX4_HOLD.value: lambda adapter: adapter.hold(),
}

DANGEROUS_PX4_COMMANDS = {
    CommandId.PX4_ARM.value,
    CommandId.PX4_TAKEOFF.value,
    CommandId.PX4_LAND.value,
}

PX4_TRANSITION_TARGETS = {
    CommandId.PX4_TAKEOFF.value: "px4_takeoff",
    CommandId.PX4_LAND.value: "px4_land",
    CommandId.PX4_HOLD.value: "px4_hold",
}


def register_px4_command_handlers(
    registry: DispatchRegistry,
    *,
    adapter: PersistentPx4CommandAdapter,
    event_log: RuntimeEventLog,
    vehicle_state_provider: Any | None = None,
    command_gate: Any | None = None,
    transition_tracker: Any | None = None,
    hold_reconciler: Any | None = None,
) -> Px4CommandHandlers:
    handlers = Px4CommandHandlers(
        adapter=adapter,
        event_log=event_log,
        vehicle_state_provider=vehicle_state_provider,
        command_gate=command_gate,
        transition_tracker=transition_tracker,
        hold_reconciler=hold_reconciler,
    )
    handlers.register(registry)
    return handlers
