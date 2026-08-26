"""Fail-closed runtime mutation gating."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class VehicleSafetyState:
    known: bool
    fresh: bool
    armed: bool | None = None
    in_air: bool | None = None
    degraded: bool = False
    reason: str | None = None


class RuntimeMutationGate:
    def __init__(
        self,
        state: VehicleSafetyState | None = None,
        *,
        clock_gate: Callable[[], str | None] | None = None,
    ):
        self.state = state or VehicleSafetyState(
            known=False, fresh=False, reason="vehicle state unknown"
        )
        self.clock_gate = clock_gate

    def update(self, state: VehicleSafetyState) -> None:
        self.state = state

    def rejection_reason(self, command_id: str) -> str | None:
        del command_id
        if self.clock_gate is not None:
            clock_reason = self.clock_gate()
            if clock_reason is not None:
                return clock_reason
        state = self.state
        if not state.known:
            return state.reason or "vehicle state unknown"
        if not state.fresh:
            return state.reason or "vehicle state stale"
        if state.degraded:
            return state.reason or "vehicle state degraded"
        if state.armed:
            return "vehicle is armed"
        if state.in_air:
            return "vehicle is in flight"
        return None


class ReceiverClockGate:
    """Read the receiver-owned gate without importing deployment internals."""

    def __init__(
        self,
        path: Path = Path("/var/lib/iii/deployment/clock-state.json"),
        boot_id_path: Path = Path("/proc/sys/kernel/random/boot_id"),
    ):
        self.path = path
        self.boot_id_path = boot_id_path

    def __call__(self) -> str | None:
        try:
            if self.path.is_symlink() or not self.path.is_file():
                return "DEGRADED_CLOCK: receiver clock gate is unavailable"
            value = json.loads(self.path.read_text(encoding="utf-8"))
            boot_id = self.boot_id_path.read_text(encoding="ascii").strip()
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return "DEGRADED_CLOCK: receiver clock gate is unreadable"
        if not isinstance(value, dict) or value.get("boot_id") != boot_id:
            return "DEGRADED_CLOCK: receiver clock gate belongs to another boot"
        gate = value.get("gate")
        if gate == "OPERATIONAL":
            return None
        if gate == "CLOCK_FAULT_ACTIVE":
            return "CLOCK_FAULT_ACTIVE: new runtime mutations are blocked"
        return "DEGRADED_CLOCK: synchronize the aircraft clock before runtime mutation"
