"""Fail-closed runtime mutation gating."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VehicleSafetyState:
    known: bool
    fresh: bool
    armed: bool | None = None
    in_air: bool | None = None
    degraded: bool = False
    reason: str | None = None


class RuntimeMutationGate:
    def __init__(self, state: VehicleSafetyState | None = None):
        self.state = state or VehicleSafetyState(known=False, fresh=False, reason="vehicle state unknown")

    def update(self, state: VehicleSafetyState) -> None:
        self.state = state

    def rejection_reason(self, command_id: str) -> str | None:
        del command_id
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
