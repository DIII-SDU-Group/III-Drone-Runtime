"""Physical vehicle-state safeguards for runtime mutations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class VehicleSafetyState:
    known: bool
    fresh: bool
    armed: bool | None = None
    in_air: bool | None = None
    degraded: bool = False
    reason: str | None = None


def vehicle_safety_state(vehicle: Any) -> VehicleSafetyState:
    """Judge runtime-mutation safety from the fused PX4 vehicle state.

    The armed and in-air evidence decides: each must be known and fresh, and
    the MAVSDK and ROS/uXRCE sources must agree on it. A degraded command
    transport alone does not hide a disarmed, landed aircraft. Without
    per-field evidence the fused state's own freshness decides.
    """
    fields = getattr(vehicle, "telemetry_fields", None) or {}
    armed_evidence = fields.get("armed")
    in_air_evidence = fields.get("in_air")
    armed = getattr(vehicle, "armed", None)
    in_air = getattr(vehicle, "in_air", None)
    if armed_evidence is None or in_air_evidence is None:
        available = str(getattr(vehicle, "source_availability", "unknown"))
        known = available != "unavailable" and armed is not None and in_air is not None
        fresh = str(getattr(vehicle, "freshness", "unknown")) == "fresh"
        return VehicleSafetyState(
            known=known,
            fresh=fresh,
            armed=armed,
            in_air=in_air,
            degraded=available == "degraded",
            reason=_reason(known, fresh, getattr(vehicle, "degraded_reason", None)),
        )
    evidence = (armed_evidence, in_air_evidence)
    known = all(
        item.value is not None and str(item.source_availability) != "unavailable"
        for item in evidence
    )
    fresh = all(str(item.freshness) == "fresh" for item in evidence)
    degraded = any(item.disagreement for item in evidence)
    detail = "; ".join(item.detail for item in evidence if item.detail)
    return VehicleSafetyState(
        known=known,
        fresh=fresh,
        armed=armed_evidence.value,
        in_air=in_air_evidence.value,
        degraded=degraded,
        reason=(
            f"vehicle armed/in-air sources disagree: {detail}"
            if known and fresh and degraded
            else _reason(known, fresh, detail or getattr(vehicle, "degraded_reason", None))
        ),
    )


def _reason(known: bool, fresh: bool, detail: str | None) -> str | None:
    if not known:
        return f"vehicle state unknown: {detail}" if detail else "vehicle state unknown"
    if not fresh:
        return f"vehicle state stale: {detail}" if detail else "vehicle state stale"
    return None


class RuntimeMutationGate:
    _VIRTUAL_PROFILES = {"hil", "sim"}

    def __init__(
        self,
        state: VehicleSafetyState | None = None,
        *,
        profile: str | None = None,
        state_provider: Callable[[], VehicleSafetyState] | None = None,
    ):
        self.profile = profile
        # A provider reads the live vehicle state for every decision.
        self.state_provider = state_provider
        self.state = state or VehicleSafetyState(
            known=False, fresh=False, reason="vehicle state unknown"
        )

    def update(self, state: VehicleSafetyState) -> None:
        self.state = state

    def rejection_reason(self, command_id: str) -> str | None:
        if self.profile in self._VIRTUAL_PROFILES:
            return None
        state = self.state_provider() if self.state_provider is not None else self.state
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
