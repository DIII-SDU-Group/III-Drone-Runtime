"""Runtime-side cache for typed mission mode status."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from iii_drone_contracts import InspectionStartEligibility, MissionDomainState, MissionIntentStatus, MissionModeRegistryEntry, MissionSpecificationIdentity
from iii_drone_contracts.envelopes import Freshness, SourceAvailability


MISSION_STATUS_TOPIC = "/mission/status"


class MissionStatusCache:
    def __init__(
        self,
        *,
        topic: str = MISSION_STATUS_TOPIC,
        stale_after: timedelta = timedelta(seconds=2),
        clock: Callable[[], datetime] | None = None,
    ):
        self.topic = topic
        self.stale_after = stale_after
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._latest_message = None
        self._last_received_at: datetime | None = None
        self._unavailable_reason = "mission status topic has not been received"
        self._system_running = False
        self._intent_effect_seen: set[str] = set()

    def set_system_running(self, running: bool) -> None:
        self._system_running = running

    def subscribe(self, node):
        try:
            from iii_drone_interfaces.msg import MissionModeStatus
            from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
        except Exception as exc:
            self._unavailable_reason = f"MissionModeStatus message unavailable: {exc}"
            return None
        qos = QoSProfile(depth=1)
        qos.reliability = ReliabilityPolicy.RELIABLE
        qos.durability = DurabilityPolicy.TRANSIENT_LOCAL
        return node.create_subscription(MissionModeStatus, self.topic, self.handle_message, qos)

    def handle_message(self, message) -> None:
        self._latest_message = message
        self._last_received_at = self._clock()
        self._unavailable_reason = None

    def mode_id(self, mode_key: str) -> int | None:
        message = self._latest_message
        if message is None or self._is_stale():
            return None
        for mode in getattr(message, "modes", []):
            if getattr(mode, "mode_key", "") != mode_key or not getattr(mode, "mode_id_valid", False):
                continue
            try:
                return int(getattr(mode, "mode_id"))
            except (AttributeError, TypeError, ValueError):
                return None
        return None

    def mission_mode_id(self) -> int | None:
        message = self._latest_message
        if message is None:
            return None
        return self.mode_id(getattr(message, "owned_mode", ""))

    def state(self) -> MissionDomainState:
        message = self._latest_message
        if message is None:
            return MissionDomainState(
                source_label="mission_status",
                freshness=Freshness.UNKNOWN,
                source_availability=SourceAvailability.UNAVAILABLE,
                degraded_reason=self._unavailable_reason,
                mission_state="unknown",
                required_modes_registered=False,
            )

        source_timestamp = _message_timestamp(message) or self._last_received_at
        stale = self._is_stale()
        freshness = Freshness.STALE if stale else Freshness.FRESH
        degraded_reasons = list(getattr(message, "degraded_reasons", []))
        if stale:
            degraded_reasons.append("mission mode registry is stale")
        modes = self._mode_registry(message, freshness)
        inspection_eligibility = self._inspection_start_eligibility(message, freshness)
        specification = self._specification_identity(message)
        intents = self._intent_statuses(message, modes)
        activation_rejections = []
        if not self._system_running:
            activation_rejections.append("system is not running")
        if stale:
            activation_rejections.append("mission mode registry is stale")
        if not getattr(message, "required_modes_registered", False):
            activation_rejections.append("required mission modes are not registered")
        if getattr(message, "degraded", False):
            activation_rejections.extend(degraded_reasons)
        if specification.canonical_loaded is False:
            activation_rejections.append("canonical inspection specification is not loaded")
        if specification.canonical_loaded is True and not specification.content_hash:
            activation_rejections.append("canonical inspection specification content hash is unavailable")
        if inspection_eligibility is not None and not inspection_eligibility.eligible:
            activation_rejections.extend(inspection_eligibility.failure_reasons)

        latest = {
            "active_mission_specification": getattr(message, "active_mission_specification", ""),
            "mission_active": getattr(message, "mission_active", False),
            "mission_state_label": getattr(message, "mission_state_label", "unknown"),
            "required_modes": list(getattr(message, "required_modes", [])),
            "registered_modes": list(getattr(message, "registered_modes", [])),
            "owned_mode": getattr(message, "owned_mode", ""),
            "mode_id": self.mission_mode_id(),
            "mode_registry_count": len(modes),
            "control_owner": getattr(message, "control_owner", ""),
            "ready": getattr(message, "ready", False),
            "degraded": getattr(message, "degraded", False),
            "degraded_reasons": degraded_reasons,
            "activation_allowed": not activation_rejections,
            "activation_rejections": activation_rejections,
            "inspection_start_eligibility": (
                inspection_eligibility.model_dump(mode="json")
                if inspection_eligibility is not None
                else None
            ),
            "specification": specification.model_dump(mode="json"),
            "intents": [intent.model_dump(mode="json") for intent in intents],
        }
        return MissionDomainState(
            source_label="mission_status",
            source_timestamp=source_timestamp,
            freshness=freshness,
            source_availability=(
                SourceAvailability.DEGRADED
                if stale or getattr(message, "degraded", False)
                else SourceAvailability.AVAILABLE
            ),
            degraded_reason="; ".join(degraded_reasons) if degraded_reasons else None,
            latest=latest,
            active_spec_id=getattr(message, "active_mission_specification", None) or None,
            mission_state=getattr(message, "mission_state_label", "unknown"),
            required_modes_registered=getattr(message, "required_modes_registered", False),
            modes=modes,
            inspection_start_eligibility=inspection_eligibility,
            specification=specification,
            intents=intents,
        )

    def _intent_statuses(self, message, modes: list[MissionModeRegistryEntry]) -> list[MissionIntentStatus]:
        active_mode = next((mode.mode_key for mode in modes if mode.active), "")
        successful_modes = {mode.mode_key for mode in modes if mode.tree_finished and mode.tree_success is True}
        labels = {
            "trigger_recharge_now": "Recharge now",
            "stay_on_cable": "Stay on cable",
            "interrupt_recharging_now": "Leave cable now",
        }
        statuses: list[MissionIntentStatus] = []
        for value in getattr(message, "intents", []):
            key = str(getattr(value, "intent_key", ""))
            if not key:
                continue
            lifecycle = str(getattr(value, "lifecycle", "cleared"))
            enabled = bool(getattr(value, "value", False))
            if lifecycle not in {"rejected", "timed_out"}:
                if not enabled:
                    lifecycle = "cleared"
                    self._intent_effect_seen.discard(key)
                elif key == "trigger_recharge_now" and active_mode in {"reach_cable", "cable_charging", "leave_cable"}:
                    lifecycle = "effect_active"
                    self._intent_effect_seen.add(key)
                elif key == "trigger_recharge_now" and active_mode == "inspection_demo" and (
                    key in self._intent_effect_seen or {"reach_cable", "cable_charging", "leave_cable"}.issubset(successful_modes)
                ):
                    lifecycle = "completed"
                elif key == "stay_on_cable" and active_mode == "cable_charging":
                    lifecycle = "effect_active"
                elif key == "stay_on_cable" and active_mode != "cable_charging" and "cable_charging" in successful_modes:
                    lifecycle = "completed"
                elif key == "interrupt_recharging_now" and active_mode == "leave_cable":
                    lifecycle = "effect_active"
                    self._intent_effect_seen.add(key)
                elif key == "interrupt_recharging_now" and active_mode == "inspection_demo" and (
                    key in self._intent_effect_seen or "leave_cable" in successful_modes
                ):
                    lifecycle = "completed"
                else:
                    lifecycle = "acknowledged_onboard"
            statuses.append(
                MissionIntentStatus(
                    intent_key=key,
                    label=labels.get(key, key.replace("_", " ").title()),
                    service_name=str(getattr(value, "service_name", "")),
                    flag_name=str(getattr(value, "flag_name", "")),
                    value=enabled,
                    sequence_id=int(getattr(value, "sequence_id", 0)),
                    lifecycle=lifecycle,
                    detail=str(getattr(value, "detail", "")) or None,
                    updated_at=_message_timestamp(value),
                )
            )
        return statuses

    def _specification_identity(self, message) -> MissionSpecificationIdentity:
        active_path = str(getattr(message, "active_mission_specification", "")) or None
        canonical_path = str(getattr(message, "canonical_mission_specification", "")) or None
        canonical_loaded = (
            bool(getattr(message, "canonical_mission_specification_loaded"))
            if hasattr(message, "canonical_mission_specification_loaded")
            else None
        )
        label_path = canonical_path or active_path
        return MissionSpecificationIdentity(
            active_path=active_path,
            canonical_path=canonical_path,
            label=label_path.rsplit("/", 1)[-1] if label_path else None,
            content_hash=str(getattr(message, "active_mission_specification_hash", "")) or None,
            canonical_loaded=canonical_loaded,
            configuration_profile=str(getattr(message, "configuration_profile", "unknown")) or "unknown",
            load_error=str(getattr(message, "mission_specification_load_error", "")) or None,
        )

    def _is_stale(self) -> bool:
        return self._last_received_at is None or self._clock() - self._last_received_at > self.stale_after

    def _mode_registry(self, message, freshness: Freshness) -> list[MissionModeRegistryEntry]:
        registry: list[MissionModeRegistryEntry] = []
        present_keys: set[str] = set()
        for mode in getattr(message, "modes", []):
            mode_key = str(getattr(mode, "mode_key", ""))
            if not mode_key:
                continue
            present_keys.add(mode_key)
            tree_finished = bool(getattr(mode, "tree_finished", False))
            degraded_reason = str(getattr(mode, "degraded_reason", "")) or None
            if freshness == Freshness.STALE:
                degraded_reason = _join_reasons(degraded_reason, "mission mode status is stale")
            registry.append(
                MissionModeRegistryEntry(
                    mode_key=mode_key,
                    display_name=str(getattr(mode, "display_name", "")) or mode_key,
                    mode_id=(int(getattr(mode, "mode_id")) if getattr(mode, "mode_id_valid", False) else None),
                    registered=bool(getattr(mode, "registered", False)),
                    active=bool(getattr(mode, "active", False)),
                    tree_running=bool(getattr(mode, "tree_running", False)),
                    tree_finished=tree_finished,
                    tree_success=(bool(getattr(mode, "tree_success", False)) if tree_finished else None),
                    source_timestamp=_message_timestamp(mode) or _message_timestamp(message) or self._last_received_at,
                    freshness=freshness,
                    degraded_reason=degraded_reason,
                )
            )

        for mode_key in getattr(message, "required_modes", []):
            if mode_key in present_keys:
                continue
            registry.append(
                MissionModeRegistryEntry(
                    mode_key=mode_key,
                    display_name=mode_key,
                    freshness=Freshness.UNKNOWN,
                    degraded_reason="typed mission mode status is missing",
                )
            )
        return registry

    def _inspection_start_eligibility(
        self,
        message,
        freshness: Freshness,
    ) -> InspectionStartEligibility | None:
        value = getattr(message, "inspection_start_eligibility", None)
        if value is None:
            return None
        reasons = [str(reason) for reason in getattr(value, "failure_reasons", [])]
        if freshness == Freshness.STALE:
            reasons.append("inspection start eligibility is stale")
        point = getattr(value, "ingress_point", None)
        side = str(getattr(value, "side", "unknown"))
        if side not in {"positive", "negative"}:
            side = "unknown"
        ingress_valid = bool(getattr(value, "ingress_point_valid", False))
        return InspectionStartEligibility(
            source_timestamp=_message_timestamp(value) or _message_timestamp(message) or self._last_received_at,
            evaluable=bool(getattr(value, "evaluable", False)),
            eligible=bool(getattr(value, "eligible", False)) and freshness == Freshness.FRESH,
            side=side,
            measured_lateral_clearance_m=_optional_float(value, "measured_lateral_clearance_m"),
            required_lateral_clearance_m=_optional_float(value, "required_lateral_clearance_m"),
            between_pylons=bool(getattr(value, "between_pylons", False)),
            distance_from_start_boundary_m=_optional_float(value, "distance_from_start_boundary_m"),
            distance_to_end_boundary_m=_optional_float(value, "distance_to_end_boundary_m"),
            pylon_span_margin_m=_optional_float(value, "pylon_span_margin_m"),
            ingress_point_valid=ingress_valid,
            ingress_x=_optional_float(point, "x") if ingress_valid else None,
            ingress_y=_optional_float(point, "y") if ingress_valid else None,
            ingress_z=_optional_float(point, "z") if ingress_valid else None,
            failure_reasons=reasons,
            freshness=freshness,
        )


def _message_timestamp(message) -> datetime | None:
    stamp = getattr(message, "stamp", None)
    if stamp is None:
        return None
    seconds = int(getattr(stamp, "sec", 0))
    nanoseconds = int(getattr(stamp, "nanosec", 0))
    if seconds == 0 and nanoseconds == 0:
        return None
    return datetime.fromtimestamp(seconds + nanoseconds / 1_000_000_000, tz=timezone.utc)


def _join_reasons(first: str | None, second: str) -> str:
    return f"{first}; {second}" if first else second


def _optional_float(value, field: str) -> float | None:
    if value is None:
        return None
    try:
        return float(getattr(value, field))
    except (AttributeError, TypeError, ValueError):
        return None
