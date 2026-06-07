"""Runtime-side cache for typed mission mode status."""

from __future__ import annotations

from iii_drone_contracts import MissionDomainState
from iii_drone_contracts.envelopes import Freshness, SourceAvailability


MISSION_STATUS_TOPIC = "/mission/status"


class MissionStatusCache:
    def __init__(self, *, topic: str = MISSION_STATUS_TOPIC):
        self.topic = topic
        self._latest_message = None
        self._unavailable_reason = "mission status topic has not been received"
        self._system_running = False

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
        self._unavailable_reason = None

    def mission_mode_id(self) -> int | None:
        message = self._latest_message
        if message is None:
            return None
        for key in ("mode_id", "owned_mode_id", "mission_mode_id"):
            try:
                return int(getattr(message, key))
            except (AttributeError, TypeError, ValueError):
                continue
        return None

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

        degraded_reasons = list(getattr(message, "degraded_reasons", []))
        activation_rejections = []
        if not self._system_running:
            activation_rejections.append("system is not running")
        if not getattr(message, "required_modes_registered", False):
            activation_rejections.append("required mission modes are not registered")
        if getattr(message, "degraded", False):
            activation_rejections.extend(degraded_reasons)

        latest = {
            "active_mission_specification": getattr(message, "active_mission_specification", ""),
            "mission_active": getattr(message, "mission_active", False),
            "mission_state_label": getattr(message, "mission_state_label", "unknown"),
            "required_modes": list(getattr(message, "required_modes", [])),
            "registered_modes": list(getattr(message, "registered_modes", [])),
            "owned_mode": getattr(message, "owned_mode", ""),
            "mode_id": self.mission_mode_id(),
            "control_owner": getattr(message, "control_owner", ""),
            "ready": getattr(message, "ready", False),
            "degraded": getattr(message, "degraded", False),
            "degraded_reasons": degraded_reasons,
            "activation_allowed": not activation_rejections,
            "activation_rejections": activation_rejections,
        }
        return MissionDomainState(
            source_label="mission_status",
            freshness=Freshness.FRESH,
            source_availability=SourceAvailability.AVAILABLE,
            degraded_reason="; ".join(degraded_reasons) if degraded_reasons else None,
            latest=latest,
            active_spec_id=getattr(message, "active_mission_specification", None) or None,
            mission_state=getattr(message, "mission_state_label", "unknown"),
            required_modes_registered=getattr(message, "required_modes_registered", False),
        )
