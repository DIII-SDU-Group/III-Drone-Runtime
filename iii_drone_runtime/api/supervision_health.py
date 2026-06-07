"""Runtime-side cache for typed supervision aggregate health."""

from __future__ import annotations

from iii_drone_contracts import SystemDomainState
from iii_drone_contracts.envelopes import Freshness, SourceAvailability


SUPERVISION_HEALTH_TOPIC = "/supervision/system_health"
REQUIRED_OPERATOR_SUBSYSTEMS = (
    "perception",
    "control",
    "mission",
    "payload",
    "configuration",
    "supervision",
)


class SupervisionHealthCache:
    def __init__(self, *, topic: str = SUPERVISION_HEALTH_TOPIC):
        self.topic = topic
        self._latest_message = None
        self._subscription = None
        self._unavailable_reason = "supervision health topic has not been received"

    def subscribe(self, node):
        try:
            from iii_drone_interfaces.msg import SystemHealthStatus
        except Exception as exc:
            self._unavailable_reason = f"SystemHealthStatus message unavailable: {exc}"
            return None
        self._subscription = node.create_subscription(SystemHealthStatus, self.topic, self.handle_message, 10)
        return self._subscription

    def handle_message(self, message) -> None:
        self._latest_message = message
        self._unavailable_reason = None

    def state(self) -> SystemDomainState:
        message = self._latest_message
        if message is None:
            return SystemDomainState(
                source_label="supervision_health",
                freshness=Freshness.UNKNOWN,
                source_availability=SourceAvailability.UNAVAILABLE,
                degraded_reason=self._unavailable_reason,
                api_state="up",
                daemon_state="unknown",
                booted=None,
                active=None,
            )

        degraded_reasons = list(getattr(message, "degraded_reasons", []))
        latest = {
            "profile": getattr(message, "profile", "unknown"),
            "system_state": getattr(message, "system_state", 0),
            "ready": getattr(message, "ready", False),
            "degraded": getattr(message, "degraded", False),
            "degraded_reasons": degraded_reasons,
            "managed_node_count": getattr(message, "managed_node_count", 0),
            "active_managed_node_count": getattr(message, "active_managed_node_count", 0),
            "service_count": getattr(message, "service_count", 0),
            "ready_service_count": getattr(message, "ready_service_count", 0),
            "subsystems": [
                {
                    "subsystem_id": getattr(subsystem, "subsystem_id", ""),
                    "label": getattr(subsystem, "label", ""),
                    "status": getattr(subsystem, "status", 0),
                    "ready": getattr(subsystem, "ready", False),
                    "degraded": getattr(subsystem, "degraded", False),
                    "reason": getattr(subsystem, "reason", ""),
                    "degraded_reasons": list(getattr(subsystem, "degraded_reasons", [])),
                    "owner": getattr(subsystem, "owner", ""),
                }
                for subsystem in getattr(message, "subsystems", [])
            ],
        }
        return SystemDomainState(
            source_label="supervision_health",
            freshness=Freshness.FRESH,
            source_availability=SourceAvailability.AVAILABLE,
            degraded_reason="; ".join(degraded_reasons) if degraded_reasons else None,
            latest=latest,
            api_state="up",
            daemon_state="ready" if getattr(message, "daemon_ready", False) else "unavailable",
            booted=getattr(message, "runtime_booted", None),
            active=getattr(message, "system_active", None),
        )

    def subsystem_health(self, required_subsystems: tuple[str, ...] = REQUIRED_OPERATOR_SUBSYSTEMS) -> list[dict]:
        message = self._latest_message
        by_id: dict[str, dict] = {}
        if message is not None:
            for subsystem in getattr(message, "subsystems", []):
                subsystem_id = getattr(subsystem, "subsystem_id", "")
                if not subsystem_id:
                    continue
                by_id[subsystem_id] = {
                    "subsystem_id": subsystem_id,
                    "label": getattr(subsystem, "label", subsystem_id),
                    "status": getattr(subsystem, "status", 0),
                    "ready": getattr(subsystem, "ready", False),
                    "degraded": getattr(subsystem, "degraded", False),
                    "reason": getattr(subsystem, "reason", ""),
                    "degraded_reasons": list(getattr(subsystem, "degraded_reasons", [])),
                    "owner": getattr(subsystem, "owner", ""),
                    "source_availability": "available",
                }

        rows = []
        for subsystem_id in required_subsystems:
            if subsystem_id in by_id:
                rows.append(by_id[subsystem_id])
                continue
            rows.append(
                {
                    "subsystem_id": subsystem_id,
                    "label": subsystem_id,
                    "status": "missing",
                    "ready": False,
                    "degraded": True,
                    "reason": f"{subsystem_id} health status has not been received",
                    "degraded_reasons": [f"{subsystem_id} health status has not been received"],
                    "owner": "runtime_api",
                    "source_availability": "unavailable",
                }
            )
        return rows
