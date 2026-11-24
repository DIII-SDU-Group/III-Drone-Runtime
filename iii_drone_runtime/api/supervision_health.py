"""Runtime-side cache for typed supervision aggregate health."""

from __future__ import annotations

from iii_drone_contracts import SystemDomainState
from iii_drone_contracts.envelopes import Freshness, SourceAvailability
from rclpy.qos import DurabilityPolicy, QoSProfile

from ..ros_sampling import create_batched_subscription


SUPERVISION_HEALTH_TOPIC = "/supervision/system_health"
# Supervision reports one subsystem per service and supervised process; an
# operator subsystem aggregates the ones present in the current profile (an
# entry with the operator id itself also counts).
OPERATOR_SUBSYSTEM_MEMBERS = {
    "perception": ("perception", "hough_transformer", "pl_dir_computer", "pl_mapper", "mmwave", "cable_camera"),
    "control": ("control", "maneuver_controller", "trajectory_generator", "tf", "micro_ros_agent"),
    "mission": (
        "mission",
        "mission_executor",
        "powerline_overview_provider",
        "pylon_overview_provider",
        "rosbag_recorder",
        "custom_operation",
    ),
    "payload": ("payload", "charger_gripper"),
    "configuration": ("configuration", "configuration_server"),
    "supervision": ("supervision",),
}
REQUIRED_OPERATOR_SUBSYSTEMS = tuple(OPERATOR_SUBSYSTEM_MEMBERS)
# SubsystemHealthStatus codes, by severity.
STATUS_UNKNOWN, STATUS_OK, STATUS_DEGRADED, STATUS_UNAVAILABLE, STATUS_ERROR = range(5)


def supervision_health_qos() -> QoSProfile:
    """Request the daemon's retained current-health sample on late join."""
    qos = QoSProfile(depth=1)
    qos.durability = DurabilityPolicy.TRANSIENT_LOCAL
    return qos


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
        self._subscription = create_batched_subscription(
            node,
            SystemHealthStatus,
            self.topic,
            self.handle_message,
            supervision_health_qos(),
        )
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
            members = [
                by_id[member]
                for member in OPERATOR_SUBSYSTEM_MEMBERS.get(subsystem_id, (subsystem_id,))
                if member in by_id
            ]
            if not members and subsystem_id == "supervision" and message is not None:
                # The daemon publishing this message is the supervision subsystem.
                daemon_ready = bool(getattr(message, "daemon_ready", False))
                members = [{
                    "subsystem_id": "supervision",
                    "label": "supervision",
                    "status": STATUS_OK if daemon_ready else STATUS_UNAVAILABLE,
                    "ready": daemon_ready,
                    "degraded": not daemon_ready,
                    "reason": "" if daemon_ready else "system daemon is not ready",
                    "degraded_reasons": [] if daemon_ready else ["system daemon is not ready"],
                    "owner": "supervision",
                    "source_availability": "available",
                }]
            if len(members) == 1 and members[0]["subsystem_id"] == subsystem_id:
                rows.append(members[0])
                continue
            if members:
                ready = all(member["ready"] for member in members)
                degraded = not ready or any(member["degraded"] for member in members)
                reasons = [
                    f"{member['subsystem_id']}: {reason}"
                    for member in members
                    for reason in (member["degraded_reasons"] or ([member["reason"]] if member["degraded"] and member["reason"] else []))
                ]
                rows.append(
                    {
                        "subsystem_id": subsystem_id,
                        "label": subsystem_id,
                        "status": max(member["status"] for member in members) if degraded else STATUS_OK,
                        "ready": ready,
                        "degraded": degraded,
                        "reason": "; ".join(reasons),
                        "degraded_reasons": reasons,
                        "owner": "supervision",
                        "source_availability": "available",
                        "members": [member["subsystem_id"] for member in members],
                    }
                )
                continue
            if message is not None:
                # Supervision is reporting but runs no process of this
                # subsystem in this profile (HIL simulates the payload on the
                # workstation): not supervised here, not degraded.
                profile = getattr(message, "profile", "unknown")
                rows.append(
                    {
                        "subsystem_id": subsystem_id,
                        "label": subsystem_id,
                        "status": STATUS_UNKNOWN,
                        "ready": False,
                        "degraded": False,
                        "reason": f"no {subsystem_id} process is supervised in profile {profile}",
                        "degraded_reasons": [],
                        "owner": "supervision",
                        "source_availability": "not_supervised",
                    }
                )
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
