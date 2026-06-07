"""Fused PX4 vehicle state from MAVSDK and ROS/uXRCE sources."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import threading
from typing import Any

from iii_drone_contracts import SourceAvailability, VehicleDomainState

from .px4_adapter import PersistentPx4CommandAdapter, Px4CommandTransportStatus


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalise_nav_state(value: str | None) -> str | None:
    if value is None:
        return None
    key = value.strip().lower()
    if not key or key == "unknown":
        return None
    if "loiter" in key or "hold" in key:
        return "hold"
    if "takeoff" in key:
        return "takeoff"
    if "land" in key:
        return "land"
    if "mission" in key or "auto_mission" in key:
        return "mission"
    if "offboard" in key:
        return "offboard"
    if "posctl" in key or "position" in key:
        return "position"
    if "manual" in key:
        return "manual"
    if "fail" in key:
        return "failsafe"
    return key


@dataclass(frozen=True)
class RosPx4BridgeStatus:
    available: bool
    source_availability: str
    degraded_reason: str | None = None
    last_vehicle_status_at: datetime | None = None
    last_land_detected_at: datetime | None = None
    armed: bool | None = None
    in_air: bool | None = None
    nav_state: str | None = None
    nav_state_id: int | None = None
    failsafe: bool | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "source_availability": self.source_availability,
            "degraded_reason": self.degraded_reason,
            "last_vehicle_status_at": self.last_vehicle_status_at.isoformat() if self.last_vehicle_status_at else None,
            "last_land_detected_at": self.last_land_detected_at.isoformat() if self.last_land_detected_at else None,
            "armed": self.armed,
            "in_air": self.in_air,
            "nav_state": self.nav_state,
            "nav_state_id": self.nav_state_id,
            "failsafe": self.failsafe,
            "raw": self.raw,
        }


class RosPx4StateCache:
    def __init__(self, *, stale_after_seconds: float = 3.0):
        self.stale_after_seconds = stale_after_seconds
        self._lock = threading.RLock()
        self._last_vehicle_status_at: datetime | None = None
        self._last_land_detected_at: datetime | None = None
        self._armed: bool | None = None
        self._in_air: bool | None = None
        self._nav_state: str | None = None
        self._nav_state_id: int | None = None
        self._failsafe: bool | None = None
        self._raw: dict[str, Any] = {}

    def subscribe(self, node: Any) -> list[Any]:
        try:
            from px4_msgs.msg import VehicleLandDetected, VehicleStatus
            from rclpy.qos import qos_profile_sensor_data
        except Exception:
            return []
        return [
            node.create_subscription(
                VehicleStatus,
                "/fmu/out/vehicle_status_v1",
                self.handle_vehicle_status_message,
                qos_profile_sensor_data,
            ),
            node.create_subscription(
                VehicleLandDetected,
                "/fmu/out/vehicle_land_detected",
                self.handle_vehicle_land_detected_message,
                qos_profile_sensor_data,
            ),
        ]

    def handle_vehicle_status_message(self, message: Any) -> None:
        now = _utc_now()
        arming_state = _optional_int(message, "arming_state")
        nav_state_id = _optional_int(message, "nav_state")
        armed = None
        if arming_state is not None:
            armed_value = int(getattr(message, "ARMING_STATE_ARMED", 2))
            armed = arming_state == armed_value
        failsafe = _optional_bool(message, "failsafe")
        nav_state = _nav_state_label(message, nav_state_id)

        with self._lock:
            self._last_vehicle_status_at = now
            self._armed = armed
            self._nav_state = nav_state
            self._nav_state_id = nav_state_id
            self._failsafe = failsafe
            self._raw["vehicle_status"] = _message_fields(
                message,
                ["arming_state", "nav_state", "failsafe", "nav_state_user_intention"],
            )

    def handle_vehicle_land_detected_message(self, message: Any) -> None:
        landed = _optional_bool(message, "landed")
        maybe_in_air = None if landed is None else not landed
        with self._lock:
            self._last_land_detected_at = _utc_now()
            self._in_air = maybe_in_air
            self._raw["vehicle_land_detected"] = _message_fields(message, ["landed", "freefall"])

    def status(self) -> RosPx4BridgeStatus:
        with self._lock:
            last_vehicle_status_at = self._last_vehicle_status_at
            last_land_detected_at = self._last_land_detected_at
            status = RosPx4BridgeStatus(
                available=last_vehicle_status_at is not None,
                source_availability=SourceAvailability.AVAILABLE.value,
                last_vehicle_status_at=last_vehicle_status_at,
                last_land_detected_at=last_land_detected_at,
                armed=self._armed,
                in_air=self._in_air,
                nav_state=self._nav_state,
                nav_state_id=self._nav_state_id,
                failsafe=self._failsafe,
                raw=dict(self._raw),
            )

        if last_vehicle_status_at is None:
            return RosPx4BridgeStatus(
                available=False,
                source_availability=SourceAvailability.UNAVAILABLE.value,
                degraded_reason="PX4 ROS/uXRCE vehicle status has not been received",
            )
        if (_utc_now() - last_vehicle_status_at).total_seconds() > self.stale_after_seconds:
            return RosPx4BridgeStatus(
                available=False,
                source_availability=SourceAvailability.DEGRADED.value,
                degraded_reason="PX4 ROS/uXRCE vehicle status is stale",
                last_vehicle_status_at=last_vehicle_status_at,
                last_land_detected_at=last_land_detected_at,
                armed=status.armed,
                in_air=status.in_air,
                nav_state=status.nav_state,
                nav_state_id=status.nav_state_id,
                failsafe=status.failsafe,
                raw=status.raw,
            )
        return status


class FusedPx4StateProvider:
    def __init__(
        self,
        *,
        command_adapter: PersistentPx4CommandAdapter,
        ros_state: RosPx4StateCache | None = None,
        mode_label_provider: Callable[[int], str | None] | None = None,
    ):
        self.command_adapter = command_adapter
        self.ros_state = ros_state or RosPx4StateCache()
        self.mode_label_provider = mode_label_provider

    def state(self) -> VehicleDomainState:
        command = self.command_adapter.status()
        ros = self._with_registered_mode_label(self.ros_state.status())
        disagreements = _disagreements(command, ros)
        degraded_reasons = _degraded_reasons(command, ros, disagreements)

        source_availability = SourceAvailability.AVAILABLE.value
        if degraded_reasons:
            source_availability = SourceAvailability.DEGRADED.value
        if not command.enabled and not ros.available:
            source_availability = SourceAvailability.UNAVAILABLE.value

        return VehicleDomainState(
            source_label="px4_fusion",
            source_timestamp=_latest_timestamp(command.last_update_at, ros.last_vehicle_status_at, ros.last_land_detected_at),
            freshness="fresh" if not degraded_reasons else "stale",
            source_availability=source_availability,
            degraded_reason="; ".join(degraded_reasons) if degraded_reasons else None,
            latest={
                "command_transport": command.as_dict(),
                "ros_uxrce": ros.as_dict(),
                "disagreements": disagreements,
                "dangerous_commands_allowed": len(degraded_reasons) == 0,
            },
            armed=ros.armed if ros.armed is not None else command.armed,
            in_air=ros.in_air if ros.in_air is not None else command.in_air,
            nav_state=ros.nav_state if ros.nav_state is not None else command.nav_state,
            flight_mode=command.flight_mode,
            failsafe=ros.failsafe,
        )

    def dangerous_command_rejection_reason(self) -> str | None:
        state = self.state()
        if state.degraded_reason:
            return state.degraded_reason
        if state.armed is None or state.in_air is None or state.nav_state is None:
            return "PX4 fused safety state is incomplete"
        if state.failsafe is True:
            return "PX4 reports failsafe active"
        return None

    def _with_registered_mode_label(self, ros: RosPx4BridgeStatus) -> RosPx4BridgeStatus:
        if self.mode_label_provider is None or ros.nav_state_id is None:
            return ros
        label = self.mode_label_provider(ros.nav_state_id)
        if label is None or label == ros.nav_state:
            return ros
        raw = dict(ros.raw)
        raw["registered_nav_state_label"] = label
        return replace(ros, nav_state=label, raw=raw)


def _disagreements(command: Px4CommandTransportStatus, ros: RosPx4BridgeStatus) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    _append_disagreement(rows, "armed", command.armed, ros.armed)
    _append_disagreement(rows, "in_air", command.in_air, ros.in_air)
    _append_disagreement(rows, "nav_state", _normalise_nav_state(command.nav_state), _normalise_nav_state(ros.nav_state))
    return rows


def _append_disagreement(rows: list[dict[str, Any]], field_name: str, command_value: Any, ros_value: Any) -> None:
    if command_value is None or ros_value is None:
        return
    if command_value != ros_value:
        rows.append({"field": field_name, "mavsdk": command_value, "ros_uxrce": ros_value})


def _degraded_reasons(
    command: Px4CommandTransportStatus,
    ros: RosPx4BridgeStatus,
    disagreements: list[dict[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    command_state_complete = command.armed is not None and command.in_air is not None and command.nav_state is not None
    if not command.command_available:
        reasons.append(command.degraded_reason or "PX4 MAVSDK command transport is not available")
    if not ros.available and not command_state_complete:
        reasons.append(ros.degraded_reason or "PX4 ROS/uXRCE bridge is not available")
    if disagreements:
        fields = ", ".join(row["field"] for row in disagreements)
        reasons.append(f"PX4 MAVSDK and ROS/uXRCE disagree on safety fields: {fields}")
    if ros.failsafe is True:
        reasons.append("PX4 ROS/uXRCE reports failsafe active")
    return reasons


def _latest_timestamp(*values: datetime | None) -> datetime | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return max(present)


def _optional_int(message: Any, field_name: str) -> int | None:
    value = getattr(message, field_name, None)
    if value is None:
        return None
    return int(value)


def _optional_bool(message: Any, field_name: str) -> bool | None:
    value = getattr(message, field_name, None)
    if value is None:
        return None
    return bool(value)


def _message_fields(message: Any, field_names: list[str]) -> dict[str, Any]:
    return {field_name: getattr(message, field_name) for field_name in field_names if hasattr(message, field_name)}


def _nav_state_label(message: Any, nav_state_id: int | None) -> str | None:
    if nav_state_id is None:
        return None
    mapping = {
        int(getattr(message, "NAVIGATION_STATE_MANUAL", -1)): "manual",
        int(getattr(message, "NAVIGATION_STATE_POSCTL", -1)): "position",
        int(getattr(message, "NAVIGATION_STATE_AUTO_LOITER", -1)): "hold",
        int(getattr(message, "NAVIGATION_STATE_AUTO_TAKEOFF", -1)): "takeoff",
        int(getattr(message, "NAVIGATION_STATE_AUTO_LAND", -1)): "land",
        int(getattr(message, "NAVIGATION_STATE_AUTO_MISSION", -1)): "mission",
        int(getattr(message, "NAVIGATION_STATE_OFFBOARD", -1)): "offboard",
    }
    if nav_state_id in mapping:
        return mapping[nav_state_id]
    return f"nav_state_{nav_state_id}"
