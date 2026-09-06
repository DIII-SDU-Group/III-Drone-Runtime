"""Fused PX4 vehicle state from MAVSDK and ROS/uXRCE sources."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import threading
from typing import Any

from iii_drone_contracts import SourceAvailability, TelemetryFieldState, VehicleDomainState

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
    telemetry: dict[str, Any] = field(default_factory=dict)

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
            "telemetry": self.telemetry,
        }


class HilSimBatteryChargeRelay:
    """Relay workstation HIL charge commands through the Pi-local XRCE writer.

    The uXRCE-DDS agent forwards local ROS writers to PX4, but does not bridge a
    writer discovered on another DDS host. Keeping the transport topic separate
    also ensures this workaround cannot affect real or OptiTrack profiles.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        input_topic: str = "/hil/sim_battery_charge",
        output_topic: str = "/fmu/in/sim_battery_charge",
    ):
        self.enabled = enabled
        self.input_topic = input_topic
        self.output_topic = output_topic
        self._publisher: Any | None = None
        self._subscription: Any | None = None

    def subscribe(self, node: Any) -> list[Any]:
        if not self.enabled:
            return []
        try:
            from px4_msgs.msg import SimBatteryCharge
            from rclpy.qos import qos_profile_sensor_data
        except Exception:
            return []

        self._publisher = node.create_publisher(
            SimBatteryCharge,
            self.output_topic,
            qos_profile_sensor_data,
        )
        self._subscription = node.create_subscription(
            SimBatteryCharge,
            self.input_topic,
            self._publisher.publish,
            qos_profile_sensor_data,
        )
        return [self._publisher, self._subscription]


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
        self._telemetry: dict[str, Any] = {}
        self._telemetry_at: dict[str, datetime] = {}

    def subscribe(self, node: Any) -> list[Any]:
        try:
            from px4_msgs.msg import (
                BatteryStatus,
                EstimatorStatus,
                FailsafeFlags,
                HomePosition,
                ManualControlSetpoint,
                SensorGps,
                VehicleGlobalPosition,
                VehicleLandDetected,
                VehicleLocalPosition,
                VehicleStatus,
            )
            from rclpy.qos import qos_profile_sensor_data
        except Exception:
            return []
        subscriptions = [
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
        for message_type, topic, callback in (
            (SensorGps, "/fmu/out/vehicle_gps_position", self.handle_gps_message),
            (VehicleGlobalPosition, "/fmu/out/vehicle_global_position", self.handle_global_position_message),
            (VehicleLocalPosition, "/fmu/out/vehicle_local_position", self.handle_local_position_message),
            (HomePosition, "/fmu/out/home_position", self.handle_home_position_message),
            (EstimatorStatus, "/fmu/out/estimator_status", self.handle_estimator_status_message),
            (FailsafeFlags, "/fmu/out/failsafe_flags", self.handle_failsafe_flags_message),
            (ManualControlSetpoint, "/fmu/out/manual_control_setpoint", self.handle_manual_control_message),
            (BatteryStatus, "/fmu/out/battery_status", self.handle_battery_status_message),
        ):
            subscriptions.append(node.create_subscription(message_type, topic, callback, qos_profile_sensor_data))
        return subscriptions

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
                [
                    "arming_state",
                    "nav_state",
                    "failsafe",
                    "nav_state_user_intention",
                    "pre_flight_checks_pass",
                    "valid_nav_states_mask",
                    "can_set_nav_states_mask",
                ],
            )
            self._telemetry["arming_checks_passed"] = _optional_bool(message, "pre_flight_checks_pass")
            self._telemetry_at["vehicle_status"] = now

    def handle_vehicle_land_detected_message(self, message: Any) -> None:
        landed = _optional_bool(message, "landed")
        maybe_in_air = None if landed is None else not landed
        with self._lock:
            self._last_land_detected_at = _utc_now()
            self._in_air = maybe_in_air
            self._raw["vehicle_land_detected"] = _message_fields(message, ["landed", "freefall"])

    def _record(self, source: str, values: dict[str, Any]) -> None:
        with self._lock:
            self._telemetry.update(values)
            self._telemetry_at[source] = _utc_now()

    def handle_gps_message(self, message: Any) -> None:
        self._record(
            "gps",
            {
                "gps_fix_type": _optional_int(message, "fix_type"),
                "satellites_used": _optional_int(message, "satellites_used"),
                "horizontal_accuracy_m": _optional_float(message, "eph"),
                "vertical_accuracy_m": _optional_float(message, "epv"),
            },
        )

    def handle_global_position_message(self, message: Any) -> None:
        self._record("global_position", {"global_position_valid": _finite_fields(message, "lat", "lon", "alt")})

    def handle_local_position_message(self, message: Any) -> None:
        valid = _optional_bool(message, "xy_valid")
        z_valid = _optional_bool(message, "z_valid")
        self._record("local_position", {"local_position_valid": bool(valid and z_valid) if valid is not None and z_valid is not None else None})

    def handle_home_position_message(self, message: Any) -> None:
        self._record("home_position", {"home_position_valid": _finite_fields(message, "lat", "lon", "alt")})

    def handle_estimator_status_message(self, message: Any) -> None:
        gps_fail = _optional_int(message, "gps_check_fail_flags")
        filter_fault = _optional_int(message, "filter_fault_flags")
        healthy = None if gps_fail is None or filter_fault is None else gps_fail == 0 and filter_fault == 0
        self._record(
            "estimator",
            {
                "estimator_healthy": healthy,
                "gps_check_fail_flags": gps_fail,
                "filter_fault_flags": filter_fault,
            },
        )

    def handle_failsafe_flags_message(self, message: Any) -> None:
        fields = [
            name
            for name in getattr(message, "__slots__", [])
            if name.startswith("_") and (name.endswith("_invalid") or "failure" in name or "lost" in name)
        ]
        flags = {name.lstrip("_"): bool(getattr(message, name)) for name in fields}
        values: dict[str, Any] = {"failsafe_flags": flags}
        if "home_position_invalid" in flags:
            # HomePosition is event-driven and is not replayed to a Runtime that starts later.
            # FailsafeFlags is continuous and carries PX4's authoritative current validity.
            values["home_position_valid"] = not flags["home_position_invalid"]
        self._record("failsafe_flags", values)

    def handle_manual_control_message(self, message: Any) -> None:
        valid = _optional_bool(message, "valid")
        self._record("manual_control", {"rc_link_available": True if valid is None else valid})

    def handle_battery_status_message(self, message: Any) -> None:
        remaining = _optional_float(message, "remaining")
        voltage = _optional_float(message, "voltage_v")
        current = _optional_float(message, "current_a")
        self._record(
            "battery",
            {
                "battery_remaining": remaining,
                "battery_voltage_v": voltage,
                "battery_current_a": current,
                "battery_power_w": voltage * current if voltage is not None and current is not None else None,
                "battery_warning": _optional_int(message, "warning"),
            },
        )

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
                telemetry={**self._telemetry, "source_timestamps": {key: value.isoformat() for key, value in self._telemetry_at.items()}},
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
                telemetry=status.telemetry,
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

        telemetry = ros.telemetry
        values = {
            "armed": ros.armed if ros.armed is not None else command.armed,
            "in_air": ros.in_air if ros.in_air is not None else command.in_air,
            "nav_state": ros.nav_state if ros.nav_state is not None else command.nav_state,
            "failsafe": ros.failsafe,
            **{key: telemetry.get(key) for key in _TELEMETRY_SOURCES},
        }
        if values["arming_checks_passed"] is None:
            values["arming_checks_passed"] = command.arming_checks_passed
        telemetry_fields = _field_evidence(
            values=values,
            telemetry=telemetry,
            command=command,
            ros=ros,
            disagreements=disagreements,
            stale_after_seconds=self.ros_state.stale_after_seconds,
        )
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
            telemetry_fields=telemetry_fields,
            armed=values["armed"],
            in_air=values["in_air"],
            nav_state=values["nav_state"],
            flight_mode=command.flight_mode,
            failsafe=ros.failsafe,
            gps_fix_type=telemetry.get("gps_fix_type"),
            satellites_used=telemetry.get("satellites_used"),
            horizontal_accuracy_m=telemetry.get("horizontal_accuracy_m"),
            vertical_accuracy_m=telemetry.get("vertical_accuracy_m"),
            local_position_valid=telemetry.get("local_position_valid"),
            global_position_valid=telemetry.get("global_position_valid"),
            home_position_valid=telemetry.get("home_position_valid"),
            estimator_healthy=telemetry.get("estimator_healthy"),
            arming_checks_passed=values["arming_checks_passed"],
            rc_link_available=telemetry.get("rc_link_available"),
            battery_remaining=telemetry.get("battery_remaining"),
            battery_voltage_v=telemetry.get("battery_voltage_v"),
            battery_current_a=telemetry.get("battery_current_a"),
            battery_power_w=telemetry.get("battery_power_w"),
            battery_warning=telemetry.get("battery_warning"),
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


_TELEMETRY_SOURCES = {
    "gps_fix_type": "gps",
    "satellites_used": "gps",
    "horizontal_accuracy_m": "gps",
    "vertical_accuracy_m": "gps",
    "global_position_valid": "global_position",
    "local_position_valid": "local_position",
    "home_position_valid": "failsafe_flags",
    "estimator_healthy": "estimator",
    "arming_checks_passed": "vehicle_status",
    "rc_link_available": "manual_control",
    "battery_remaining": "battery",
    "battery_voltage_v": "battery",
    "battery_current_a": "battery",
    "battery_power_w": "battery",
    "battery_warning": "battery",
}


def _field_evidence(
    *,
    values: dict[str, Any],
    telemetry: dict[str, Any],
    command: Px4CommandTransportStatus,
    ros: RosPx4BridgeStatus,
    disagreements: list[dict[str, Any]],
    stale_after_seconds: float,
) -> dict[str, TelemetryFieldState]:
    now = _utc_now()
    timestamps = telemetry.get("source_timestamps", {})
    disagreement_fields = {row["field"] for row in disagreements}
    evidence: dict[str, TelemetryFieldState] = {}
    for field_name, source_key in _TELEMETRY_SOURCES.items():
        timestamp = _parse_timestamp(timestamps.get(source_key))
        value = values.get(field_name)
        available = timestamp is not None and value is not None
        fresh = available and (now - timestamp).total_seconds() <= stale_after_seconds
        evidence[field_name] = TelemetryFieldState(
            value=value,
            source=f"PX4 ROS/uXRCE:{source_key}",
            source_timestamp=timestamp,
            freshness="fresh" if fresh else "stale" if available else "unknown",
            source_availability="available" if available else "unavailable",
            detail=None if available else f"{source_key} telemetry unavailable",
        )
    for field_name, ros_value, ros_timestamp in (
        ("armed", ros.armed, ros.last_vehicle_status_at),
        ("in_air", ros.in_air, ros.last_land_detected_at),
        ("nav_state", ros.nav_state, ros.last_vehicle_status_at),
        ("failsafe", ros.failsafe, ros.last_vehicle_status_at),
    ):
        source = "PX4 ROS/uXRCE" if ros_value is not None else "MAVSDK"
        timestamp = ros_timestamp if ros_value is not None else command.last_update_at
        available = values.get(field_name) is not None and timestamp is not None
        fresh = available and (now - timestamp).total_seconds() <= stale_after_seconds
        disagreement = field_name in disagreement_fields
        evidence[field_name] = TelemetryFieldState(
            value=values.get(field_name),
            source=source,
            source_timestamp=timestamp,
            freshness="fresh" if fresh else "stale" if available else "unknown",
            source_availability="degraded" if disagreement else "available" if available else "unavailable",
            disagreement=disagreement,
            detail="MAVSDK and ROS/uXRCE disagree" if disagreement else None,
        )
    return evidence


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


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


def _optional_float(message: Any, field_name: str) -> float | None:
    value = getattr(message, field_name, None)
    if value is None:
        return None
    try:
        numeric = float(value)
        return numeric if numeric == numeric and abs(numeric) != float("inf") else None
    except (TypeError, ValueError):
        return None


def _finite_fields(message: Any, *field_names: str) -> bool | None:
    values = [_optional_float(message, name) for name in field_names]
    return None if any(value is None for value in values) else True


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
