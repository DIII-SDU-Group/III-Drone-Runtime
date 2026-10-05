"""External-vision (motion-capture) positioning health for the opti_track profile.

In the OptiTrack lab PX4 positions from external vision: the pose relay
publishes the motion-capture pose on ``/fmu/in/vehicle_visual_odometry`` and
reports its own health; PX4's estimator flags tell whether it fuses that
position, height and yaw; and the relay sets the EKF global origin once per
boot. No GPS fix ever exists indoors, so these replace the GPS evidence of the
field profile.
"""

from __future__ import annotations

from datetime import datetime, timezone
import math
import threading
from typing import Any

from iii_drone_contracts import ExternalVisionState, InspectionPreflightItem, VehicleDomainState
from iii_drone_contracts.envelopes import Freshness

from ..ros_sampling import create_sampled_subscription


# Profiles whose PX4 positions from the motion-capture pose relay.
EXTERNAL_VISION_PROFILES = frozenset({"opti_track"})
POSE_RELAY_HEALTH_TOPIC = "/opti_track/pose_relay/health"
ESTIMATOR_STATUS_FLAGS_TOPIC = "/fmu/out/estimator_status_flags"
# The relay reports at 2 Hz and PX4 sends its estimator flags at about 1 Hz;
# neither needs following faster.
SAMPLE_RATE_HZ = 2.0

_RELAY_LEVELS = {0: "ok", 1: "warn", 2: "error", 3: "stale"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ExternalVisionMonitor:
    """Newest pose-relay health and PX4 external-vision fusion flags."""

    def __init__(
        self,
        *,
        enabled: bool,
        relay_stale_after_seconds: float = 2.0,
        fusion_stale_after_seconds: float = 3.0,
        origin_stale_after_seconds: float = 3.0,
    ):
        self.enabled = enabled
        self.relay_stale_after_seconds = relay_stale_after_seconds
        self.fusion_stale_after_seconds = fusion_stale_after_seconds
        self.origin_stale_after_seconds = origin_stale_after_seconds
        self._lock = threading.Lock()
        self._relay: dict[str, Any] | None = None
        self._relay_at: datetime | None = None
        self._fusion: tuple[bool | None, bool | None, bool | None] | None = None
        self._fusion_at: datetime | None = None

    def subscribe(self, node: Any) -> list[Any]:
        if not self.enabled:
            return []
        try:
            from diagnostic_msgs.msg import DiagnosticStatus
            from px4_msgs.msg import EstimatorStatusFlags
            from rclpy.qos import qos_profile_sensor_data
        except Exception:
            return []
        return [
            create_sampled_subscription(
                node,
                DiagnosticStatus,
                POSE_RELAY_HEALTH_TOPIC,
                self.handle_relay_health_message,
                10,
                rate_hz=SAMPLE_RATE_HZ,
            ),
            create_sampled_subscription(
                node,
                EstimatorStatusFlags,
                ESTIMATOR_STATUS_FLAGS_TOPIC,
                self.handle_estimator_status_flags_message,
                qos_profile_sensor_data,
                rate_hz=SAMPLE_RATE_HZ,
            ),
        ]

    def handle_relay_health_message(self, message: Any) -> None:
        values = {
            str(getattr(item, "key", "")): str(getattr(item, "value", ""))
            for item in getattr(message, "values", [])
        }
        relay = {
            "level": _RELAY_LEVELS.get(_level(getattr(message, "level", None)), "unknown"),
            "message": str(getattr(message, "message", "")) or None,
            "input_rate_hz": _float_value(values.get("input_rate_hz")),
            "output_rate_hz": _float_value(values.get("output_rate_hz")),
            "last_input_age_ms": _float_value(values.get("last_input_age_ms")),
            "max_input_gap_ms": _float_value(values.get("max_input_gap_ms")),
            "lab_stamp_age_ms": _float_value(values.get("lab_stamp_age_ms")),
            "stale": _bool_value(values.get("stale")),
            "origin_sent": _bool_value(values.get("origin_sent")),
            "rigid_body_id": values.get("rigid_body_id") or None,
        }
        with self._lock:
            self._relay = relay
            self._relay_at = _utc_now()

    def handle_estimator_status_flags_message(self, message: Any) -> None:
        fusion = (
            _optional_bool(message, "cs_ev_pos"),
            _optional_bool(message, "cs_ev_hgt"),
            _optional_bool(message, "cs_ev_yaw"),
        )
        with self._lock:
            self._fusion = fusion
            self._fusion_at = _utc_now()

    def state(
        self,
        *,
        origin_valid: bool | None,
        origin_timestamp: datetime | None,
        now: datetime | None = None,
    ) -> ExternalVisionState:
        current = now or _utc_now()
        with self._lock:
            relay = dict(self._relay) if self._relay is not None else None
            relay_at = self._relay_at
            fusion = self._fusion
            fusion_at = self._fusion_at
        relay_freshness = _freshness(relay_at, current, self.relay_stale_after_seconds)
        fusion_freshness = _freshness(fusion_at, current, self.fusion_stale_after_seconds)
        origin_freshness = (
            _freshness(origin_timestamp, current, self.origin_stale_after_seconds)
            if origin_valid is not None
            else Freshness.UNKNOWN
        )
        relay = relay or {}
        ev_pos, ev_hgt, ev_yaw = fusion if fusion is not None else (None, None, None)

        reasons: list[str] = []
        if relay_freshness == Freshness.UNKNOWN:
            reasons.append("pose relay health has not been received")
        elif relay_freshness == Freshness.STALE:
            reasons.append("pose relay health is stale")
        else:
            if relay.get("level") != "ok":
                detail = relay.get("message")
                reasons.append(
                    f"pose relay reports {relay.get('level', 'unknown')}"
                    + (f": {detail}" if detail else "")
                )
            if relay.get("stale") is not False:
                reasons.append("pose relay input is stale")
        if fusion_freshness == Freshness.UNKNOWN:
            reasons.append("PX4 estimator status flags have not been received")
        elif fusion_freshness == Freshness.STALE:
            reasons.append("PX4 estimator status flags are stale")
        else:
            missing = [
                label
                for label, fused in (("position", ev_pos), ("height", ev_hgt), ("yaw", ev_yaw))
                if fused is not True
            ]
            if missing:
                reasons.append(f"PX4 is not fusing external-vision {', '.join(missing)}")
        if origin_valid is not True:
            reasons.append("EKF global origin is not set")
        elif origin_freshness != Freshness.FRESH:
            reasons.append("EKF global origin state is stale")

        components = (relay_freshness, fusion_freshness)
        if all(value == Freshness.FRESH for value in components):
            freshness = Freshness.FRESH
        elif any(value == Freshness.STALE for value in components):
            freshness = Freshness.STALE
        else:
            freshness = Freshness.UNKNOWN
        return ExternalVisionState(
            ready=not reasons,
            freshness=freshness,
            degraded_reason="; ".join(reasons) if reasons else None,
            relay_level=relay.get("level", "unknown"),
            relay_message=relay.get("message"),
            relay_freshness=relay_freshness,
            relay_timestamp=relay_at,
            relay_stale=relay.get("stale"),
            input_rate_hz=relay.get("input_rate_hz"),
            output_rate_hz=relay.get("output_rate_hz"),
            last_input_age_ms=relay.get("last_input_age_ms"),
            max_input_gap_ms=relay.get("max_input_gap_ms"),
            lab_stamp_age_ms=relay.get("lab_stamp_age_ms"),
            origin_sent=relay.get("origin_sent"),
            rigid_body_id=relay.get("rigid_body_id"),
            ev_pos_fused=ev_pos,
            ev_hgt_fused=ev_hgt,
            ev_yaw_fused=ev_yaw,
            fusion_freshness=fusion_freshness,
            fusion_timestamp=fusion_at,
            origin_valid=origin_valid,
            origin_freshness=origin_freshness,
        )


def external_vision_preflight_items(vehicle: VehicleDomainState) -> list[InspectionPreflightItem]:
    """Hard gates that replace GPS and stored-geometry evidence indoors."""
    vision = vehicle.external_vision or ExternalVisionState()
    local_position = vehicle.telemetry_fields.get("local_position_valid")
    fused = (vision.ev_pos_fused, vision.ev_hgt_fused, vision.ev_yaw_fused)
    return [
        InspectionPreflightItem(
            key="local_position",
            label="Local position valid",
            passed=bool(
                local_position
                and local_position.freshness == "fresh"
                and local_position.source_availability == "available"
                and local_position.value is True
            ),
            source="PX4 VehicleLocalPosition",
            detail=None if local_position is None else local_position.detail,
        ),
        InspectionPreflightItem(
            key="ekf_origin",
            label="EKF global origin set",
            passed=vision.origin_valid is True and vision.origin_freshness == "fresh",
            source="PX4 VehicleLocalPosition",
            detail=f"origin {_flag_text(vision.origin_valid, 'set', 'not set')} ({vision.origin_freshness})",
        ),
        InspectionPreflightItem(
            key="vision_fusion",
            label="PX4 external-vision fusion",
            passed=all(value is True for value in fused) and vision.fusion_freshness == "fresh",
            source="PX4 EstimatorStatusFlags",
            detail=(
                f"position {_flag_text(fused[0], 'fused', 'not fused')}, "
                f"height {_flag_text(fused[1], 'fused', 'not fused')}, "
                f"yaw {_flag_text(fused[2], 'fused', 'not fused')} ({vision.fusion_freshness})"
            ),
        ),
        InspectionPreflightItem(
            key="pose_relay",
            label="OptiTrack pose relay healthy",
            passed=(
                vision.relay_level == "ok"
                and vision.relay_freshness == "fresh"
                and vision.relay_stale is False
            ),
            source=POSE_RELAY_HEALTH_TOPIC,
            detail=_relay_detail(vision),
        ),
    ]


def _relay_detail(vision: ExternalVisionState) -> str:
    parts = [f"level {vision.relay_level} ({vision.relay_freshness})"]
    if vision.input_rate_hz is not None:
        parts.append(f"input {vision.input_rate_hz:.0f} Hz")
    if vision.last_input_age_ms is not None:
        parts.append(f"last input {vision.last_input_age_ms:.0f} ms ago")
    if vision.relay_message:
        parts.append(vision.relay_message)
    return ", ".join(parts)


def _flag_text(value: bool | None, true_text: str, false_text: str) -> str:
    if value is None:
        return "unknown"
    return true_text if value else false_text


def _freshness(timestamp: datetime | None, now: datetime, stale_after_seconds: float) -> Freshness:
    if timestamp is None:
        return Freshness.UNKNOWN
    if (now - timestamp).total_seconds() > stale_after_seconds:
        return Freshness.STALE
    return Freshness.FRESH


def _level(value: Any) -> int | None:
    # A ROS ``byte`` field arrives as a one-byte ``bytes`` object.
    if isinstance(value, (bytes, bytearray)):
        return value[0] if value else None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_value(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _bool_value(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    return None


def _optional_bool(message: Any, field_name: str) -> bool | None:
    value = getattr(message, field_name, None)
    return None if value is None else bool(value)
