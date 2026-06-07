"""Runtime-side cache for typed custom operation mode status."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from iii_drone_contracts import OperationDomainState
from iii_drone_contracts.envelopes import Freshness, SourceAvailability


CUSTOM_OPERATION_STATUS_TOPIC = "/mission/custom_operation/mode_status"
LEGACY_CUSTOM_OPERATION_STATUS_TOPIC = "/mission/custom_operation/status"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CustomOperationStatusCache:
    def __init__(
        self,
        *,
        topic: str = CUSTOM_OPERATION_STATUS_TOPIC,
        legacy_topic: str = LEGACY_CUSTOM_OPERATION_STATUS_TOPIC,
        stale_after_seconds: float = 2.0,
    ):
        self.topic = topic
        self.legacy_topic = legacy_topic
        self.stale_after = timedelta(seconds=stale_after_seconds)
        self._latest_message = None
        self._legacy_status: dict[str, Any] | None = None
        self._last_legacy_update_at: datetime | None = None
        self._legacy_publisher_available = False
        self._unavailable_reason = "custom operation status topic has not been received"

    def subscribe(self, node):
        subscriptions = []
        try:
            from iii_drone_interfaces.msg import CustomOperationModeStatus, StringStamped
            from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
        except Exception as exc:
            self._unavailable_reason = f"CustomOperationModeStatus message unavailable: {exc}"
            return []
        qos = QoSProfile(depth=1)
        qos.reliability = ReliabilityPolicy.BEST_EFFORT
        qos.durability = DurabilityPolicy.TRANSIENT_LOCAL
        subscriptions.append(node.create_subscription(CustomOperationModeStatus, self.topic, self.handle_message, qos))
        subscriptions.append(node.create_subscription(StringStamped, self.legacy_topic, self.handle_legacy_status_message, qos))
        if hasattr(node, "create_timer"):
            subscriptions.append(node.create_timer(1.0, lambda: self.refresh_graph_state(node)))
        self.refresh_graph_state(node)
        return subscriptions

    def handle_message(self, message) -> None:
        self._latest_message = message
        self._unavailable_reason = None

    def handle_legacy_status_message(self, message) -> None:
        self._legacy_status = _parse_legacy_status(getattr(message, "data", ""))
        self._last_legacy_update_at = _utc_now()
        self._legacy_publisher_available = True
        self._unavailable_reason = None

    def refresh_graph_state(self, node) -> None:
        count_publishers = getattr(node, "count_publishers", None)
        if count_publishers is None:
            return
        try:
            self._legacy_publisher_available = count_publishers(self.legacy_topic) > 0
        except Exception:
            return

    def legacy_mode_id(self) -> int | None:
        legacy = self._legacy_status or {}
        try:
            return int(legacy["mode_id"])
        except (KeyError, TypeError, ValueError):
            return None

    def mode_id(self) -> int | None:
        message = self._latest_message
        if message is not None:
            for key in ("mode_id", "owned_mode_id", "custom_operation_mode_id"):
                try:
                    return int(getattr(message, key))
                except (AttributeError, TypeError, ValueError):
                    continue
        return self.legacy_mode_id()

    def state(self) -> OperationDomainState:
        message = self._latest_message
        if message is None:
            fallback = self._legacy_state()
            if fallback is not None:
                return fallback
            return OperationDomainState(
                source_label="custom_operation_status",
                freshness=Freshness.UNKNOWN,
                source_availability=SourceAvailability.UNAVAILABLE,
                degraded_reason=self._unavailable_reason,
                status="unknown",
            )

        degraded_reasons = list(getattr(message, "degraded_reasons", []))
        operation_active = bool(getattr(message, "operation_active", False))
        activation_rejections = []
        if not getattr(message, "custom_operation_modes_registered", False):
            activation_rejections.append("CustomOperation mode is not registered")
        if operation_active:
            activation_rejections.append("another custom operation is active")
        if getattr(message, "degraded", False):
            activation_rejections.extend(degraded_reasons)

        operation_state = "custom_operation_active" if operation_active else "custom_operation_idle"
        latest = {
            "operation_state_label": getattr(message, "operation_state_label", "unknown"),
            "operation_active": operation_active,
            "active_operation": getattr(message, "active_operation", ""),
            "custom_operation_modes_registered": getattr(message, "custom_operation_modes_registered", False),
            "required_modes": list(getattr(message, "required_modes", [])),
            "registered_modes": list(getattr(message, "registered_modes", [])),
            "owned_mode": getattr(message, "owned_mode", ""),
            "mode_id": self.mode_id(),
            "control_owner": getattr(message, "control_owner", ""),
            "cancel_available": getattr(message, "cancel_available", False),
            "degraded": getattr(message, "degraded", False),
            "degraded_reasons": degraded_reasons,
            "start_allowed": not activation_rejections,
            "start_rejections": activation_rejections,
        }
        return OperationDomainState(
            source_label="custom_operation_status",
            freshness=Freshness.FRESH,
            source_availability=SourceAvailability.AVAILABLE,
            degraded_reason="; ".join(degraded_reasons) if degraded_reasons else None,
            latest=latest,
            active_operation_id=getattr(message, "active_operation", None) or None,
            active_operation_type=getattr(message, "active_operation", None) or None,
            status=operation_state,
        )

    def _legacy_state(self) -> OperationDomainState | None:
        if self._legacy_status is None and not self._legacy_publisher_available:
            return None

        now = _utc_now()
        stale = False
        if self._last_legacy_update_at is not None:
            stale = now - self._last_legacy_update_at > self.stale_after
        operation_active = bool((self._legacy_status or {}).get("active", False))
        latest = {
            "operation_state_label": "active" if operation_active else "legacy_status_available",
            "operation_active": operation_active,
            "active_operation": "",
            "custom_operation_modes_registered": True,
            "required_modes": ["custom_operation"],
            "registered_modes": ["custom_operation"],
            "owned_mode": "CustomOperation",
            "mode_id": self.mode_id(),
            "control_owner": "custom_operation" if operation_active else "",
            "cancel_available": operation_active,
            "degraded": False,
            "degraded_reasons": [],
            "start_allowed": True,
            "start_rejections": [],
            "legacy_status": self._legacy_status or {},
            "legacy_status_topic_available": self._legacy_publisher_available,
        }
        if self._legacy_status is None:
            latest["legacy_status_missing_reason"] = "legacy custom operation status publisher is present but no sample has been received"
        return OperationDomainState(
            source_label="custom_operation_legacy_status",
            source_timestamp=self._last_legacy_update_at,
            freshness=Freshness.STALE if stale else (Freshness.FRESH if self._legacy_status is not None else Freshness.UNKNOWN),
            source_availability=SourceAvailability.DEGRADED if stale else SourceAvailability.AVAILABLE,
            degraded_reason="legacy custom operation status topic is stale" if stale else None,
            latest=latest,
            active_operation_id=None,
            active_operation_type=None,
            status="custom_operation_active" if operation_active else "custom_operation_idle",
        )


def _parse_legacy_status(raw: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except Exception:
        return {"raw": raw}
    return payload if isinstance(payload, dict) else {"raw": raw}
