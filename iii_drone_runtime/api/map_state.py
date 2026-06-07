"""Runtime map state aggregation from ROS-like source messages."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from math import atan2, cos, degrees, pi, sin
from threading import RLock
from typing import Any, Callable

from iii_drone_contracts import (
    Bounds2D,
    ConductorGeometry,
    MapProjection,
    MapSourceStatus,
    MapState,
    Point2D,
    Point3D,
    PolylineLayer,
    PoseProjection,
    PowerlineFrameStatus,
    TargetState,
)
from iii_drone_contracts.envelopes import Freshness, SourceAvailability

from iii_drone_runtime.geometry import Quaternion, quaternion_multiply, quaternion_to_euler


STORED_POWERLINE_OVERVIEW_SERVICE = "/mission/powerline_overview_provider/get_powerline_overview"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class _StampedValue:
    value: Any
    updated_at: datetime


@dataclass(frozen=True)
class _HistoryPoint:
    point: Point2D | Point3D
    updated_at: datetime


@dataclass(frozen=True)
class _HistoryConductors:
    conductors: list["_RawConductor"]
    updated_at: datetime


@dataclass(frozen=True)
class _RawConductor:
    conductor_id: str
    point: Point3D
    source: str
    source_status: MapSourceStatus
    updated_at: datetime
    powerline_yaw_radians: float | None = None


class RuntimeMapAggregator:
    def __init__(
        self,
        *,
        stale_after_s: float = 2.0,
        history_ttl_s: float = 30.0,
        max_publish_hz: float = 10.0,
        drone_trail_limit: int = 120,
        target_history_limit: int = 80,
        live_conductor_history_limit: int = 20,
        operation_id_provider: Callable[[], str | None] | None = None,
        stored_overview_service_name: str = STORED_POWERLINE_OVERVIEW_SERVICE,
        map_frame_id: str = "world",
    ):
        self.stale_after = timedelta(seconds=stale_after_s)
        self.history_ttl = timedelta(seconds=history_ttl_s)
        self.min_publish_interval = timedelta(seconds=1.0 / max_publish_hz) if max_publish_hz > 0 else timedelta(0)
        self.drone_trail_limit = drone_trail_limit
        self.target_history_limit = target_history_limit
        self.live_conductor_history_limit = live_conductor_history_limit
        self.operation_id_provider = operation_id_provider or (lambda: None)
        self.stored_overview_service_name = stored_overview_service_name
        self.map_frame_id = map_frame_id
        self._lock = RLock()
        self._drone_pose: _StampedValue | None = None
        self._live_powerline: _StampedValue | None = None
        self._stored_powerline: _StampedValue | None = None
        self._target_state: _StampedValue | None = None
        self._trajectory: _StampedValue | None = None
        self._drone_trail: list[_HistoryPoint] = []
        self._target_history: list[_HistoryPoint] = []
        self._target_history_operation_id: str | None = None
        self._live_conductor_history: list[_HistoryConductors] = []
        self._cached_state: MapState | None = None
        self._cached_at: datetime | None = None
        self._dirty = True
        self._stored_overview_client: Any | None = None
        self._stored_overview_request_type: Any | None = None
        self._stored_overview_request_pending = False
        self._live_powerline_publisher_available = False
        self._tf_buffer: Any | None = None
        self._tf_listener: Any | None = None

    def subscribe(self, node: Any) -> list[Any]:
        try:
            from geometry_msgs.msg import PoseStamped
            from nav_msgs.msg import Path
            from iii_drone_interfaces.msg import CombinedDroneAwareness, Powerline, Target
            from iii_drone_interfaces.srv import GetPowerlineOverview
        except Exception:
            return []
        created = [
            node.create_subscription(
                CombinedDroneAwareness,
                "/control/maneuver_controller/combined_drone_awareness",
                self.handle_combined_drone_awareness,
                10,
            ),
            node.create_subscription(Target, "/control/maneuver_controller/target", self.handle_target, 10),
            node.create_subscription(PoseStamped, "/control/trajectory_controller/target_pose", self.handle_target_pose, 10),
            node.create_subscription(Path, "/control/trajectory_controller/trajectory_path", self.handle_trajectory_path, 10),
            node.create_subscription(Powerline, "/perception/pl_mapper/powerline", self.handle_live_powerline, 10),
        ]
        if hasattr(node, "create_client"):
            self._stored_overview_request_type = GetPowerlineOverview
            self._stored_overview_client = node.create_client(GetPowerlineOverview, self.stored_overview_service_name)
            created.append(self._stored_overview_client)
            if hasattr(node, "create_timer"):
                created.append(node.create_timer(1.0, self.refresh_stored_powerline_overview))
        if hasattr(node, "create_timer"):
            created.append(node.create_timer(1.0, lambda: self.refresh_graph_state(node)))
        try:
            from tf2_ros import Buffer, TransformListener

            self._tf_buffer = Buffer()
            self._tf_listener = TransformListener(self._tf_buffer, node)
        except Exception:
            self._tf_buffer = None
            self._tf_listener = None
        self.refresh_graph_state(node)
        return created

    def refresh_graph_state(self, node: Any) -> None:
        count_publishers = getattr(node, "count_publishers", None)
        if count_publishers is None:
            return
        try:
            publisher_available = count_publishers("/perception/pl_mapper/powerline") > 0
        except Exception:
            return
        with self._lock:
            self._live_powerline_publisher_available = publisher_available
            if self._live_powerline is not None:
                self._live_powerline = _StampedValue(self._live_powerline.value, _utc_now() if publisher_available else self._live_powerline.updated_at)
                self._dirty = True

    def refresh_stored_powerline_overview(self) -> None:
        client = self._stored_overview_client
        request_type = self._stored_overview_request_type
        if client is None or request_type is None or self._stored_overview_request_pending:
            return
        try:
            if not client.wait_for_service(timeout_sec=0.0):
                return
            future = client.call_async(request_type.Request())
        except Exception:
            return
        self._stored_overview_request_pending = True

        def on_response(done_future: Any) -> None:
            try:
                response = done_future.result()
                if bool(getattr(response, "success", False)):
                    self.handle_stored_powerline(getattr(response, "stored_powerline", None), now=_utc_now())
            except Exception:
                pass
            finally:
                self._stored_overview_request_pending = False

        future.add_done_callback(on_response)

    def handle_combined_drone_awareness(self, message: Any, *, now: datetime | None = None) -> None:
        timestamp = now or _message_time(message) or _utc_now()
        source_pose = getattr(getattr(message, "state", None), "pose", None)
        pose = _pose_projection(source_pose)
        with self._lock:
            if pose is not None:
                self._drone_pose = _StampedValue(pose, timestamp)
                self._append_trail(_point_from_pose(source_pose) or Point3D(x=pose.position.x, y=pose.position.y, z=pose.altitude_m or 0.0), timestamp)
            if getattr(message, "has_target", False) and getattr(message, "target_position_known", False):
                target = _target_state_from_message(getattr(message, "target", None), timestamp)
                if target is not None:
                    self._target_state = _StampedValue(target, timestamp)
                    self._append_target_history(target, timestamp)
            self._dirty = True

    def handle_drone_pose(self, message: Any, *, now: datetime | None = None) -> None:
        timestamp = now or _message_time(message) or _utc_now()
        source_pose = getattr(message, "pose", message)
        pose = _pose_projection(source_pose)
        if pose is None:
            return
        with self._lock:
            self._drone_pose = _StampedValue(pose, timestamp)
            self._append_trail(_point_from_pose(source_pose) or Point3D(x=pose.position.x, y=pose.position.y, z=pose.altitude_m or 0.0), timestamp)
            self._dirty = True

    def handle_live_powerline(self, message: Any, *, now: datetime | None = None) -> None:
        timestamp = now or _message_time(message) or _utc_now()
        self._live_powerline_publisher_available = True
        self._set_powerline(message, source="live_perception", timestamp=timestamp)

    def handle_stored_powerline(self, message: Any, *, now: datetime | None = None) -> None:
        timestamp = now or _message_time(message) or _utc_now()
        self._set_powerline(message, source="stored_overview", timestamp=timestamp)

    def handle_target(self, message: Any, *, now: datetime | None = None) -> None:
        timestamp = now or _message_time(message) or _utc_now()
        target = _target_state_from_message(message, timestamp)
        if target is None:
            return
        with self._lock:
            self._target_state = _StampedValue(target, timestamp)
            self._append_target_history(target, timestamp)
            self._dirty = True

    def handle_target_pose(self, message: Any, *, now: datetime | None = None) -> None:
        timestamp = now or _message_time(message) or _utc_now()
        point = _point_from_pose(getattr(message, "pose", message))
        if point is None:
            return
        with self._lock:
            target = TargetState(position=_point2d(point), status=MapSourceStatus.AVAILABLE, updated_at=timestamp)
            self._target_state = _StampedValue(target, timestamp)
            self._append_target_history(target, timestamp)
            self._dirty = True

    def handle_trajectory_path(self, message: Any, *, now: datetime | None = None) -> None:
        timestamp = now or _message_time(message) or _utc_now()
        points = []
        for pose_stamped in list(getattr(message, "poses", [])):
            point = _point_from_pose(getattr(pose_stamped, "pose", pose_stamped))
            if point is not None:
                points.append(_point2d(point))
        with self._lock:
            self._trajectory = _StampedValue(
                PolylineLayer(
                    label="trajectory",
                    points=points,
                    source_status=MapSourceStatus.AVAILABLE if points else MapSourceStatus.MISSING,
                    updated_at=timestamp,
                ),
                timestamp,
            )
            self._dirty = True

    def state(self, *, now: datetime | None = None, force: bool = False) -> MapState:
        timestamp = now or _utc_now()
        with self._lock:
            if (
                self._cached_state is not None
                and self._cached_at is not None
                and self._dirty
                and not force
                and timestamp - self._cached_at < self.min_publish_interval
            ):
                return self._cached_state
            if self._cached_state is not None and not self._dirty and not force:
                return self._cached_state
            state = self._build_state(timestamp)
            self._cached_state = state
            self._cached_at = timestamp
            self._dirty = False
            return state

    def _set_powerline(self, message: Any, *, source: str, timestamp: datetime) -> None:
        conductors = _conductors_from_powerline(
            message,
            source=source,
            timestamp=timestamp,
            transform_line=self._transform_line_to_map_frame,
        )
        with self._lock:
            if source == "live_perception":
                self._live_powerline = _StampedValue(conductors, timestamp)
                self._append_live_conductor_history(conductors, timestamp)
            else:
                self._stored_powerline = _StampedValue(conductors, timestamp)
            self._dirty = True

    def _transform_line_to_map_frame(
        self,
        *,
        point: Point3D,
        orientation: Quaternion | None,
        frame_id: str | None,
    ) -> tuple[Point3D, Quaternion | None]:
        if not frame_id or frame_id == self.map_frame_id or self._tf_buffer is None:
            return point, orientation
        try:
            from rclpy.time import Time

            transform = self._tf_buffer.lookup_transform(self.map_frame_id, frame_id, Time())
        except Exception:
            return point, orientation
        return _apply_transform(point, orientation, transform)

    def _append_trail(self, point: Point2D | Point3D, timestamp: datetime) -> None:
        self._drone_trail.append(_HistoryPoint(point=point, updated_at=timestamp))
        del self._drone_trail[:-self.drone_trail_limit]

    def _append_target_history(self, target: TargetState, timestamp: datetime) -> None:
        operation_id = self.operation_id_provider()
        if not operation_id or target.position is None:
            self._target_history.clear()
            self._target_history_operation_id = None
            return
        if operation_id != self._target_history_operation_id:
            self._target_history.clear()
            self._target_history_operation_id = operation_id
        self._target_history.append(_HistoryPoint(point=target.position, updated_at=timestamp))
        del self._target_history[:-self.target_history_limit]

    def _append_live_conductor_history(self, conductors: list[ConductorGeometry], timestamp: datetime) -> None:
        if not conductors:
            return
        self._live_conductor_history.append(_HistoryConductors(conductors=conductors, updated_at=timestamp))
        del self._live_conductor_history[:-self.live_conductor_history_limit]

    def _build_state(self, now: datetime) -> MapState:
        self._prune_histories(now)
        live = _persistent_source(self._live_powerline) if self._live_powerline_publisher_available else _fresh_or_stale(self._live_powerline, now, self.stale_after)
        stored = _persistent_source(self._stored_powerline)
        drone = _fresh_or_stale(self._drone_pose, now, self.stale_after)
        target = _fresh_or_stale(self._target_state, now, self.stale_after)
        trajectory = _fresh_or_stale(self._trajectory, now, self.stale_after)

        raw_live_conductors = _with_source_status(live.value if live else [], live.status if live else MapSourceStatus.MISSING)
        raw_stored_conductors = _with_source_status(
            stored.value if stored else [],
            stored.status if stored else MapSourceStatus.MISSING,
        )
        frame = _projection_frame(raw_live_conductors, raw_stored_conductors)
        frame_status = _frame_status(live, stored, now, frame)
        live_conductors = _transform_conductors(raw_live_conductors, frame)
        recent_live_conductors = _transform_conductors(
            _recent_live_conductor_layer(self._live_conductor_history, now, self.stale_after),
            frame,
        )
        stored_conductors = _transform_conductors(raw_stored_conductors, frame)
        drone_pose = _transform_pose(drone.value if drone else None, frame)
        target_state = _transform_target(target.value if target else None, frame)
        trajectory_layer = _transform_layer(trajectory.value if trajectory else None, frame)
        drone_trail = _transform_points([point.point for point in self._drone_trail], frame)
        target_history = _transform_points([point.point for point in self._target_history], frame)
        all_points = _all_points(
            live_conductors=live_conductors,
            recent_live_conductors=recent_live_conductors,
            stored_conductors=stored_conductors,
            drone_pose=drone_pose,
            target_state=target_state,
            trajectory=trajectory_layer,
            drone_trail=drone_trail,
            target_history=target_history,
        )

        if not any([live_conductors, stored_conductors, drone, target, trajectory, self._drone_trail]):
            return MapState.empty("no runtime map sources have been received")

        frame_available = frame_status.status == MapSourceStatus.AVAILABLE
        stale_reasons = _blocking_stale_reasons(frame_status=frame_status, live=live)
        availability = SourceAvailability.DEGRADED if not frame_available or stale_reasons else SourceAvailability.AVAILABLE
        freshness = Freshness.STALE if frame_status.status == MapSourceStatus.STALE or stale_reasons else Freshness.FRESH
        return MapState(
            source_label="runtime_ros_map_sources",
            source_timestamp=max(_timestamps(live, stored, drone, target, trajectory), default=None),
            freshness=freshness,
            source_availability=availability,
            degraded_reason="; ".join(stale_reasons) if stale_reasons else None if frame_available else frame_status.reason,
            frame=frame_status,
            live_conductors=live_conductors,
            recent_live_conductors=recent_live_conductors,
            stored_overview_conductors=stored_conductors,
            drone_pose=drone_pose,
            target_state=target_state if target_state else TargetState(),
            target_history=target_history,
            trajectory=trajectory_layer,
            drone_trail=PolylineLayer(
                label="drone_trail",
                points=drone_trail,
                source_status=MapSourceStatus.AVAILABLE if self._drone_trail else MapSourceStatus.MISSING,
                updated_at=self._drone_pose.updated_at if self._drone_pose else None,
            ),
            auto_fit_bounds=_bounds(all_points),
            generated_at=now,
        )

    def _prune_histories(self, now: datetime) -> None:
        cutoff = now - self.history_ttl
        self._drone_trail = [point for point in self._drone_trail if point.updated_at >= cutoff]
        self._target_history = [point for point in self._target_history if point.updated_at >= cutoff]
        self._live_conductor_history = [
            sample for sample in self._live_conductor_history if sample.updated_at >= cutoff
        ]
        del self._drone_trail[:-self.drone_trail_limit]
        del self._target_history[:-self.target_history_limit]
        del self._live_conductor_history[:-self.live_conductor_history_limit]
        if not self._target_history:
            self._target_history_operation_id = None


@dataclass(frozen=True)
class _SourceView:
    value: Any
    updated_at: datetime
    status: MapSourceStatus


@dataclass(frozen=True)
class _ProjectionFrame:
    origin: Point3D
    powerline_yaw_radians: float
    reference_source: str

    def project(self, point: Point3D) -> Point2D:
        dx = point.x - self.origin.x
        dy = point.y - self.origin.y
        lateral_x = -dx * sin(self.powerline_yaw_radians) + dy * cos(self.powerline_yaw_radians)
        return Point2D(
            x=lateral_x,
            y=point.z - self.origin.z,
        )


def _fresh_or_stale(value: _StampedValue | None, now: datetime, stale_after: timedelta) -> _SourceView | None:
    if value is None:
        return None
    status = MapSourceStatus.STALE if now - value.updated_at > stale_after else MapSourceStatus.AVAILABLE
    return _SourceView(value=value.value, updated_at=value.updated_at, status=status)


def _persistent_source(value: _StampedValue | None) -> _SourceView | None:
    if value is None:
        return None
    return _SourceView(value=value.value, updated_at=value.updated_at, status=MapSourceStatus.AVAILABLE)


def _frame_status(
    live: _SourceView | None,
    stored: _SourceView | None,
    now: datetime,
    frame: _ProjectionFrame | None,
) -> PowerlineFrameStatus:
    if stored is not None and stored.value:
        return PowerlineFrameStatus(
            source_label="stored_overview",
            source_timestamp=stored.updated_at,
            freshness=Freshness.STALE if stored.status == MapSourceStatus.STALE else Freshness.FRESH,
            source_availability=SourceAvailability.AVAILABLE,
            status=stored.status,
            reference_source=frame.reference_source if frame else "stored_overview",
            reason=None if stored.status == MapSourceStatus.AVAILABLE else "stored powerline overview is stale",
            runtime_timestamp=now,
        )
    if live is not None and live.value:
        return PowerlineFrameStatus(
            source_label="live_perception",
            source_timestamp=live.updated_at,
            freshness=Freshness.STALE if live.status == MapSourceStatus.STALE else Freshness.FRESH,
            source_availability=SourceAvailability.AVAILABLE,
            status=live.status,
            reference_source=frame.reference_source if frame else "live_perception",
            reason=None if live.status == MapSourceStatus.AVAILABLE else "live powerline perception is stale",
            runtime_timestamp=now,
        )
    return PowerlineFrameStatus(
        source_label="runtime_ros_map_sources",
        freshness=Freshness.UNKNOWN,
        source_availability=SourceAvailability.UNAVAILABLE,
        status=MapSourceStatus.MISSING,
        reason="no powerline reference available",
        runtime_timestamp=now,
    )


def _conductors_from_powerline(
    message: Any,
    *,
    source: str,
    timestamp: datetime,
    transform_line: Callable[..., tuple[Point3D, Quaternion | None]] | None = None,
) -> list[_RawConductor]:
    conductors = []
    for index, line in enumerate(list(getattr(message, "lines", []))):
        pose = getattr(line, "pose", None)
        point = _point_from_pose(pose) or _point_from_object(getattr(line, "projected_position", None))
        if point is None:
            continue
        orientation = _quaternion_from_pose(pose)
        if transform_line is not None:
            point, orientation = transform_line(
                point=point,
                orientation=orientation,
                frame_id=_frame_id(line),
            )
        conductors.append(
            _RawConductor(
                conductor_id=str(getattr(line, "id", index)),
                point=point,
                source=source,
                source_status=MapSourceStatus.AVAILABLE,
                updated_at=timestamp,
                powerline_yaw_radians=_yaw_from_quaternion(orientation),
            )
        )
    return conductors


def _with_source_status(conductors: list[_RawConductor], status: MapSourceStatus) -> list[_RawConductor]:
    return [
        replace(conductor, source_status=status)
        for conductor in conductors
    ]


def _recent_live_conductor_layer(
    history: list[_HistoryConductors],
    now: datetime,
    stale_after: timedelta,
) -> list[_RawConductor]:
    recent: list[_RawConductor] = []
    for sample_index, sample in enumerate(history):
        source_status = MapSourceStatus.STALE if now - sample.updated_at > stale_after else MapSourceStatus.AVAILABLE
        for conductor in sample.conductors:
            recent.append(
                replace(
                    conductor,
                    conductor_id=f"{conductor.conductor_id}@{sample_index}",
                    source="live_perception_recent",
                    source_status=source_status,
                    updated_at=sample.updated_at,
                )
            )
    return recent


def _projection_frame(
    live_conductors: list[ConductorGeometry],
    stored_conductors: list[ConductorGeometry],
) -> _ProjectionFrame | None:
    stored = _frame_from_conductors(stored_conductors, "stored_overview")
    if stored is not None:
        return stored
    return _frame_from_conductors(live_conductors, "live_perception")


def _frame_from_conductors(conductors: list[_RawConductor], reference_source: str) -> _ProjectionFrame | None:
    points = [conductor.point for conductor in conductors]
    if not points:
        return None
    angle = next((conductor.powerline_yaw_radians for conductor in conductors if conductor.powerline_yaw_radians is not None), None)
    if angle is None and len(points) > 1:
        lateral_angle = atan2(points[1].y - points[0].y, points[1].x - points[0].x)
        angle = lateral_angle - pi / 2.0
    return _ProjectionFrame(
        origin=points[0],
        powerline_yaw_radians=angle or 0.0,
        reference_source=reference_source,
    )


def _transform_conductors(conductors: list[_RawConductor], frame: _ProjectionFrame | None) -> list[ConductorGeometry]:
    if frame is None:
        return [
            ConductorGeometry(
                conductor_id=conductor.conductor_id,
                points=[_point2d(conductor.point)],
                source=conductor.source,
                source_status=conductor.source_status,
                updated_at=conductor.updated_at,
            )
            for conductor in conductors
        ]
    return [
        ConductorGeometry(
            conductor_id=conductor.conductor_id,
            points=[frame.project(conductor.point)],
            source=conductor.source,
            source_status=conductor.source_status,
            updated_at=conductor.updated_at,
        )
        for conductor in conductors
    ]


def _transform_pose(pose: PoseProjection | None, frame: _ProjectionFrame | None) -> PoseProjection | None:
    if pose is None or frame is None:
        return pose
    return pose.model_copy(
        update={
            "projection": MapProjection.POWERLINE_ORTHOGONAL,
            "position": frame.project(Point3D(x=pose.position.x, y=pose.position.y, z=pose.altitude_m or 0.0)),
        }
    )


def _transform_target(target: TargetState | None, frame: _ProjectionFrame | None) -> TargetState | None:
    if target is None or target.position is None or frame is None:
        return target
    return target.model_copy(update={"position": frame.project(Point3D(x=target.position.x, y=target.position.y, z=0.0))})


def _transform_layer(layer: PolylineLayer | None, frame: _ProjectionFrame | None) -> PolylineLayer | None:
    if layer is None or frame is None:
        return layer
    return layer.model_copy(update={"points": _transform_points(layer.points, frame)})


def _transform_points(points: list[Point2D | Point3D], frame: _ProjectionFrame | None) -> list[Point2D]:
    if frame is None:
        return [_point2d(point) if isinstance(point, Point3D) else point for point in points]
    return [
        frame.project(point if isinstance(point, Point3D) else Point3D(x=point.x, y=point.y, z=0.0))
        for point in points
    ]


def _pose_projection(pose: Any) -> PoseProjection | None:
    point = _point_from_pose(pose)
    if point is None:
        return None
    orientation = getattr(pose, "orientation", None)
    yaw = None
    if orientation is not None:
        yaw = degrees(
            quaternion_to_euler(
                Quaternion(
                    w=float(getattr(orientation, "w", 1.0)),
                    x=float(getattr(orientation, "x", 0.0)),
                    y=float(getattr(orientation, "y", 0.0)),
                    z=float(getattr(orientation, "z", 0.0)),
                )
            ).yaw
        )
    return PoseProjection(
        projection=MapProjection.TOP_DOWN,
        position=_point2d(point),
        yaw_degrees=yaw,
        altitude_m=point.z,
    )


def _quaternion_from_pose(pose: Any) -> Quaternion | None:
    orientation = getattr(pose, "orientation", None)
    if orientation is None:
        return None
    try:
        return Quaternion(
            w=float(getattr(orientation, "w", 1.0)),
            x=float(getattr(orientation, "x", 0.0)),
            y=float(getattr(orientation, "y", 0.0)),
            z=float(getattr(orientation, "z", 0.0)),
        )
    except (TypeError, ValueError):
        return None


def _yaw_from_quaternion(quaternion: Quaternion | None) -> float | None:
    if quaternion is None:
        return None
    return quaternion_to_euler(quaternion).yaw


def _frame_id(message: Any) -> str | None:
    frame_id = getattr(getattr(message, "header", None), "frame_id", None)
    return str(frame_id) if frame_id else None


def _apply_transform(
    point: Point3D,
    orientation: Quaternion | None,
    transform: Any,
) -> tuple[Point3D, Quaternion | None]:
    translation = getattr(getattr(transform, "transform", None), "translation", None)
    rotation = getattr(getattr(transform, "transform", None), "rotation", None)
    transform_quaternion = _quaternion_from_object(rotation)
    if transform_quaternion is None:
        return point, orientation
    rotated = _rotate_point(point, transform_quaternion)
    transformed_point = Point3D(
        x=rotated.x + float(getattr(translation, "x", 0.0)),
        y=rotated.y + float(getattr(translation, "y", 0.0)),
        z=rotated.z + float(getattr(translation, "z", 0.0)),
    )
    transformed_orientation = quaternion_multiply(transform_quaternion, orientation) if orientation else None
    return transformed_point, transformed_orientation


def _quaternion_from_object(value: Any) -> Quaternion | None:
    if value is None:
        return None
    try:
        return Quaternion(
            w=float(getattr(value, "w", 1.0)),
            x=float(getattr(value, "x", 0.0)),
            y=float(getattr(value, "y", 0.0)),
            z=float(getattr(value, "z", 0.0)),
        )
    except (TypeError, ValueError):
        return None


def _rotate_point(point: Point3D, quaternion: Quaternion) -> Point3D:
    vector = Quaternion(w=0.0, x=point.x, y=point.y, z=point.z)
    inverse = Quaternion(w=quaternion.w, x=-quaternion.x, y=-quaternion.y, z=-quaternion.z)
    rotated = quaternion_multiply(quaternion_multiply(quaternion, vector), inverse)
    return Point3D(x=rotated.x, y=rotated.y, z=rotated.z)


def _target_state_from_message(message: Any, timestamp: datetime) -> TargetState | None:
    if message is None:
        return None
    transform = getattr(message, "target_transform", None)
    point = _point_from_object(getattr(transform, "translation", None)) if transform is not None else None
    point = point or _point_from_pose(getattr(message, "pose", None)) or _point_from_object(getattr(message, "position", None))
    if point is None:
        return None
    target_id = getattr(message, "target_id", None)
    try:
        valid_target_id = target_id is not None and int(target_id) >= 0
    except (TypeError, ValueError):
        valid_target_id = False
    return TargetState(
        target_id=str(target_id) if valid_target_id else None,
        position=_point2d(point),
        label=f"target {target_id}" if valid_target_id else None,
        status=MapSourceStatus.AVAILABLE,
        updated_at=timestamp,
    )


def _point_from_pose(pose: Any) -> Point3D | None:
    if pose is None:
        return None
    return _point_from_object(getattr(pose, "position", None))


def _point_from_object(point: Any) -> Point3D | None:
    if point is None:
        return None
    if isinstance(point, dict):
        try:
            return Point3D(x=float(point["x"]), y=float(point["y"]), z=float(point.get("z", 0.0)))
        except (KeyError, TypeError, ValueError):
            return None
    try:
        return Point3D(
            x=float(getattr(point, "x")),
            y=float(getattr(point, "y")),
            z=float(getattr(point, "z", 0.0)),
        )
    except (AttributeError, TypeError, ValueError):
        return None


def _point2d(point: Point3D) -> Point2D:
    return Point2D(x=point.x, y=point.y)


def _message_time(message: Any) -> datetime | None:
    stamp = getattr(message, "stamp", None) or getattr(getattr(message, "header", None), "stamp", None)
    if stamp is None:
        return None
    seconds = float(getattr(stamp, "sec", 0)) + float(getattr(stamp, "nanosec", 0)) / 1_000_000_000.0
    if seconds <= 0:
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _stale_reasons(**sources: _SourceView | None) -> list[str]:
    reasons = []
    for label, source in sources.items():
        if source is not None and source.status == MapSourceStatus.STALE:
            reasons.append(f"{label} source is stale")
    return reasons


def _blocking_stale_reasons(*, frame_status: PowerlineFrameStatus, live: _SourceView | None) -> list[str]:
    if frame_status.reference_source == "live_perception":
        return _stale_reasons(live=live)
    return []


def _timestamps(*sources: _SourceView | None) -> list[datetime]:
    return [source.updated_at for source in sources if source is not None]


def _all_points(
    *,
    live_conductors: list[ConductorGeometry],
    recent_live_conductors: list[ConductorGeometry],
    stored_conductors: list[ConductorGeometry],
    drone_pose: PoseProjection | None,
    target_state: TargetState | None,
    trajectory: PolylineLayer | None,
    drone_trail: list[Point2D],
    target_history: list[Point2D],
) -> list[Point2D]:
    points: list[Point2D] = []
    for conductor in [*live_conductors, *recent_live_conductors, *stored_conductors]:
        points.extend(conductor.points)
    if drone_pose is not None:
        points.append(drone_pose.position)
    if target_state is not None and target_state.position is not None:
        points.append(target_state.position)
    if trajectory is not None:
        points.extend(trajectory.points)
    points.extend(drone_trail)
    points.extend(target_history)
    return points


def _bounds(points: list[Point2D]) -> Bounds2D | None:
    if not points:
        return None
    xs = [point.x for point in points]
    ys = [point.y for point in points]
    return Bounds2D(min_x=min(xs), min_y=min(ys), max_x=max(xs), max_y=max(ys))
