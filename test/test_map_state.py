from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from iii_drone_runtime.api.app import RuntimeApiSettings, create_app
from iii_drone_runtime.api.map_state import RuntimeMapAggregator


def _point(x, y, z=0.0):
    return SimpleNamespace(x=x, y=y, z=z)


def _quat(w=1.0, x=0.0, y=0.0, z=0.0):
    return SimpleNamespace(w=w, x=x, y=y, z=z)


def _pose(x, y, z=0.0):
    return SimpleNamespace(position=_point(x, y, z), orientation=_quat())


def _line(line_id, x, y, z=0.0, *, frame_id=""):
    return SimpleNamespace(
        id=line_id,
        header=SimpleNamespace(frame_id=frame_id),
        pose=_pose(x, y, z),
        projected_position=_point(x, y, z),
    )


def _powerline(*lines):
    return SimpleNamespace(lines=list(lines))


def _target(target_id, x, y, z=0.0):
    return SimpleNamespace(
        target_id=target_id,
        target_transform=SimpleNamespace(translation=_point(x, y, z)),
    )


def _path(*points):
    return SimpleNamespace(poses=[SimpleNamespace(pose=_pose(x, y, z)) for x, y, z in points])


def _awareness(x, y, z=0.0, *, target=None):
    return SimpleNamespace(
        state=SimpleNamespace(pose=_pose(x, y, z)),
        has_target=target is not None,
        target_position_known=target is not None,
        target=target or SimpleNamespace(),
    )


def _headers(client):
    token = client.post("/session/login", json={"password": "secret"}).json()["session_token"]
    return {"Authorization": f"Bearer {token}"}


def test_empty_map_state_is_degraded_until_runtime_sources_arrive():
    aggregator = RuntimeMapAggregator()

    state = aggregator.state()

    assert state.frame.status == "missing"
    assert state.source_availability == "unknown"
    assert "no runtime map sources" in state.degraded_reason


def test_map_aggregates_drone_live_stored_target_and_trajectory_sources():
    aggregator = RuntimeMapAggregator()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_combined_drone_awareness(_awareness(1.0, 2.0, 3.0, target=_target(7, 4.0, 5.0, 6.0)), now=now)
    aggregator.handle_live_powerline(_powerline(_line(1, 10.0, 11.0, 12.0)), now=now)
    aggregator.handle_stored_powerline(_powerline(_line(2, 20.0, 21.0, 22.0)), now=now)
    aggregator.handle_trajectory_path(_path((0.0, 0.0, 0.0), (8.0, 9.0, 10.0)), now=now)
    state = aggregator.state(now=now, force=True)

    assert state.source_label == "runtime_ros_map_sources"
    assert state.source_availability == "available"
    assert state.frame.reference_source == "stored_overview"
    assert state.drone_pose.position.x == -19.0
    assert state.drone_pose.altitude_m == 3.0
    assert state.target_state.target_id == "7"
    assert state.target_state.position.x == -16.0
    assert state.trajectory.points[-1].x == -12.0
    assert state.live_conductors[0].source == "live_perception"
    assert state.live_conductors[0].conductor_id == "1"
    assert state.live_conductors[0].points[0].x == -10.0
    assert state.stored_overview_conductors[0].source == "stored_overview"
    assert state.stored_overview_conductors[0].conductor_id == "2"
    assert state.stored_overview_conductors[0].points[0].x == 0.0
    assert state.drone_trail.points == [state.drone_pose.position]
    assert state.auto_fit_bounds.min_x == -21.0
    assert state.auto_fit_bounds.min_y == -22.0
    assert state.auto_fit_bounds.max_x == 0.0
    assert state.auto_fit_bounds.max_y == 0.0


def test_stored_overview_stabilizes_frame_and_live_overlay_does_not_recenter_view():
    aggregator = RuntimeMapAggregator()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_stored_powerline(_powerline(_line(1, 100.0, 100.0), _line(2, 110.0, 100.0)), now=now)
    aggregator.handle_live_powerline(_powerline(_line(1, 1000.0, 1000.0), _line(2, 1010.0, 1000.0)), now=now)
    first = aggregator.state(now=now, force=True)
    aggregator.handle_live_powerline(_powerline(_line(1, 1200.0, 900.0), _line(2, 1210.0, 900.0)), now=now)
    second = aggregator.state(now=now + timedelta(seconds=1), force=True)

    assert first.frame.reference_source == "stored_overview"
    assert first.stored_overview_conductors[0].points[0].x == 0.0
    assert first.stored_overview_conductors[1].points[0].x == 0.0
    assert first.live_conductors[0].points[0].x == 900.0
    assert second.frame.reference_source == "stored_overview"
    assert second.stored_overview_conductors[0].points[0].x == 0.0
    assert second.live_conductors[0].points[0].x == 800.0


def test_live_powerline_defines_temporary_frame_when_stored_overview_missing():
    aggregator = RuntimeMapAggregator()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_live_powerline(_powerline(_line(1, 10.0, 20.0), _line(2, 20.0, 20.0)), now=now)
    state = aggregator.state(now=now, force=True)

    assert state.frame.reference_source == "live_perception"
    assert state.live_conductors[0].points[0].x == 0.0
    assert state.live_conductors[0].points[0].y == 0.0
    assert state.live_conductors[1].points[0].x == 0.0


def test_live_powerline_is_transformed_to_world_before_projection():
    class _FakeTfBuffer:
        def lookup_transform(self, target_frame, source_frame, stamp):
            del stamp
            assert target_frame == "world"
            assert source_frame == "drone"
            return SimpleNamespace(
                transform=SimpleNamespace(
                    translation=_point(100.0, 0.0, 10.0),
                    rotation=_quat(),
                )
            )

    aggregator = RuntimeMapAggregator(map_frame_id="world")
    aggregator._tf_buffer = _FakeTfBuffer()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_stored_powerline(_powerline(_line(1, 100.0, 0.0, 10.0, frame_id="world")), now=now)
    aggregator.handle_live_powerline(_powerline(_line(1, 0.0, 2.0, 0.0, frame_id="drone")), now=now)
    state = aggregator.state(now=now, force=True)

    assert state.frame.reference_source == "stored_overview"
    assert state.stored_overview_conductors[0].points[0].x == 0.0
    assert state.stored_overview_conductors[0].points[0].y == 0.0
    assert state.live_conductors[0].points[0].x == 2.0
    assert state.live_conductors[0].points[0].y == 0.0


def test_powerline_orthogonal_projection_uses_lateral_offset_and_altitude():
    aggregator = RuntimeMapAggregator()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_stored_powerline(_powerline(_line(1, 0.0, 0.0, 10.0)), now=now)
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(5.0, 3.0, 12.0)), now=now)
    state = aggregator.state(now=now, force=True)

    assert state.frame.reference_source == "stored_overview"
    assert state.stored_overview_conductors[0].points[0].x == 0.0
    assert state.stored_overview_conductors[0].points[0].y == 0.0
    assert state.drone_pose.position.x == 3.0
    assert state.drone_pose.position.y == 2.0
    assert state.drone_trail.points == [state.drone_pose.position]


def test_map_carries_distinct_top_down_projection_pylon_order_corridor_and_capture_preview():
    aggregator = RuntimeMapAggregator()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_stored_powerline(_powerline(_line(1, 10.0, 20.0, 8.0)), now=now)
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(14.0, 23.0, 6.0)), now=now)
    aggregator.handle_pylon_overview_status(
        SimpleNamespace(
            stamp=SimpleNamespace(sec=int(now.timestamp()), nanosec=0),
            overview=SimpleNamespace(
                pylons=[SimpleNamespace(id=2, x=30.0, y=40.0), SimpleNamespace(id=1, x=10.0, y=20.0)]
            ),
        ),
        now=now,
    )

    state = aggregator.state(now=now, force=True)

    assert state.drone_pose.position.x == 3.0
    assert state.drone_pose.position.y == -2.0
    assert state.top_down_drone_pose.position.x == 14.0
    assert state.top_down_drone_pose.position.y == 23.0
    assert [endpoint.pylon_id for endpoint in state.pylon_endpoints] == [2, 1]
    assert [point.x for point in state.inferred_corridor.points] == [10.0, 30.0]
    assert state.capture_preview.label == "pylon capture preview"
    assert state.capture_preview.position == state.top_down_drone_pose.position


def test_stored_overview_service_response_populates_map_reference():
    class _Request:
        pass

    class _Service:
        Request = _Request

    class _Future:
        def __init__(self, response):
            self._response = response

        def result(self):
            return self._response

        def add_done_callback(self, callback):
            callback(self)

    class _Client:
        def wait_for_service(self, *, timeout_sec):
            assert timeout_sec == 0.0
            return True

        def call_async(self, request):
            assert isinstance(request, _Request)
            return _Future(SimpleNamespace(success=True, stored_powerline=_powerline(_line(7, 12.0, 14.0))))

    aggregator = RuntimeMapAggregator()
    aggregator._stored_overview_client = _Client()
    aggregator._stored_overview_request_type = _Service

    aggregator.refresh_stored_powerline_overview()
    state = aggregator.state(force=True)

    assert state.frame.reference_source == "stored_overview"
    assert state.stored_overview_conductors[0].conductor_id == "7"
    assert state.stored_overview_conductors[0].points[0].x == 0.0


def test_no_powerline_reference_degrades_even_when_other_operational_data_exists():
    aggregator = RuntimeMapAggregator()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(3.0, 4.0, 5.0)), now=now)
    state = aggregator.state(now=now, force=True)

    assert state.frame.status == "missing"
    assert state.frame.reason == "no powerline reference available"
    assert state.source_availability == "degraded"
    assert state.drone_pose.position.x == 3.0


def test_stale_sources_degrade_map_state_without_dropping_last_geometry():
    aggregator = RuntimeMapAggregator(stale_after_s=1.0)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = start + timedelta(seconds=5)

    aggregator.handle_live_powerline(_powerline(_line(1, 10.0, 11.0)), now=start)
    aggregator._live_powerline_publisher_available = False
    state = aggregator.state(now=later, force=True)

    assert state.source_availability == "degraded"
    assert state.freshness == "stale"
    assert state.frame.status == "stale"
    assert state.live_conductors[0].source_status == "stale"
    assert "live source is stale" in state.degraded_reason


def test_live_powerline_becomes_stale_when_publisher_remains_but_samples_stop():
    aggregator = RuntimeMapAggregator(stale_after_s=1.0)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = start + timedelta(seconds=5)

    aggregator.handle_live_powerline(_powerline(_line(1, 10.0, 11.0)), now=start)
    aggregator.refresh_graph_state(SimpleNamespace(count_publishers=lambda topic: 1 if topic == "/perception/pl_mapper/powerline" else 0))
    state = aggregator.state(now=later, force=True)

    assert state.frame.status == "stale"
    assert state.source_availability == "degraded"
    assert state.live_conductors[0].source_status == "stale"
    assert "live source is stale" in state.degraded_reason


def test_publisher_flag_never_refreshes_old_live_geometry_timestamp():
    aggregator = RuntimeMapAggregator(stale_after_s=1.0)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = start + timedelta(seconds=5)

    aggregator.handle_live_powerline(_powerline(_line(1, 10.0, 11.0)), now=start)
    aggregator._live_powerline_publisher_available = True
    state = aggregator.state(now=later, force=True)

    assert state.live_conductors[0].source_status == "stale"
    assert state.frame.status == "stale"


def test_map_transport_diagnostics_report_age_size_rate_and_geometry_count():
    aggregator = RuntimeMapAggregator(stale_after_s=2.0, max_publish_hz=5.0)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    aggregator.handle_live_powerline(_powerline(_line(1, 10.0, 11.0)), now=start)
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(3.0, 4.0)), now=start)

    state = aggregator.state(now=start + timedelta(milliseconds=250), force=True)

    assert state.transport.live_source_age_ms == 250.0
    assert state.transport.drone_pose_age_ms == 250.0
    assert state.transport.stale_after_ms == 2000.0
    assert state.transport.publish_rate_limit_hz == 5.0
    assert state.transport.serialized_bytes > 0
    assert state.transport.geometry_point_count >= 4
    assert state.transport.estimated_max_kbps > 0


def test_stored_overview_remains_available_when_live_and_drone_sources_are_stale():
    aggregator = RuntimeMapAggregator(stale_after_s=1.0)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = start + timedelta(seconds=5)

    aggregator.handle_stored_powerline(_powerline(_line(1, 10.0, 11.0)), now=start)
    aggregator.handle_live_powerline(_powerline(_line(2, 20.0, 21.0)), now=start)
    aggregator._live_powerline_publisher_available = False
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(3.0, 4.0)), now=start)
    state = aggregator.state(now=later, force=True)

    assert state.frame.reference_source == "stored_overview"
    assert state.frame.status == "available"
    assert state.source_availability == "available"
    assert state.freshness == "fresh"
    assert state.degraded_reason is None
    assert state.stored_overview_conductors[0].source_status == "available"
    assert state.live_conductors[0].source_status == "stale"


def test_map_state_generation_is_throttled_and_coalesces_fast_updates():
    aggregator = RuntimeMapAggregator(max_publish_hz=1.0)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(1.0, 1.0)), now=start)
    first = aggregator.state(now=start)
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(2.0, 2.0)), now=start + timedelta(milliseconds=100))
    coalesced = aggregator.state(now=start + timedelta(milliseconds=100))
    updated = aggregator.state(now=start + timedelta(seconds=2))

    assert coalesced.generated_at == first.generated_at
    assert coalesced.drone_pose.position.x == 1.0
    assert updated.generated_at > first.generated_at
    assert updated.drone_pose.position.x == 2.0


def test_history_buffers_are_bounded_and_expire_predictably():
    aggregator = RuntimeMapAggregator(history_ttl_s=1.0, drone_trail_limit=2)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(1.0, 1.0)), now=start)
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(2.0, 2.0)), now=start + timedelta(milliseconds=100))
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(3.0, 3.0)), now=start + timedelta(milliseconds=200))
    bounded = aggregator.state(now=start + timedelta(milliseconds=300), force=True)
    expired = aggregator.state(now=start + timedelta(seconds=2), force=True)

    assert [point.x for point in bounded.drone_trail.points] == [2.0, 3.0]
    assert expired.drone_trail.points == []


def test_target_history_is_operation_aware_and_resets_between_operations():
    active_operation_id = None
    aggregator = RuntimeMapAggregator(operation_id_provider=lambda: active_operation_id)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    aggregator.handle_live_powerline(_powerline(_line(1, 0.0, 0.0), _line(2, 10.0, 0.0)), now=now)

    aggregator.handle_target(_target(1, 1.0, 1.0), now=now)
    inactive = aggregator.state(now=now, force=True)
    active_operation_id = "op-1"
    aggregator.handle_target(_target(1, 2.0, 2.0), now=now + timedelta(seconds=1))
    aggregator.handle_target(_target(1, 3.0, 3.0), now=now + timedelta(seconds=2))
    first_operation = aggregator.state(now=now + timedelta(seconds=2), force=True)
    active_operation_id = "op-2"
    aggregator.handle_target(_target(1, 4.0, 4.0), now=now + timedelta(seconds=3))
    second_operation = aggregator.state(now=now + timedelta(seconds=3), force=True)
    active_operation_id = None
    aggregator.handle_target(_target(1, 5.0, 5.0), now=now + timedelta(seconds=4))
    inactive_again = aggregator.state(now=now + timedelta(seconds=4), force=True)

    assert inactive.target_history == []
    assert [point.x for point in first_operation.target_history] == [2.0, 3.0]
    assert [point.x for point in second_operation.target_history] == [4.0]
    assert inactive_again.target_history == []


def test_recent_live_conductor_history_is_bounded_and_marked_separately():
    aggregator = RuntimeMapAggregator(live_conductor_history_limit=2)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    aggregator.handle_live_powerline(_powerline(_line(1, 0.0, 0.0), _line(2, 10.0, 0.0)), now=now)
    aggregator.handle_live_powerline(_powerline(_line(1, 20.0, 0.0), _line(2, 30.0, 0.0)), now=now + timedelta(seconds=1))
    aggregator.handle_live_powerline(_powerline(_line(1, 40.0, 0.0), _line(2, 50.0, 0.0)), now=now + timedelta(seconds=2))
    state = aggregator.state(now=now + timedelta(seconds=2), force=True)

    assert len(state.recent_live_conductors) == 4
    assert {conductor.source for conductor in state.recent_live_conductors} == {"live_perception_recent"}
    assert state.live_conductors[0].source == "live_perception"
    assert state.live_conductors[0].points[0].x == 0.0
    assert state.recent_live_conductors[0].points[0].x == 0.0


def test_runtime_map_endpoint_returns_aggregated_contract_state():
    aggregator = RuntimeMapAggregator()
    aggregator.handle_drone_pose(SimpleNamespace(pose=_pose(3.0, 4.0, 5.0)))
    client = TestClient(
        create_app(
            settings=RuntimeApiSettings(
                runtime_id="test-runtime",
                runtime_name="Test Runtime",
                browser_password="secret",
                cli_token="cli-secret",
            ),
            map_aggregator=aggregator,
        )
    )
    headers = _headers(client)

    response = client.get("/map/state", headers=headers)

    assert response.status_code == 200
    payload = response.json()
    assert payload["drone_pose"]["position"]["x"] == 3.0
    assert payload["drone_pose"]["altitude_m"] == 5.0
