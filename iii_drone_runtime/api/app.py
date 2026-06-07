"""FastAPI application factory for iii-runtime-api."""

from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass

from fastapi import Depends, FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from iii_drone_contracts import (
    ActionStartResponse,
    ApiIdentity,
    CommandRejection,
    CommandRequest,
    CommandResponse,
    CommandResultMessage,
    ConfigurationApplyRequest,
    ControlDomainState,
    DomainName,
    ErrorCode,
    GenericDomainState,
    HandlerPermission,
    MapState,
    MissionDomainState,
    OperationDomainState,
    OperatorEvent,
    OperatorStatePatch,
    OperatorStateSnapshot,
    PayloadDomainState,
    PerceptionDomainState,
    PowerlineDomainState,
    RosbagDomainState,
    SimulationDomainState,
    ServiceCallRequest,
    ServiceCallResponse,
    SnapshotDownloadRequest,
    SnapshotLoadRequest,
    SnapshotSaveRequest,
    SnapshotSetDefaultRequest,
    SystemDomainState,
    VehicleDomainState,
)

from .configuration import (
    ConfigurationRuntimeController,
    ConfigurationPermissionGate,
    ConfigurationServerAdapter,
    UnavailableConfigurationServerAdapter,
    register_configuration_command_handlers,
)
from .custom_operations import (
    NonblockingCustomOperationClient,
    OperationEvent,
    OperationReadinessContext,
    UnavailableCustomOperationTransport,
)
from .dispatch import DispatchRegistry
from .events import RuntimeEventLog
from .flight_commands import (
    ControlModeCommandAdapter,
    ControlTransitionTracker,
    DroneAwarenessCache,
    FlightCommandGate,
    HoldInterruptionReconciler,
    Px4NavStateModeAdapter,
    register_flight_mode_command_handlers,
)
from .logs import LogSourceProvider
from .map_state import RuntimeMapAggregator
from .mission_status import MissionStatusCache
from .mdns import RuntimeApiAdvertiser
from .operation_status import CustomOperationStatusCache
from .operation_commands import register_custom_operation_command_handlers
from .payload import (
    GripperServiceAdapter,
    PayloadPermissionGate,
    PayloadStatusCache,
    UnavailableGripperServiceAdapter,
    register_payload_command_handlers,
)
from .perception import (
    OperationalPermissionGate,
    PerceptionStatusCache,
    PLMapperServiceAdapter,
    RosPLMapperServiceAdapter,
    PowerlineOverviewServiceAdapter,
    UnavailablePowerlineOverviewServiceAdapter,
    register_perception_command_handlers,
)
from .px4_adapter import PersistentPx4CommandAdapter, register_px4_command_handlers
from .px4_state import FusedPx4StateProvider, RosPx4StateCache
from .runtime_commands import register_runtime_command_handlers
from .rosbag import (
    RosbagController,
    RosbagRecorderAdapter,
    RosRosbagRecorderAdapter,
    register_rosbag_command_handlers,
)
from .safety import RuntimeMutationGate
from .session import BrowserSessionLease, SessionMetadata
from .simulation import SimulationRuntimeController
from .state_bus import RuntimeStateBus
from .supervision_health import SupervisionHealthCache
from .system_adapter import RuntimeSystemAdapter
from ..ros_lifecycle import RuntimeRosExecutor


security = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class RuntimeApiSettings:
    runtime_id: str = "iii-runtime"
    runtime_name: str = "III Runtime"
    profile: str | None = None
    host: str = "0.0.0.0"
    port: int = 8765
    mdns_enabled: bool = False
    mdns_instance_name: str = "III Runtime API"
    mdns_advertise_host: str | None = None
    system_id: str = "iii-drone"
    browser_password: str = "dev-password"
    cli_token: str = "dev-cli-token"
    heartbeat_interval_seconds: float = 2.0
    lease_timeout_seconds: float = 8.0
    px4_mavlink_endpoint: str = "udpin://0.0.0.0:14540"
    px4_command_transport_enabled: bool = True
    log_dir: str = "/tmp/iii_drone/runtime-api"

    @classmethod
    def from_env(cls) -> "RuntimeApiSettings":
        profile = os.environ.get("III_RUNTIME_API_PROFILE") or os.environ.get("III_SYSTEM_PROFILE")
        require_secrets = _env_bool("III_RUNTIME_API_REQUIRE_SECRETS", default=profile == "real")
        browser_password = os.environ.get("III_RUNTIME_API_BROWSER_PASSWORD")
        cli_token = os.environ.get("III_RUNTIME_API_CLI_TOKEN")
        if require_secrets:
            missing = [
                name
                for name, value in (
                    ("III_RUNTIME_API_BROWSER_PASSWORD", browser_password),
                    ("III_RUNTIME_API_CLI_TOKEN", cli_token),
                )
                if not value
            ]
            if missing:
                raise RuntimeError(f"missing required runtime API secret environment variables: {', '.join(missing)}")
        return cls(
            runtime_id=os.environ.get("III_RUNTIME_API_ID", "iii-runtime"),
            runtime_name=os.environ.get("III_RUNTIME_API_NAME", "III Runtime"),
            profile=profile,
            host=os.environ.get("III_RUNTIME_API_HOST", "0.0.0.0"),
            port=int(os.environ.get("III_RUNTIME_API_PORT", "8765")),
            mdns_enabled=_env_bool("III_RUNTIME_API_MDNS_ENABLED", default=True),
            mdns_instance_name=os.environ.get("III_RUNTIME_API_MDNS_INSTANCE", "III Runtime API"),
            mdns_advertise_host=os.environ.get("III_RUNTIME_API_MDNS_HOST")
            or os.environ.get("III_RUNTIME_API_ADVERTISE_HOST"),
            system_id=os.environ.get("III_RUNTIME_API_SYSTEM_ID", "iii-drone"),
            browser_password=browser_password or "dev-password",
            cli_token=cli_token or "dev-cli-token",
            heartbeat_interval_seconds=float(os.environ.get("III_RUNTIME_API_HEARTBEAT_INTERVAL_SEC", "2")),
            lease_timeout_seconds=float(os.environ.get("III_RUNTIME_API_SESSION_LEASE_TIMEOUT_SEC", "8")),
            px4_mavlink_endpoint=os.environ.get("III_RUNTIME_API_PX4_MAVLINK_ENDPOINT", "udpin://0.0.0.0:14540"),
            px4_command_transport_enabled=os.environ.get("III_RUNTIME_API_PX4_ENABLED", "1").lower()
            not in {"0", "false", "no", "off"},
            log_dir=os.environ.get("III_RUNTIME_API_LOG_DIR", "/tmp/iii_drone/runtime-api"),
        )


class LoginRequest(BaseModel):
    password: str
    client_label: str | None = None


class LoginResponse(BaseModel):
    session_token: str
    token_type: str = "bearer"


class SessionResponse(BaseModel):
    acquired_at: str
    last_heartbeat_at: str
    client_label: str | None = None
    client_address: str | None = None
    heartbeat_interval_seconds: float
    lease_timeout_seconds: float

    @classmethod
    def from_metadata(cls, metadata: SessionMetadata, settings: RuntimeApiSettings) -> "SessionResponse":
        return cls(
            acquired_at=metadata.acquired_at.isoformat(),
            last_heartbeat_at=metadata.last_heartbeat_at.isoformat(),
            client_label=metadata.client_label,
            client_address=metadata.client_address,
            heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
            lease_timeout_seconds=settings.lease_timeout_seconds,
        )


def _identity(settings: RuntimeApiSettings) -> ApiIdentity:
    return ApiIdentity(
        runtime_id=settings.runtime_id,
        runtime_name=settings.runtime_name,
        profile=settings.profile,
        host_label=settings.system_id,
    )


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _optional_rclpy():
    try:
        import rclpy
        import rclpy.executors
    except Exception:
        return None
    return rclpy


def create_app(
    settings: RuntimeApiSettings | None = None,
    session_lease: BrowserSessionLease | None = None,
    system_adapter: RuntimeSystemAdapter | None = None,
    state_bus: RuntimeStateBus | None = None,
    dispatch_registry: DispatchRegistry | None = None,
    mutation_gate: RuntimeMutationGate | None = None,
    log_provider: LogSourceProvider | None = None,
    map_aggregator: RuntimeMapAggregator | None = None,
    simulation_controller: SimulationRuntimeController | None = None,
    supervision_health: SupervisionHealthCache | None = None,
    mission_status: MissionStatusCache | None = None,
    operation_status: CustomOperationStatusCache | None = None,
    custom_operation_client: NonblockingCustomOperationClient | None = None,
    custom_operation_transport: object | None = None,
    payload_status: PayloadStatusCache | None = None,
    gripper_service: GripperServiceAdapter | None = None,
    perception_status: PerceptionStatusCache | None = None,
    pl_mapper_service: PLMapperServiceAdapter | None = None,
    powerline_overview_service: PowerlineOverviewServiceAdapter | None = None,
    rosbag_adapter: RosbagRecorderAdapter | None = None,
    configuration_adapter: ConfigurationServerAdapter | None = None,
    px4_adapter: PersistentPx4CommandAdapter | None = None,
    px4_ros_state: RosPx4StateCache | None = None,
    px4_state_provider: FusedPx4StateProvider | None = None,
    drone_awareness: DroneAwarenessCache | None = None,
    flight_gate: FlightCommandGate | None = None,
    control_transition_tracker: ControlTransitionTracker | None = None,
    control_mode_adapter: ControlModeCommandAdapter | None = None,
    hold_reconciler: HoldInterruptionReconciler | None = None,
    mdns_advertiser: RuntimeApiAdvertiser | None = None,
    ros_executor: RuntimeRosExecutor | None = None,
) -> FastAPI:
    runtime_settings = settings or RuntimeApiSettings.from_env()
    browser_sessions = session_lease or BrowserSessionLease(
        lease_timeout_seconds=runtime_settings.lease_timeout_seconds
    )
    event_log = RuntimeEventLog()
    runtime_system = system_adapter or RuntimeSystemAdapter()
    runtime_state_bus = state_bus or RuntimeStateBus()
    runtime_logs = log_provider or LogSourceProvider()
    runtime_map = map_aggregator or RuntimeMapAggregator()
    runtime_simulation = simulation_controller or SimulationRuntimeController(profile=runtime_settings.profile)
    runtime_supervision_health = supervision_health or SupervisionHealthCache()
    runtime_mission_status = mission_status or MissionStatusCache()
    runtime_operation_status = operation_status or CustomOperationStatusCache()
    runtime_payload_status = payload_status or PayloadStatusCache()
    runtime_perception_status = perception_status or PerceptionStatusCache()
    runtime_px4_adapter = px4_adapter or PersistentPx4CommandAdapter(
        endpoint=runtime_settings.px4_mavlink_endpoint,
        enabled=runtime_settings.px4_command_transport_enabled,
    )
    runtime_px4_ros_state = px4_ros_state or RosPx4StateCache()

    def px4_registered_mode_label(nav_state_id: int) -> str | None:
        mission_mode_id = runtime_mission_status.mission_mode_id()
        if mission_mode_id is not None and nav_state_id == mission_mode_id:
            return "mission"
        custom_operation_mode_id = runtime_operation_status.mode_id()
        if custom_operation_mode_id is not None and nav_state_id == custom_operation_mode_id:
            return "custom_operation"
        return None

    runtime_px4_state = px4_state_provider or FusedPx4StateProvider(
        command_adapter=runtime_px4_adapter,
        ros_state=runtime_px4_ros_state,
        mode_label_provider=px4_registered_mode_label,
    )
    runtime_drone_awareness = drone_awareness or DroneAwarenessCache()
    runtime_mdns_advertiser = mdns_advertiser
    runtime_ros_executor = ros_executor or RuntimeRosExecutor(
        rclpy_module=_optional_rclpy(),
        event_log=event_log,
    )
    runtime_rosbag = RosbagController(
        adapter=rosbag_adapter or RosRosbagRecorderAdapter(node_provider=lambda: runtime_ros_executor.node)
    )
    if runtime_mdns_advertiser is None and runtime_settings.mdns_enabled:
        runtime_mdns_advertiser = RuntimeApiAdvertiser(
            runtime_id=runtime_settings.runtime_id,
            runtime_name=runtime_settings.runtime_name,
            profile=runtime_settings.profile,
            bind_host=runtime_settings.host,
            port=runtime_settings.port,
            instance_name=runtime_settings.mdns_instance_name,
            system_id=runtime_settings.system_id,
            advertise_host=runtime_settings.mdns_advertise_host,
        )
    runtime_transition_tracker = control_transition_tracker or ControlTransitionTracker()
    runtime_hold_reconciler = hold_reconciler or HoldInterruptionReconciler(
        mission_state_provider=lambda: effective_mission_state(),
        operation_state_provider=lambda: operation_domain_state(),
        event_log=event_log,
    )

    def effective_system_state() -> SystemDomainState:
        state = runtime_supervision_health.state()
        status = runtime_system.status()
        fallback_available = status.daemon_socket_state == "responding"
        if state.source_availability != "unavailable" and not fallback_available:
            return state

        if state.source_availability != "unavailable" and fallback_available:
            daemon_overrides = (
                (status.runtime_booted is not None and status.runtime_booted != state.booted)
                or (status.system_active is not None and status.system_active != state.active)
            )
            if not daemon_overrides:
                return state
            latest = dict(state.latest)
            latest["runtime_status"] = status.as_dict()
            return SystemDomainState(
                source_label="supervision_health+daemon_socket",
                freshness=state.freshness,
                source_availability=state.source_availability,
                degraded_reason=state.degraded_reason,
                latest=latest,
                api_state=status.api_state or state.api_state,
                daemon_state=status.daemon_socket_state or state.daemon_state,
                booted=status.runtime_booted if status.runtime_booted is not None else state.booted,
                active=status.system_active if status.system_active is not None else state.active,
            )

        return SystemDomainState(
            source_label="daemon_socket",
            freshness="fresh" if fallback_available else "unknown",
            source_availability="available" if fallback_available else "unavailable",
            degraded_reason=status.error,
            latest={"runtime_status": status.as_dict()},
            api_state=status.api_state,
            daemon_state=status.daemon_socket_state,
            booted=status.runtime_booted,
            active=status.system_active,
        )

    def vehicle_state_with_awareness() -> VehicleDomainState:
        state = runtime_px4_state.state()
        state.latest["combined_drone_awareness"] = runtime_drone_awareness.state().as_dict()
        return state

    def px4_mode_label() -> str:
        vehicle = runtime_px4_state.state()
        value = vehicle.nav_state or vehicle.flight_mode or ""
        return str(value).strip().lower()

    def effective_mission_state() -> MissionDomainState:
        state = runtime_mission_status.state()
        mode = px4_mode_label()
        if mode and mode != "mission":
            latest = dict(state.latest)
            latest["reported_mission_active"] = latest.get("mission_active")
            latest["mission_active"] = False
            latest["reconciled_from_px4_nav_state"] = mode
            if state.mission_state == "active":
                state.mission_state = "idle"
            state.latest = latest
        return state

    def operation_domain_state() -> OperationDomainState:
        state = runtime_operation_status.state()
        try:
            active = runtime_custom_operations.status()
        except KeyError:
            try:
                latest_operation = runtime_custom_operations.latest()
            except KeyError:
                latest_operation = None
        else:
            latest_operation = active
        if latest_operation is not None:
            state.latest["runtime_operation"] = latest_operation.as_dict()
            if latest_operation.terminal:
                state.active_operation_id = None
                state.active_operation_type = None
                state.latest["operation_active"] = False
                state.latest["active_operation"] = ""
                state.latest["cancel_available"] = False
                if state.status == "custom_operation_active":
                    state.status = "custom_operation_idle"
            else:
                state.active_operation_id = latest_operation.operation_id
                state.active_operation_type = latest_operation.operation
                state.latest["operation_active"] = True
                state.latest["active_operation"] = latest_operation.operation
                state.latest["cancel_available"] = True
                state.status = "custom_operation_active"
        mode = px4_mode_label()
        if mode and mode != "custom_operation":
            state.latest["reported_operation_active"] = state.latest.get("operation_active")
            state.latest["operation_active"] = False
            state.latest["active_operation"] = ""
            state.latest["cancel_available"] = False
            state.latest["reconciled_from_px4_nav_state"] = mode
            state.active_operation_id = None
            state.active_operation_type = None
            if state.status == "custom_operation_active":
                state.status = "custom_operation_idle"
        if runtime_custom_operations.events():
            state.latest["operation_events"] = [event.as_dict() for event in runtime_custom_operations.events()]
        return state

    runtime_flight_gate = flight_gate or FlightCommandGate(
        vehicle_state_provider=runtime_px4_state,
        system_state_provider=effective_system_state,
        mission_state_provider=effective_mission_state,
        operation_state_provider=operation_domain_state,
        transition_tracker=runtime_transition_tracker,
        hold_reconciler=runtime_hold_reconciler,
        awareness_state_provider=runtime_drone_awareness.state,
    )
    loop_holder: dict[str, asyncio.AbstractEventLoop | None] = {"loop": None}
    state_refresh_task: dict[str, asyncio.Task | None] = {"task": None}

    def operation_readiness() -> OperationReadinessContext:
        state = runtime_operation_status.state()
        mission = effective_mission_state()
        return OperationReadinessContext(
            custom_operation_mode_registered=state.latest.get("custom_operation_modes_registered") is True,
            custom_operation_mode_active=state.status in {"custom_operation_idle", "custom_operation_active"},
            mission_active=mission.latest.get("mission_active") is True or mission.mission_state == "active",
            active_operation_id=state.active_operation_id,
        )

    def operation_event_sink(event: OperationEvent) -> None:
        events = runtime_state_bus.snapshot.operation.latest.setdefault("operation_events", [])
        events.append(event.as_dict())
        del events[:-50]
        if event.event_type in {"started", "feedback"}:
            result_status = "running"
        elif event.event_type == "rejected":
            result_status = "rejected"
        elif event.event_type == "result" and (event.payload.get("result") or {}).get("success") is False:
            result_status = "failed"
        else:
            result_status = "succeeded"
        result = CommandResultMessage(
            request_id=event.payload.get("request_id", event.operation_id),
            command_id=f"custom_operation.{event.operation}.{event.event_type}",
            status=result_status,
            action_id=event.operation_id,
            result=event.as_dict(),
        )
        loop = loop_holder.get("loop")
        if loop is not None and loop.is_running():
            asyncio.run_coroutine_threadsafe(runtime_state_bus.send_command_result(result), loop)
            operation_state = operation_domain_state()
            runtime_state_bus.snapshot.operation = operation_state
            asyncio.run_coroutine_threadsafe(
                runtime_state_bus.send_patch(OperatorStatePatch(domain=DomainName.OPERATION, state=operation_state)),
                loop,
            )

    runtime_custom_operations = custom_operation_client or NonblockingCustomOperationClient(
        transport=custom_operation_transport or UnavailableCustomOperationTransport(),
        readiness_provider=operation_readiness,
        event_sink=operation_event_sink,
    )
    runtime_payload_permission = PayloadPermissionGate(
        mission_state_provider=effective_mission_state,
        operation_state_provider=lambda: operation_domain_state(),
    )
    runtime_perception_permission = OperationalPermissionGate(
        mission_state_provider=effective_mission_state,
        operation_state_provider=lambda: operation_domain_state(),
    )
    runtime_configuration = ConfigurationRuntimeController(
        adapter=configuration_adapter or UnavailableConfigurationServerAdapter(),
        permission_gate=ConfigurationPermissionGate(
            mission_state_provider=effective_mission_state,
            operation_state_provider=lambda: operation_domain_state(),
        ),
    )
    dispatcher = dispatch_registry or DispatchRegistry.empty()
    if dispatch_registry is None:
        register_runtime_command_handlers(
            dispatcher,
            daemon_client=runtime_system.daemon_client,
            event_log=event_log,
            mutation_gate=mutation_gate,
        )
        register_px4_command_handlers(
            dispatcher,
            adapter=runtime_px4_adapter,
            event_log=event_log,
            vehicle_state_provider=runtime_px4_state,
            command_gate=runtime_flight_gate,
            transition_tracker=runtime_transition_tracker,
            hold_reconciler=runtime_hold_reconciler,
        )
        register_flight_mode_command_handlers(
            dispatcher,
            gate=runtime_flight_gate,
            transition_tracker=runtime_transition_tracker,
            event_log=event_log,
            mode_adapter=control_mode_adapter
            or Px4NavStateModeAdapter(
                node_provider=lambda: runtime_ros_executor.node,
                mission_mode_id_provider=runtime_mission_status.mission_mode_id,
                custom_operation_mode_id_provider=runtime_operation_status.mode_id,
            ),
        )
        register_custom_operation_command_handlers(
            dispatcher,
            client=runtime_custom_operations,
            event_log=event_log,
        )
        register_payload_command_handlers(
            dispatcher,
            status_cache=runtime_payload_status,
            permission_gate=runtime_payload_permission,
            gripper_service=gripper_service or UnavailableGripperServiceAdapter(),
            event_log=event_log,
        )
        register_perception_command_handlers(
            dispatcher,
            status_cache=runtime_perception_status,
            permission_gate=runtime_perception_permission,
            pl_mapper_service=pl_mapper_service or RosPLMapperServiceAdapter(node_provider=lambda: runtime_ros_executor.node),
            overview_service=powerline_overview_service or UnavailablePowerlineOverviewServiceAdapter(),
            event_log=event_log,
        )
        register_rosbag_command_handlers(
            dispatcher,
            controller=runtime_rosbag,
            event_log=event_log,
            mission_state_provider=effective_mission_state,
        )
        register_configuration_command_handlers(
            dispatcher,
            controller=runtime_configuration,
            event_log=event_log,
        )
    app = FastAPI(
        title="III Runtime API",
        version="v2alpha1",
        description="Network-facing runtime/operator API for III-Drone GUI v2 and remote CLI.",
    )

    @app.on_event("startup")
    async def start_runtime_services() -> None:
        loop_holder["loop"] = asyncio.get_running_loop()
        runtime_ros_executor.start(
            [
                runtime_supervision_health.subscribe,
                runtime_mission_status.subscribe,
                runtime_operation_status.subscribe,
                runtime_payload_status.subscribe,
                runtime_perception_status.subscribe,
                runtime_map.subscribe,
                runtime_px4_ros_state.subscribe,
                runtime_drone_awareness.subscribe,
            ]
        )
        await runtime_px4_adapter.start()
        state_refresh_task["task"] = asyncio.create_task(periodic_vehicle_control_refresh())
        if runtime_mdns_advertiser is not None:
            await asyncio.to_thread(runtime_mdns_advertiser.start)

    @app.on_event("shutdown")
    async def stop_runtime_services() -> None:
        try:
            if runtime_mdns_advertiser is not None:
                await asyncio.to_thread(runtime_mdns_advertiser.stop)
        finally:
            try:
                await runtime_px4_adapter.stop()
            finally:
                task = state_refresh_task.get("task")
                if task is not None:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                runtime_ros_executor.stop()

    def require_browser_session(
        credentials: HTTPAuthorizationCredentials | None = Depends(security),
    ) -> SessionMetadata:
        if credentials is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing browser session token",
            )
        try:
            return browser_sessions.validate(credentials.credentials)
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    def require_cli_token(x_iii_cli_token: str | None = Header(default=None)) -> str:
        if x_iii_cli_token != runtime_settings.cli_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing or invalid CLI token",
            )
        return x_iii_cli_token

    def simulation_domain_state(result: dict) -> SimulationDomainState:
        status_payload = result.get("status") or {}
        return SimulationDomainState(
            source_label="simulation_tools",
            freshness="fresh" if result.get("enabled") else "unknown",
            source_availability="available" if result.get("enabled") else "unavailable",
            degraded_reason=result.get("disabled_reason"),
            latest=result,
            profile=result.get("profile") or "unknown",
            px4_gazebo_status=status_payload.get("px4_gazebo", "unknown"),
        )

    def simulation_response(result: dict) -> dict:
        state = simulation_domain_state(result)
        runtime_state_bus.snapshot.simulation = state
        return {"simulation": state.model_dump(mode="json"), "result": result}

    def map_domain_state(state: MapState) -> GenericDomainState:
        return GenericDomainState(
            source_label=state.source_label,
            source_timestamp=state.source_timestamp,
            freshness=state.freshness,
            source_availability=state.source_availability,
            degraded_reason=state.degraded_reason,
            error_reason=state.error_reason,
            value=state.model_dump(mode="json"),
        )

    def hydrate_state_snapshot() -> None:
        system_state = effective_system_state()
        runtime_mission_status.set_system_running(bool(system_state.booted and system_state.active))
        runtime_state_bus.snapshot.system = system_state
        runtime_state_bus.snapshot.vehicle = vehicle_state_with_awareness()
        runtime_state_bus.snapshot.control = control_state_with_awareness()
        runtime_state_bus.snapshot.mission = effective_mission_state()
        runtime_state_bus.snapshot.operation = operation_domain_state()
        runtime_state_bus.snapshot.payload = runtime_payload_status.state(permission=runtime_payload_permission.gripper_permission())
        runtime_state_bus.snapshot.perception = runtime_perception_status.perception_state(
            permission=runtime_perception_permission.mutating_permission("perception")
        )
        runtime_state_bus.snapshot.powerline = runtime_perception_status.powerline_state(
            permission=runtime_perception_permission.mutating_permission("perception")
        )
        runtime_state_bus.snapshot.map = map_domain_state(runtime_map.state(force=True))
        runtime_state_bus.snapshot.rosbag = runtime_rosbag.state()
        runtime_state_bus.snapshot.configuration = runtime_configuration.state()

    def control_state_with_awareness() -> ControlDomainState:
        state = runtime_flight_gate.control_state()
        state.latest["combined_drone_awareness"] = runtime_drone_awareness.state().as_dict()
        return state

    async def periodic_vehicle_control_refresh() -> None:
        while True:
            await asyncio.sleep(0.5)
            publish_vehicle_control_refresh()

    def publish_vehicle_control_refresh() -> None:
        vehicle_state = vehicle_state_with_awareness()
        control_state = control_state_with_awareness()
        mission_state = effective_mission_state()
        operation_state = operation_domain_state()
        perception_state = runtime_perception_status.perception_state(
            permission=runtime_perception_permission.mutating_permission("perception")
        )
        powerline_state = runtime_perception_status.powerline_state(
            permission=runtime_perception_permission.mutating_permission("perception")
        )
        map_state = runtime_map.state(force=True)
        runtime_state_bus.snapshot.vehicle = vehicle_state
        runtime_state_bus.snapshot.control = control_state
        runtime_state_bus.snapshot.mission = mission_state
        runtime_state_bus.snapshot.operation = operation_state
        runtime_state_bus.snapshot.perception = perception_state
        runtime_state_bus.snapshot.powerline = powerline_state
        runtime_state_bus.snapshot.map = map_domain_state(map_state)
        loop = loop_holder.get("loop")
        if loop is None or not loop.is_running():
            return
        for patch in (
            OperatorStatePatch(domain=DomainName.VEHICLE, state=vehicle_state),
            OperatorStatePatch(domain=DomainName.CONTROL, state=control_state),
            OperatorStatePatch(domain=DomainName.MISSION, state=mission_state),
            OperatorStatePatch(domain=DomainName.OPERATION, state=operation_state),
            OperatorStatePatch(domain=DomainName.PERCEPTION, state=perception_state),
            OperatorStatePatch(domain=DomainName.POWERLINE, state=powerline_state),
            OperatorStatePatch(domain=DomainName.MAP, state=map_domain_state(map_state)),
        ):
            _schedule_on_runtime_loop(runtime_state_bus.send_patch(patch))

    def _schedule_on_runtime_loop(awaitable) -> None:
        loop = loop_holder.get("loop")
        if loop is None or not loop.is_running():
            return
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is loop:
            loop.create_task(awaitable)
        else:
            asyncio.run_coroutine_threadsafe(awaitable, loop)

    @app.get("/identity", response_model=ApiIdentity)
    def identity() -> ApiIdentity:
        return _identity(runtime_settings)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"api": "up"}

    @app.get("/runtime/status")
    def runtime_status(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return runtime_system.status().as_dict()

    @app.get("/system/health", response_model=SystemDomainState)
    def system_health(session_metadata: SessionMetadata = Depends(require_browser_session)) -> SystemDomainState:
        del session_metadata
        state = effective_system_state()
        runtime_mission_status.set_system_running(bool(state.booted and state.active))
        runtime_state_bus.snapshot.system = state
        return state

    @app.get("/subsystems/health")
    def subsystem_health(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        rows = runtime_supervision_health.subsystem_health()
        by_id = {row["subsystem_id"]: row for row in rows}
        runtime_state_bus.snapshot.perception.latest["health"] = by_id.get("perception", {})
        runtime_state_bus.snapshot.control.latest["health"] = by_id.get("control", {})
        runtime_state_bus.snapshot.mission.latest["health"] = by_id.get("mission", {})
        runtime_state_bus.snapshot.payload.latest["health"] = by_id.get("payload", {})
        runtime_state_bus.snapshot.configuration.latest["health"] = by_id.get("configuration", {})
        runtime_state_bus.snapshot.system.latest["supervision_health"] = by_id.get("supervision", {})
        return {"subsystems": rows}

    @app.get("/mission/status", response_model=MissionDomainState)
    def mission_status_endpoint(session_metadata: SessionMetadata = Depends(require_browser_session)) -> MissionDomainState:
        del session_metadata
        state = effective_mission_state()
        runtime_state_bus.snapshot.mission = state
        return state

    @app.get("/operations/status", response_model=OperationDomainState)
    def operations_status(session_metadata: SessionMetadata = Depends(require_browser_session)) -> OperationDomainState:
        del session_metadata
        state = operation_domain_state()
        runtime_state_bus.snapshot.operation = state
        return state

    @app.get("/payload/status", response_model=PayloadDomainState)
    def payload_status_endpoint(session_metadata: SessionMetadata = Depends(require_browser_session)) -> PayloadDomainState:
        del session_metadata
        state = runtime_payload_status.state(permission=runtime_payload_permission.gripper_permission())
        runtime_state_bus.snapshot.payload = state
        return state

    @app.get("/perception/status", response_model=PerceptionDomainState)
    def perception_status_endpoint(session_metadata: SessionMetadata = Depends(require_browser_session)) -> PerceptionDomainState:
        del session_metadata
        state = runtime_perception_status.perception_state(
            permission=runtime_perception_permission.mutating_permission("perception")
        )
        runtime_state_bus.snapshot.perception = state
        return state

    @app.get("/powerline/status", response_model=PowerlineDomainState)
    def powerline_status_endpoint(session_metadata: SessionMetadata = Depends(require_browser_session)) -> PowerlineDomainState:
        del session_metadata
        state = runtime_perception_status.powerline_state(
            permission=runtime_perception_permission.mutating_permission("perception")
        )
        runtime_state_bus.snapshot.powerline = state
        return state

    @app.get("/rosbag/status", response_model=RosbagDomainState)
    def rosbag_status(session_metadata: SessionMetadata = Depends(require_browser_session)) -> RosbagDomainState:
        del session_metadata
        state = runtime_rosbag.state()
        runtime_state_bus.snapshot.rosbag = state
        return state

    @app.get("/rosbags")
    def list_rosbags(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return {"recordings": runtime_rosbag.adapter.list_recordings()}

    @app.get("/rosbags/{recording_id}/download")
    def download_rosbag(recording_id: str, session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return runtime_rosbag.adapter.download(recording_id)

    @app.get("/configuration/manifest")
    def configuration_manifest(session_metadata: SessionMetadata = Depends(require_browser_session)):
        del session_metadata
        manifest = runtime_configuration.manifest()
        runtime_state_bus.snapshot.configuration = runtime_configuration.state()
        return manifest

    @app.get("/configuration/status")
    def configuration_status(session_metadata: SessionMetadata = Depends(require_browser_session)):
        del session_metadata
        state = runtime_configuration.state()
        runtime_state_bus.snapshot.configuration = state
        return state

    @app.post("/configuration/apply")
    def configuration_apply(
        request: ConfigurationApplyRequest,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ):
        del session_metadata
        response = runtime_configuration.apply(request)
        runtime_state_bus.snapshot.configuration = runtime_configuration.state()
        return response

    @app.get("/configuration/snapshots")
    def configuration_snapshots(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return {"snapshots": [snapshot.model_dump(mode="json") for snapshot in runtime_configuration.list_snapshots()]}

    @app.post("/configuration/snapshots/save")
    def configuration_snapshot_save(
        request: SnapshotSaveRequest,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ):
        del session_metadata
        response = runtime_configuration.save_snapshot(request)
        runtime_state_bus.snapshot.configuration = runtime_configuration.state()
        return response

    @app.post("/configuration/snapshots/load")
    def configuration_snapshot_load(
        request: SnapshotLoadRequest,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ):
        del session_metadata
        response = runtime_configuration.load_snapshot(request)
        runtime_state_bus.snapshot.configuration = runtime_configuration.state()
        return response

    @app.get("/configuration/snapshots/{snapshot_id:path}/download")
    def configuration_snapshot_download(
        snapshot_id: str,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ) -> dict:
        del session_metadata
        return runtime_configuration.download_snapshot(SnapshotDownloadRequest(snapshot_id=snapshot_id))

    @app.post("/configuration/snapshots/default")
    def configuration_snapshot_set_default(
        request: SnapshotSetDefaultRequest,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ):
        del session_metadata
        response = runtime_configuration.set_default_snapshot(request)
        runtime_state_bus.snapshot.configuration = runtime_configuration.state()
        return response

    @app.get("/px4/status", response_model=VehicleDomainState)
    def px4_status(session_metadata: SessionMetadata = Depends(require_browser_session)) -> VehicleDomainState:
        del session_metadata
        state = vehicle_state_with_awareness()
        runtime_state_bus.snapshot.vehicle = state
        return state

    @app.get("/vehicle/status", response_model=VehicleDomainState)
    def vehicle_status(session_metadata: SessionMetadata = Depends(require_browser_session)) -> VehicleDomainState:
        del session_metadata
        state = vehicle_state_with_awareness()
        runtime_state_bus.snapshot.vehicle = state
        return state

    @app.get("/control/status", response_model=ControlDomainState)
    def control_status(session_metadata: SessionMetadata = Depends(require_browser_session)) -> ControlDomainState:
        del session_metadata
        state = control_state_with_awareness()
        runtime_state_bus.snapshot.control = state
        return state

    @app.get("/map/state", response_model=MapState)
    def map_state(session_metadata: SessionMetadata = Depends(require_browser_session)) -> MapState:
        del session_metadata
        state = runtime_map.state(force=True)
        runtime_state_bus.snapshot.map = map_domain_state(state)
        return state

    @app.get("/simulation/status")
    def simulation_status(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return simulation_response(runtime_simulation.status().as_dict())

    @app.post("/simulation/backend/start")
    def simulation_backend_start(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return simulation_response(runtime_simulation.start_backend().as_dict())

    @app.post("/simulation/backend/stop")
    def simulation_backend_stop(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return simulation_response(runtime_simulation.stop_backend().as_dict())

    @app.post("/session/login", response_model=LoginResponse)
    def login(request: LoginRequest, http_request: Request) -> LoginResponse:
        if request.password != runtime_settings.browser_password:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid password")
        try:
            metadata = browser_sessions.acquire(
                client_label=request.client_label,
                client_address=http_request.client.host if http_request.client else None,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return LoginResponse(session_token=metadata.session_token)

    @app.post("/session/logout")
    def logout(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict[str, bool]:
        browser_sessions.release(session_metadata.session_token)
        return {"released": True}

    @app.get("/session", response_model=SessionResponse)
    def session(session_metadata: SessionMetadata = Depends(require_browser_session)) -> SessionResponse:
        return SessionResponse.from_metadata(session_metadata, runtime_settings)

    @app.post("/session/heartbeat", response_model=SessionResponse)
    def heartbeat(session_metadata: SessionMetadata = Depends(require_browser_session)) -> SessionResponse:
        metadata = browser_sessions.heartbeat(session_metadata.session_token)
        return SessionResponse.from_metadata(metadata, runtime_settings)

    @app.post("/commands/actions/start", response_model=ActionStartResponse)
    def start_action(
        request: CommandRequest,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ) -> ActionStartResponse:
        del session_metadata
        response, result = dispatcher.start_action(request)
        if response.accepted:
            publish_vehicle_control_refresh()
        if result is not None:
            # The full action lifecycle is streamed by concrete handlers later;
            # this immediate accepted marker keeps the boundary contract wired.
            try:
                asyncio.get_running_loop().create_task(runtime_state_bus.send_command_result(result))
            except RuntimeError:
                loop = loop_holder.get("loop")
                if loop is not None and loop.is_running():
                    asyncio.run_coroutine_threadsafe(runtime_state_bus.send_command_result(result), loop)
        return response

    @app.post("/commands/services/call", response_model=ServiceCallResponse)
    def call_service(
        request: ServiceCallRequest,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ) -> ServiceCallResponse:
        del session_metadata
        return dispatcher.call_service(request)

    @app.get("/commands/handlers")
    def command_handlers(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return dispatcher.metadata()

    @app.get("/cli/readiness", response_model=CommandResponse)
    def cli_readiness(cli_token: str = Depends(require_cli_token)) -> CommandResponse:
        del cli_token
        return CommandResponse(
            request_id="cli-readiness",
            command_id="runtime.status",
            accepted=True,
            result={"api": "up"},
        )

    @app.post("/cli/commands", response_model=CommandResponse)
    def cli_command(
        request: CommandRequest,
        cli_token: str = Depends(require_cli_token),
    ) -> CommandResponse:
        del cli_token
        permission = dispatcher.action_permission(request.command_id)
        active_browser = browser_sessions.active()
        if permission is not None and permission != HandlerPermission.READ_ONLY and active_browser is not None:
            reason = "mutating remote CLI command blocked while browser GUI session is active"
            event_log.record_cli_rejection(
                command_id=request.command_id,
                request_id=request.request_id,
                client_label=request.client_label,
                reason=reason,
            )
            return CommandResponse(
                request_id=request.request_id,
                command_id=request.command_id,
                accepted=False,
                rejection=CommandRejection(
                    code=ErrorCode.CONFLICT,
                    message=reason,
                    request_id=request.request_id,
                    command_id=request.command_id,
                    details={"active_browser_client": active_browser.client_label},
                ),
            )
        response, _result = dispatcher.start_action(request)
        return CommandResponse(
            request_id=response.request_id,
            command_id=response.command_id,
            accepted=response.accepted,
            message=response.message,
            rejection=response.rejection,
            result=response.result,
        )

    @app.get("/events/recent", response_model=list[OperatorEvent])
    def recent_events(session_metadata: SessionMetadata = Depends(require_browser_session)) -> list[OperatorEvent]:
        del session_metadata
        return event_log.recent()

    @app.post("/runtime/daemon/start")
    def runtime_daemon_start(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return runtime_system.start_daemon().as_dict()

    @app.post("/runtime/daemon/restart")
    def runtime_daemon_restart(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return runtime_system.restart_daemon().as_dict()

    @app.get("/runtime/daemon/nodes")
    def runtime_daemon_nodes(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return {"managed_nodes": runtime_system.list_nodes()}

    @app.get("/runtime/daemon/services")
    def runtime_daemon_services(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return {"services": runtime_system.list_services()}

    @app.get("/runtime/daemon/log-dir/{entity_id}")
    def runtime_daemon_log_dir(
        entity_id: str,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ) -> dict:
        del session_metadata
        return {"entity_id": entity_id, "log_dir": runtime_system.log_dir(entity_id)}

    @app.get("/logs/sources")
    def logs_sources(session_metadata: SessionMetadata = Depends(require_browser_session)) -> dict:
        del session_metadata
        return {"sources": [source.as_dict() for source in runtime_logs.list_sources()]}

    @app.get("/logs/{source_id}/tail")
    def logs_tail(
        source_id: str,
        lines: int = 200,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ) -> dict:
        del session_metadata
        return {"source_id": source_id, "lines": runtime_logs.tail(source_id, lines=lines)}

    @app.get("/logs/{source_id}/download")
    def logs_download(
        source_id: str,
        session_metadata: SessionMetadata = Depends(require_browser_session),
    ) -> dict:
        del session_metadata
        return {"source_id": source_id, "content": runtime_logs.download(source_id)}

    @app.get("/cli/logs/sources")
    def cli_logs_sources(cli_token: str = Depends(require_cli_token)) -> dict:
        del cli_token
        return {"sources": [source.as_dict() for source in runtime_logs.list_sources()]}

    @app.get("/cli/logs/{source_id}/tail")
    def cli_logs_tail(
        source_id: str,
        lines: int = 200,
        cli_token: str = Depends(require_cli_token),
    ) -> dict:
        del cli_token
        return {"source_id": source_id, "lines": runtime_logs.tail(source_id, lines=lines)}

    @app.websocket("/logs/follow/{source_id}")
    async def logs_follow(websocket: WebSocket, source_id: str, token: str | None = None):
        try:
            browser_sessions.validate(token or "")
        except RuntimeError:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        await websocket.accept()
        try:
            async for row in runtime_logs.follow(source_id, initial_lines=200):
                await websocket.send_json(row)
        except KeyError:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        except WebSocketDisconnect:
            return

    @app.websocket("/ws")
    async def websocket(websocket: WebSocket):
        token = websocket.query_params.get("token")
        try:
            browser_sessions.validate(token or "")
        except RuntimeError:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        hydrate_state_snapshot()
        connected = await runtime_state_bus.connect(websocket)
        if not connected:
            return
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            runtime_state_bus.disconnect(websocket)
            return

    return app
