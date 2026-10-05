"""Which operator commands the active runtime profile supports.

The ``opti_track`` profile flies "flight basics" in a motion-capture lab: there
is no cable, payload, powerline perception or overview, so its runtime accepts
only an explicit allowlist of commands. Anything else, including commands added
later, is rejected with ``ErrorCode.PROFILE_RESTRICTED``. Other profiles are
unrestricted.
"""

from __future__ import annotations

from iii_drone_contracts import (
    CommandId,
    CommandRejection,
    CommandRequest,
    ErrorCode,
    ProfileCapabilities,
)

from .custom_operations import SUPPORTED_OPERATIONS
from .operation_commands import OPERATION_START_COMMANDS
from .simulation import SIMULATION_PROFILES


FLIGHT_BASICS_PROFILES = frozenset({"opti_track"})

# Custom operations that need no cable, powerline or perception target.
FLIGHT_BASICS_CUSTOM_OPERATIONS = frozenset({"fly_to_position", "follow_waypoint_path", "hover"})

FLIGHT_BASICS_COMMANDS = frozenset(
    {
        CommandId.PX4_ARM.value,
        CommandId.PX4_TAKEOFF.value,
        CommandId.PX4_LAND.value,
        CommandId.PX4_HOLD.value,
        CommandId.MISSION_ACTIVATE.value,
        CommandId.MISSION_PROCEED.value,
        CommandId.MISSION_CATALOG_STATUS.value,
        CommandId.MISSION_CATALOG_LIST.value,
        CommandId.MISSION_CATALOG_SHOW.value,
        CommandId.MISSION_CATALOG_SELECT.value,
        CommandId.CUSTOM_OPERATION_ACTIVATE.value,
        CommandId.CUSTOM_OPERATION_VALIDATE.value,
        CommandId.CUSTOM_OPERATION_CANCEL.value,
        *(
            command_id
            for command_id, operation in OPERATION_START_COMMANDS.items()
            if operation in FLIGHT_BASICS_CUSTOM_OPERATIONS
        ),
        CommandId.CONFIGURATION_APPLY.value,
        CommandId.CONFIGURATION_SAVE_SNAPSHOT.value,
        CommandId.CONFIGURATION_LOAD_SNAPSHOT.value,
        CommandId.CONFIGURATION_DOWNLOAD_SNAPSHOT.value,
        CommandId.CONFIGURATION_SET_DEFAULT_SNAPSHOT.value,
        CommandId.CONFIGURATION_LIST_SNAPSHOTS.value,
        CommandId.RUNTIME_BOOT.value,
        CommandId.RUNTIME_SYSTEM_START.value,
        CommandId.RUNTIME_START.value,
        CommandId.RUNTIME_STOP.value,
        CommandId.RUNTIME_RESTART.value,
        CommandId.RUNTIME_PARAMETER_COLD_RESTART.value,
        CommandId.RUNTIME_SHUTDOWN.value,
        CommandId.RUNTIME_SERVICE_START.value,
        CommandId.RUNTIME_SERVICE_STOP.value,
        CommandId.RUNTIME_SERVICE_RESTART.value,
        CommandId.RUNTIME_STATUS.value,
        CommandId.RUNTIME_LIST_ENTITIES.value,
        CommandId.RUNTIME_LIST_SERVICES.value,
        CommandId.ROSBAG_START.value,
        CommandId.ROSBAG_STOP.value,
        CommandId.ROSBAG_LIST.value,
        CommandId.ROSBAG_DOWNLOAD.value,
    }
)

PAYLOAD_CONTROL = "payload control"
POWERLINE_PERCEPTION = "powerline perception"
OVERVIEW_CAPTURE = "overview capture"

OVERVIEW_COMMANDS = frozenset(
    {
        CommandId.POWERLINE_OVERVIEW_UPDATE.value,
        CommandId.PYLON_CAPTURE_CURRENT.value,
        CommandId.PYLON_OVERVIEW_CLEAR.value,
    }
)


class RuntimeProfilePolicy:
    """The operator surfaces of one runtime profile."""

    def __init__(self, profile: str | None):
        self.profile = profile

    @property
    def flight_basics(self) -> bool:
        return self.profile in FLIGHT_BASICS_PROFILES

    def unavailable(self, thing: str) -> str:
        return f"{thing} is not available in the {self.profile} profile"

    def restriction(self, label: str) -> str | None:
        """Why a whole operator surface is unavailable, or None."""
        return self.unavailable(label) if self.flight_basics else None

    def custom_operation_rejection(self, operation: str) -> str | None:
        if not self.flight_basics or not operation or operation in FLIGHT_BASICS_CUSTOM_OPERATIONS:
            return None
        return self.unavailable(f"custom operation {operation}")

    def command_rejection(self, request: CommandRequest) -> str | None:
        if not self.flight_basics:
            return None
        command_id = request.command_id
        if command_id == CommandId.CUSTOM_OPERATION_VALIDATE.value:
            operation = (request.parameters or {}).get("operation", "")
            return self.custom_operation_rejection(str(operation))
        if command_id in FLIGHT_BASICS_COMMANDS:
            return None
        return self.unavailable(_restricted_surface(command_id))

    def rejection(self, request: CommandRequest) -> CommandRejection | None:
        reason = self.command_rejection(request)
        if reason is None:
            return None
        return CommandRejection(
            code=ErrorCode.PROFILE_RESTRICTED,
            message=reason,
            request_id=request.request_id,
            command_id=request.command_id,
            retryable=False,
        )

    def capabilities(self) -> ProfileCapabilities:
        restricted = self.flight_basics
        return ProfileCapabilities(
            profile=self.profile,
            payload_available=not restricted,
            perception_available=not restricted,
            overviews_available=not restricted,
            cable_intents_available=not restricted,
            simulation_available=self.profile in SIMULATION_PROFILES,
            # Advertise only what this runtime can actually start.
            custom_operations=(
                sorted(FLIGHT_BASICS_CUSTOM_OPERATIONS & SUPPORTED_OPERATIONS) if restricted else None
            ),
        )


def _restricted_surface(command_id: str) -> str:
    if command_id.startswith("payload."):
        return PAYLOAD_CONTROL
    if command_id.startswith("perception."):
        return POWERLINE_PERCEPTION
    if command_id in OVERVIEW_COMMANDS:
        return OVERVIEW_CAPTURE
    if command_id in {
        CommandId.MISSION_RECHARGE_NOW.value,
        CommandId.MISSION_STAY_ON_CABLE.value,
        CommandId.MISSION_LEAVE_CABLE_NOW.value,
    }:
        return f"cable intent {command_id}"
    operation = OPERATION_START_COMMANDS.get(command_id)
    if operation is not None:
        return f"custom operation {operation}"
    return f"command {command_id}"
