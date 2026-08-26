"""Remote CLI authentication conflict policy."""

from __future__ import annotations

from iii_drone_contracts import CommandId, HandlerPermission


READ_ONLY_COMMANDS = {
    CommandId.RUNTIME_STATUS.value,
    CommandId.RUNTIME_LIST_ENTITIES.value,
    CommandId.RUNTIME_LIST_SERVICES.value,
    CommandId.ROSBAG_LIST.value,
    CommandId.MISSION_CATALOG_STATUS.value,
    CommandId.MISSION_CATALOG_LIST.value,
    CommandId.MISSION_CATALOG_SHOW.value,
}


def classify_cli_command(command_id: str) -> HandlerPermission:
    if command_id in READ_ONLY_COMMANDS:
        return HandlerPermission.READ_ONLY
    return HandlerPermission.MUTATING
