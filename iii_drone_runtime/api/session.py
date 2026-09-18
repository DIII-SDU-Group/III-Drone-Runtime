"""Developer session metadata used for API response compatibility."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SessionMetadata:
    session_token: str
    acquired_at: datetime
    last_heartbeat_at: datetime
    client_label: str | None = None
    client_address: str | None = None
