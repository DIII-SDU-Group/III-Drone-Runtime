"""Browser session lease management for iii-runtime-api."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import secrets
from threading import RLock
from typing import Callable


def utc_from_seconds(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


@dataclass(frozen=True)
class SessionMetadata:
    session_token: str
    acquired_at: datetime
    last_heartbeat_at: datetime
    client_label: str | None = None
    client_address: str | None = None


class BrowserSessionLease:
    def __init__(
        self,
        *,
        lease_timeout_seconds: float = 8.0,
        token_factory: Callable[[], str] | None = None,
        time_fn: Callable[[], float] | None = None,
    ):
        self.lease_timeout_seconds = lease_timeout_seconds
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(32))
        self._time_fn = time_fn
        self._session: SessionMetadata | None = None
        self._lock = RLock()

    def _now_seconds(self) -> float:
        if self._time_fn is not None:
            return self._time_fn()
        return datetime.now(timezone.utc).timestamp()

    def _now(self) -> datetime:
        return utc_from_seconds(self._now_seconds())

    def active(self) -> SessionMetadata | None:
        with self._lock:
            if self._session is None:
                return None
            if self.is_expired(self._session.session_token):
                self._session = None
                return None
            return self._session

    def is_expired(self, session_token: str) -> bool:
        with self._lock:
            if self._session is None or self._session.session_token != session_token:
                return True
            age = self._now_seconds() - self._session.last_heartbeat_at.timestamp()
            return age > self.lease_timeout_seconds

    def acquire(self, *, client_label: str | None, client_address: str | None) -> SessionMetadata:
        with self._lock:
            active = self.active()
            if active is not None:
                raise RuntimeError("another browser session is already active")

            now = self._now()
            self._session = SessionMetadata(
                session_token=self._token_factory(),
                acquired_at=now,
                last_heartbeat_at=now,
                client_label=client_label,
                client_address=client_address,
            )
            return self._session

    def validate(self, session_token: str) -> SessionMetadata:
        with self._lock:
            active = self.active()
            if active is None or active.session_token != session_token:
                raise RuntimeError("missing or invalid browser session token")
            return active

    def heartbeat(self, session_token: str) -> SessionMetadata:
        with self._lock:
            active = self.validate(session_token)
            self._session = SessionMetadata(
                session_token=active.session_token,
                acquired_at=active.acquired_at,
                last_heartbeat_at=self._now(),
                client_label=active.client_label,
                client_address=active.client_address,
            )
            return self._session

    def release(self, session_token: str) -> None:
        with self._lock:
            self.validate(session_token)
            self._session = None
