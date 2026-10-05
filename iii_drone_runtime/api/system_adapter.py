"""Systemd and daemon-socket adapter for iii-runtime-api."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from time import monotonic
from typing import Callable, Protocol

from iii_drone_runtime.daemon.client import DaemonClient


class SystemdRunner(Protocol):
    def is_active(self, service: str) -> bool: ...

    def start(self, service: str) -> None: ...

    def restart(self, service: str) -> None: ...


IS_ACTIVE_CACHE_SECONDS = 1.0


class SubprocessSystemdRunner:
    """systemctl calls; is-active results are reused briefly.

    Mission and system state read the daemon's unit state several times per
    vehicle-control refresh; a systemctl process per read was a measurable
    part of the runtime API's CPU on the Pi.
    """

    def __init__(self, clock: Callable[[], float] = monotonic):
        self._clock = clock
        self._lock = Lock()
        self._active: dict[str, tuple[float, bool]] = {}

    def is_active(self, service: str) -> bool:
        import subprocess

        with self._lock:
            cached = self._active.get(service)
            if cached is not None and self._clock() - cached[0] < IS_ACTIVE_CACHE_SECONDS:
                return cached[1]
        active = subprocess.run(["systemctl", "is-active", "--quiet", service], check=False).returncode == 0
        with self._lock:
            self._active[service] = (self._clock(), active)
        return active

    def start(self, service: str) -> None:
        import subprocess

        self._forget(service)
        try:
            subprocess.run(DaemonClient._systemctl_command("start", service), check=True)
        finally:
            self._forget(service)

    def restart(self, service: str) -> None:
        import subprocess

        self._forget(service)
        try:
            subprocess.run(DaemonClient._systemctl_command("restart", service), check=True)
        finally:
            self._forget(service)

    def _forget(self, service: str) -> None:
        with self._lock:
            self._active.pop(service, None)


@dataclass(frozen=True)
class RuntimeSystemStatus:
    api_state: str
    daemon_systemd_state: str
    daemon_socket_state: str
    runtime_booted: bool | None
    system_active: bool | None
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "api_state": self.api_state,
            "daemon_systemd_state": self.daemon_systemd_state,
            "daemon_socket_state": self.daemon_socket_state,
            "runtime_booted": self.runtime_booted,
            "system_active": self.system_active,
            "error": self.error,
        }


class RuntimeSystemAdapter:
    def __init__(
        self,
        *,
        daemon_client: DaemonClient | None = None,
        systemd: SystemdRunner | None = None,
        daemon_service: str = "iii-system-daemon.service",
    ):
        self.daemon_client = daemon_client or DaemonClient()
        self.systemd = systemd or SubprocessSystemdRunner()
        self.daemon_service = daemon_service

    def status(self) -> RuntimeSystemStatus:
        daemon_active = self.systemd.is_active(self.daemon_service)
        daemon_socket_up = False
        daemon_status: dict | None = None
        error: str | None = None
        try:
            runtime_status = getattr(self.daemon_client, "runtime_status", None)
            if callable(runtime_status):
                daemon_status = runtime_status()
                daemon_socket_up = True
            elif self.daemon_client.ping():
                daemon_status = self.daemon_client.status()
                daemon_socket_up = True
        except Exception as exc:  # pragma: no cover - defensive runtime path
            error = str(exc)

        managed_nodes = (daemon_status or {}).get("managed_nodes") or {}
        system_active = (daemon_status or {}).get("active")
        if system_active is None and managed_nodes:
            system_active = all(label == "active" for label in managed_nodes.values())

        return RuntimeSystemStatus(
            api_state="up",
            daemon_systemd_state="active" if daemon_active else "inactive",
            daemon_socket_state="responding" if daemon_socket_up else "unavailable",
            runtime_booted=daemon_status.get("booted") if daemon_status is not None else None,
            system_active=system_active,
            error=error,
        )

    def start_daemon(self) -> RuntimeSystemStatus:
        self.systemd.start(self.daemon_service)
        return self.status()

    def restart_daemon(self) -> RuntimeSystemStatus:
        self.systemd.restart(self.daemon_service)
        return self.status()

    def list_nodes(self) -> list[str]:
        return self.daemon_client.list_nodes()

    def list_services(self) -> list[str]:
        return self.daemon_client.list_services()

    def log_dir(self, entity_id: str) -> str:
        return self.daemon_client.log_dir(entity_id)
