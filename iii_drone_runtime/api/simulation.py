"""Simulation-profile runtime controls for PX4/Gazebo backend tooling."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
from typing import Protocol


SIMULATION_PROFILES = {"sim", "simulation"}


@dataclass(frozen=True)
class SimulationControlResult:
    ok: bool
    profile: str
    enabled: bool
    status: dict
    message: str | None = None
    disabled_reason: str | None = None

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "profile": self.profile,
            "enabled": self.enabled,
            "status": self.status,
            "message": self.message,
            "disabled_reason": self.disabled_reason,
        }


class SimulationToolAdapter(Protocol):
    def status(self) -> dict: ...

    def start_backend(self) -> dict: ...

    def stop_backend(self) -> dict: ...


class SubprocessSimulationToolAdapter:
    def __init__(self, script_path: Path | None = None):
        self.script_path = script_path or _default_simulation_script()

    def status(self) -> dict:
        return _parse_status_output(self._run("--status").stdout)

    def start_backend(self) -> dict:
        result = self._run("--headless", "--no-attach")
        status = self.status()
        status["start_stdout"] = result.stdout
        return status

    def stop_backend(self) -> dict:
        return _parse_status_output(self._run("--stop").stdout)

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.script_path), *args],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


class SimulationRuntimeController:
    def __init__(self, *, profile: str | None, adapter: SimulationToolAdapter | None = None):
        self.profile = profile or os.environ.get("III_SYSTEM_PROFILE", "unknown")
        self.adapter = adapter or SubprocessSimulationToolAdapter()

    def status(self) -> SimulationControlResult:
        if not self.enabled:
            return self._disabled("simulation controls are only available in simulation profile")
        return SimulationControlResult(
            ok=True,
            profile=self.profile,
            enabled=True,
            status=self.adapter.status(),
        )

    def start_backend(self) -> SimulationControlResult:
        if not self.enabled:
            return self._disabled("simulation backend start is disabled outside simulation profile")
        return SimulationControlResult(
            ok=True,
            profile=self.profile,
            enabled=True,
            status=self.adapter.start_backend(),
            message="simulation backend start requested",
        )

    def stop_backend(self) -> SimulationControlResult:
        if not self.enabled:
            return self._disabled("simulation backend stop is disabled outside simulation profile")
        return SimulationControlResult(
            ok=True,
            profile=self.profile,
            enabled=True,
            status=self.adapter.stop_backend(),
            message="simulation backend stop requested",
        )

    @property
    def enabled(self) -> bool:
        return self.profile in SIMULATION_PROFILES

    def _disabled(self, reason: str) -> SimulationControlResult:
        return SimulationControlResult(
            ok=False,
            profile=self.profile,
            enabled=False,
            status={
                "px4_gazebo": "disabled",
                "gazebo_transport": "disabled",
                "qgroundcontrol": "not_controlled",
            },
            disabled_reason=reason,
        )


def _default_simulation_script() -> Path:
    workspace_root = Path(os.environ.get("WORKSPACE_DIR", Path(__file__).resolve().parents[4]))
    return workspace_root / "tools" / "simulation" / "launch_simulation_tools.sh"


def _parse_status_output(output: str) -> dict:
    status: dict[str, str | list[str]] = {
        "tmux_session": "unknown",
        "px4_gazebo": "unknown",
        "gazebo_transport": "unknown",
        "qgroundcontrol": "not_controlled",
    }
    for raw_line in output.splitlines():
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key == "simulation_process_groups":
            status[key] = [] if value == "none" else value.split()
            status["px4_gazebo"] = "running" if value != "none" else "stopped"
            continue
        status[key] = value
    return status
