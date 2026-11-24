"""The flight controller's parameters against the profile's PX4 baseline.

On real and opti_track the PX4 that flies is the physical flight controller.
Its transport (and, for opti_track, its estimator and failsafes) must match
the profile's baseline in ``deployment/px4/parameters``; otherwise the stack starts
against a flight controller configured for something else. The baseline is
read and written through the Runtime API's own MAVLink connection, which owns
the Pi's MAVLink port.
"""

from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path
import threading
import time
from typing import Any

from iii_drone_contracts.px4_parameters import (
    APPLY_COMMAND,
    BASELINE_DIRECTORY,
    CHECKED_PROFILES,
    BaselineError,
    describe,
    load_baseline,
    mismatches,
)

# A rehearsal flies an aircraft profile against a simulated PX4, whose
# transport parameters legitimately differ from the flight controller's.
SIMULATED_PX4_ENV = "III_PX4_SIMULATED"
BASELINE_DIRECTORY_ENV = "III_PX4_BASELINE_DIR"
_AUTOSAVE_SETTLE_SECONDS = 3.0
_REBOOT_DROP_TIMEOUT_SECONDS = 30.0
_REBOOT_RETURN_TIMEOUT_SECONDS = 90.0


def default_baseline_directory() -> Path:
    configured = os.environ.get(BASELINE_DIRECTORY_ENV)
    return Path(configured) if configured else Path.cwd() / BASELINE_DIRECTORY


def _stack_domain() -> int | None:
    try:
        return int(os.environ["ROS_DOMAIN_ID"])
    except (KeyError, ValueError):
        return None


class Px4ParameterBaseline:
    def __init__(
        self,
        *,
        profile: str,
        adapter: Any,
        directory: Path | None = None,
        ros_domain_id: int | None = None,
        simulated_px4: bool | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.profile = profile
        self.adapter = adapter
        self.directory = directory or default_baseline_directory()
        self.ros_domain_id = _stack_domain() if ros_domain_id is None else ros_domain_id
        self.simulated_px4 = (
            os.environ.get(SIMULATED_PX4_ENV, "") == "1"
            if simulated_px4 is None
            else simulated_px4
        )
        self._sleep = sleep
        self._clock = clock
        self._apply_lock = threading.Lock()

    @property
    def apply_command(self) -> str:
        return APPLY_COMMAND.format(profile=self.profile)

    @property
    def applicable(self) -> bool:
        return self.profile in CHECKED_PROFILES and not self.simulated_px4

    def _baseline(self) -> dict[str, int | float]:
        return load_baseline(self.profile, self.directory, ros_domain_id=self.ros_domain_id)

    def _read(self, names: list[str]) -> dict[str, int | float | None]:
        return self.adapter.run_blocking(lambda adapter: adapter.read_parameters(names))

    def state(self) -> dict[str, Any]:
        """Compare the live parameters with the baseline; never raises.

        ``rejection`` says why the system must not boot or start against this
        flight controller, or is None.
        """

        state: dict[str, Any] = {
            "profile": self.profile,
            "applicable": self.applicable,
            "checked": False,
            "matches": None,
            "mismatches": [],
            "detail": None,
            "rejection": None,
            "apply_command": self.apply_command,
            "ros_domain_id": self.ros_domain_id,
        }
        if not self.applicable:
            state["detail"] = (
                "the PX4 is simulated"
                if self.simulated_px4
                else f"profile {self.profile} does not fly the physical flight controller"
            )
            return state
        try:
            baseline = self._baseline()
            state["parameters"] = len(baseline)
            actual = self._read(list(baseline))
        except BaselineError as exc:
            state["detail"] = str(exc)
        except Exception as exc:
            state["detail"] = f"the PX4 MAVLink link is not usable: {exc}"
        else:
            differences = mismatches(baseline, actual)
            state.update(
                checked=True,
                matches=not differences,
                mismatches=differences,
                detail=describe(differences) if differences else None,
            )
        if not state["matches"]:
            problem = (
                f"PX4 parameters differ from the {self.profile} baseline: {state['detail']}"
                if state["checked"]
                else f"PX4 parameters could not be checked against the {self.profile} "
                f"baseline: {state['detail']}"
            )
            state["rejection"] = f"{problem}. Run `{self.apply_command}`."
        return state

    def rejection_reason(self) -> str | None:
        return self.state()["rejection"]

    def apply(self) -> dict[str, Any]:
        """Write the differing parameters, reboot the flight controller, verify.

        The caller has already established that the aircraft is disarmed and
        landed.
        """

        if not self.applicable:
            raise RuntimeError(self.state()["detail"])
        if not self._apply_lock.acquire(blocking=False):
            raise RuntimeError("a PX4 parameter baseline is already being applied")
        try:
            baseline = self._baseline()
            before = self._read(list(baseline))
            differences = mismatches(baseline, before)
            if not differences:
                return {"profile": self.profile, "changed": [], "rebooted": False, "matches": True}
            for item in differences:
                name = str(item["name"])
                expected = baseline[name]
                # Write with the type PX4 reported; an unreadable parameter
                # takes the type of the baseline's literal.
                current = before.get(name)
                value = float(expected) if isinstance(current, float) else expected
                self.adapter.run_blocking(
                    lambda adapter, name=name, value=value: adapter.write_parameter(name, value)
                )
            written = mismatches(baseline, self._read(list(baseline)))
            if written:
                raise RuntimeError(
                    f"PX4 did not accept the baseline: {describe(written)}"
                )
            # PX4 saves changed parameters on its own shortly after a write.
            self._sleep(_AUTOSAVE_SETTLE_SECONDS)
            self.adapter.run_blocking(lambda adapter: adapter.reboot_flight_controller())
            self._wait_connected(False, _REBOOT_DROP_TIMEOUT_SECONDS, "did not reboot")
            self._wait_connected(
                True, _REBOOT_RETURN_TIMEOUT_SECONDS, "did not come back after its reboot"
            )
            after = self._verify_after_reboot(baseline)
            if after:
                raise RuntimeError(
                    f"PX4 lost the baseline over its reboot: {describe(after)}"
                )
            return {
                "profile": self.profile,
                "changed": differences,
                "rebooted": True,
                "matches": True,
            }
        finally:
            self._apply_lock.release()

    def _wait_connected(self, connected: bool, timeout: float, failure: str) -> None:
        deadline = self._clock() + timeout
        while self._clock() < deadline:
            if bool(self.adapter.status().connected) is connected:
                return
            self._sleep(0.5)
        raise RuntimeError(f"the flight controller {failure} within {timeout:.0f} s")

    def _verify_after_reboot(self, baseline: dict[str, int | float]) -> list[dict[str, Any]]:
        # The parameter server answers a little after the first heartbeat.
        deadline = self._clock() + 20.0
        differences: list[dict[str, Any]] = []
        while True:
            try:
                differences = mismatches(baseline, self._read(list(baseline)))
            except Exception:
                differences = [{"name": name, "expected": value, "actual": None}
                               for name, value in baseline.items()]
            if not differences or self._clock() >= deadline:
                return differences
            self._sleep(1.0)
