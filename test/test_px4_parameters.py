"""The flight controller's parameters against the profile's PX4 baseline."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from iii_drone_contracts import ActionStartResponse, CommandId, CommandRequest
from iii_drone_runtime.api.events import RuntimeEventLog
from iii_drone_runtime.api.px4_parameters import Px4ParameterBaseline
from iii_drone_runtime.api.runtime_commands import (
    PX4_BASELINE_GATED_COMMANDS,
    RuntimeCommandHandlers,
)

BASELINE = """param set UXRCE_DDS_PRT 8888
param set UXRCE_DDS_DOM_ID 42
param set EKF2_EV_DELAY 30
param set EKF2_EVP_NOISE 0.05
param save
reboot
"""
MATCHING = {"UXRCE_DDS_PRT": 8888, "UXRCE_DDS_DOM_ID": 42, "EKF2_EV_DELAY": 30.0,
            "EKF2_EVP_NOISE": 0.05000000074505806}


class _FlightController:
    """A PX4 seen through the adapter: parameters, a link, and a reboot."""

    def __init__(self, parameters, *, connected=True, keeps=True):
        self.parameters = dict(parameters)
        self.connected = connected
        self.keeps = keeps
        self.saved = dict(parameters)
        self.writes: list[tuple[str, object]] = []
        self.reboots = 0
        self._status_reads_until_back = 0

    def run_blocking(self, operation):
        return asyncio.run(operation(self))

    def status(self):
        if self._status_reads_until_back > 0:
            self._status_reads_until_back -= 1
            return SimpleNamespace(connected=False)
        return SimpleNamespace(connected=self.connected)

    async def read_parameters(self, names):
        if not self.connected:
            raise RuntimeError("PX4 command transport is not connected")
        return {name: self.parameters.get(name) for name in names}

    async def write_parameter(self, name, value):
        self.writes.append((name, value))
        self.parameters[name] = value
        if self.keeps:
            self.saved[name] = value

    async def reboot_flight_controller(self):
        self.reboots += 1
        self.parameters = dict(self.saved)
        self._status_reads_until_back = 2


def _baseline(tmp_path: Path, controller, *, profile="opti_track", **kwargs):
    for name in ("opti-track.nsh", "real.nsh", "hil-ethernet.nsh"):
        (tmp_path / name).write_text(BASELINE, encoding="utf-8")
    return Px4ParameterBaseline(
        profile=profile,
        adapter=controller,
        directory=tmp_path,
        ros_domain_id=kwargs.pop("ros_domain_id", 42),
        simulated_px4=kwargs.pop("simulated_px4", False),
        sleep=lambda _seconds: None,
        **kwargs,
    )


def test_a_matching_flight_controller_may_boot(tmp_path):
    baseline = _baseline(tmp_path, _FlightController(MATCHING))
    state = baseline.state()
    assert state["checked"] and state["matches"] and state["mismatches"] == []
    assert baseline.rejection_reason() is None


def test_a_differing_flight_controller_is_refused_and_names_the_command(tmp_path):
    controller = _FlightController({**MATCHING, "UXRCE_DDS_PRT": 8889, "EKF2_EV_DELAY": 0.0})
    reason = _baseline(tmp_path, controller).rejection_reason()
    assert reason == (
        "PX4 parameters differ from the opti_track baseline: UXRCE_DDS_PRT is 8889 "
        "(baseline 8888), EKF2_EV_DELAY is 0.0 (baseline 30). "
        "Run `iii px4 param-baseline --profile opti_track`."
    )


def test_an_unreadable_flight_controller_is_refused(tmp_path):
    reason = _baseline(tmp_path, _FlightController(MATCHING, connected=False)).rejection_reason()
    assert "could not be checked against the opti_track baseline" in reason
    assert "iii px4 param-baseline --profile opti_track" in reason


def test_the_dds_domain_is_held_to_the_provisioned_stack_domain(tmp_path):
    baseline = _baseline(tmp_path, _FlightController(MATCHING), ros_domain_id=7)
    assert baseline.state()["mismatches"] == [
        {"name": "UXRCE_DDS_DOM_ID", "expected": 7, "actual": 42}
    ]


@pytest.mark.parametrize(
    ("profile", "simulated"), [("hil", False), ("sim", False), ("opti_track", True)]
)
def test_profiles_that_fly_a_simulated_px4_are_not_checked(tmp_path, profile, simulated):
    controller = _FlightController({}, connected=False)
    baseline = _baseline(tmp_path, controller, profile=profile, simulated_px4=simulated)
    assert baseline.state()["applicable"] is False
    assert baseline.rejection_reason() is None
    with pytest.raises(RuntimeError):
        baseline.apply()


def test_apply_writes_only_the_differences_with_px4s_types_then_reboots_and_verifies(tmp_path):
    controller = _FlightController({**MATCHING, "UXRCE_DDS_PRT": 8889, "EKF2_EV_DELAY": 0.0})
    result = _baseline(tmp_path, controller).apply()
    # EKF2_EV_DELAY is a float32 on PX4 although the baseline writes "30".
    assert controller.writes == [("UXRCE_DDS_PRT", 8888), ("EKF2_EV_DELAY", 30.0)]
    assert isinstance(controller.writes[1][1], float)
    assert controller.reboots == 1
    assert result["rebooted"] and result["matches"]
    assert [item["name"] for item in result["changed"]] == ["UXRCE_DDS_PRT", "EKF2_EV_DELAY"]


def test_apply_leaves_a_matching_flight_controller_alone(tmp_path):
    controller = _FlightController(MATCHING)
    result = _baseline(tmp_path, controller).apply()
    assert result == {"profile": "opti_track", "changed": [], "rebooted": False, "matches": True}
    assert controller.writes == [] and controller.reboots == 0


def test_apply_fails_when_the_baseline_does_not_survive_the_reboot(tmp_path):
    controller = _FlightController({**MATCHING, "UXRCE_DDS_PRT": 8889}, keeps=False)
    ticks = iter(range(0, 100000, 5))
    baseline = _baseline(tmp_path, controller, clock=lambda: float(next(ticks)))
    with pytest.raises(RuntimeError, match="lost the baseline over its reboot"):
        baseline.apply()


class _Daemon:
    def __init__(self):
        self.calls = []

    def boot(self, profile):
        self.calls.append("boot")
        return {"success": True}

    def start(self, **_kwargs):
        self.calls.append("start")
        return {"success": True}

    def stop(self, **_kwargs):
        self.calls.append("stop")
        return {"success": True}

    def shutdown(self, **_kwargs):
        self.calls.append("shutdown")
        return {"success": True}


def _handle(handlers, command_id) -> ActionStartResponse:
    return handlers.handle(
        CommandRequest(request_id="r", command_id=command_id, client_label="test", parameters={})
    )


def test_boot_and_start_are_refused_on_a_baseline_mismatch_but_stopping_is_not():
    daemon = _Daemon()
    handlers = RuntimeCommandHandlers(
        daemon_client=daemon,
        event_log=RuntimeEventLog(),
        px4_baseline_rejection=lambda: "PX4 parameters differ. Run `iii px4 param-baseline`.",
    )
    assert PX4_BASELINE_GATED_COMMANDS == {
        CommandId.RUNTIME_BOOT.value,
        CommandId.RUNTIME_SYSTEM_START.value,
        CommandId.RUNTIME_START.value,
    }
    for command_id in (CommandId.RUNTIME_BOOT.value, CommandId.RUNTIME_START.value):
        response = _handle(handlers, command_id)
        assert response.accepted is False
        assert "iii px4 param-baseline" in response.message
    assert daemon.calls == []
    assert _handle(handlers, CommandId.RUNTIME_STOP.value).accepted is True
    assert _handle(handlers, CommandId.RUNTIME_SHUTDOWN.value).accepted is True
    assert daemon.calls == ["stop", "shutdown"]


def test_boot_proceeds_when_the_baseline_matches():
    daemon = _Daemon()
    handlers = RuntimeCommandHandlers(
        daemon_client=daemon, event_log=RuntimeEventLog(), px4_baseline_rejection=lambda: None
    )
    assert _handle(handlers, CommandId.RUNTIME_BOOT.value).accepted is True
    assert daemon.calls == ["boot"]
