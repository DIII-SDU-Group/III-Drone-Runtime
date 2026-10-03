import subprocess

from iii_drone_runtime.api.clock_sync import (
    ChronyClockMonitor,
    ClockSyncState,
    parse_chronyc_tracking,
)
from test_flight_commands import _gate, _vehicle
from iii_drone_contracts import CommandId

SYNCED = (
    "D9C6DB66,217.198.219.102,2,1790654333.879468253,0.000071271,-0.000004221,"
    "0.000195147,8.727,0.001,0.009,0.011298233,0.000460433,1026.1,Normal\n"
)
UNSYNCED = "00000000,,0,0.000000000,0.000000000,0.000000000,0.000000000,0.000,0.000,0.000,1.0,1.0,0.0,Not synchronised\n"


def _with_offset(offset: str) -> str:
    fields = SYNCED.strip().split(",")
    fields[4] = offset
    return ",".join(fields)


def test_synchronized_clock_with_small_offset_is_settled():
    state = parse_chronyc_tracking(SYNCED)
    assert state.applicable and state.settled
    assert state.leap_status == "Normal"
    assert state.reference == "217.198.219.102"
    assert abs(state.system_offset_seconds - 0.000071271) < 1e-12


def test_unsynchronized_clock_may_still_step():
    state = parse_chronyc_tracking(UNSYNCED)
    assert not state.settled
    assert "may step" in state.detail


def test_large_residual_offset_in_either_direction_is_not_settled():
    for offset in ("0.25", "-0.25"):
        state = parse_chronyc_tracking(_with_offset(offset))
        assert not state.settled
        assert "residual offset" in state.detail


def test_malformed_output_is_not_settled():
    assert not parse_chronyc_tracking("garbage").settled
    assert not parse_chronyc_tracking(_with_offset("nan?")).settled


def test_monitor_is_not_applicable_off_aircraft_and_never_runs_chronyc():
    def tracking():
        raise AssertionError("chronyc must not run off-aircraft")

    state = ChronyClockMonitor(enabled=False, tracking=tracking).state()
    assert state == ClockSyncState(applicable=False, settled=True, detail="simulation host clock")


def test_monitor_caches_and_reports_missing_chronyc():
    now = [0.0]
    calls = []

    def tracking():
        calls.append(now[0])
        raise FileNotFoundError("chronyc")

    monitor = ChronyClockMonitor(enabled=True, tracking=tracking, monotonic=lambda: now[0])
    assert not monitor.state().settled
    now[0] = 4.0
    monitor.state()
    assert calls == [0.0]
    now[0] = 5.5
    assert "unavailable" in monitor.state().detail
    assert calls == [0.0, 5.5]


def test_monitor_reports_chronyc_timeout():
    def tracking():
        raise subprocess.TimeoutExpired("chronyc", 2)

    assert not ChronyClockMonitor(enabled=True, tracking=tracking).state().settled


def test_unsettled_clock_blocks_arming_but_not_hold():
    gate = _gate(vehicle=_vehicle(armed=False, in_air=False))
    gate.clock_state_provider = lambda: parse_chronyc_tracking(UNSYNCED)
    reasons = gate.disabled_reasons(CommandId.PX4_ARM.value)
    assert len(reasons) == 1 and reasons[0].startswith("onboard clock is not settled")
    assert gate.disabled_reasons(CommandId.PX4_HOLD.value) == []

    gate.clock_state_provider = lambda: parse_chronyc_tracking(SYNCED)
    assert gate.disabled_reasons(CommandId.PX4_ARM.value) == []
    gate.clock_state_provider = ChronyClockMonitor(enabled=False).state
    assert gate.disabled_reasons(CommandId.PX4_ARM.value) == []


def _preflight_clock_item(profile: str, monitor: ChronyClockMonitor | None = None) -> dict:
    from fastapi.testclient import TestClient

    from iii_drone_runtime.api.app import RuntimeApiSettings, create_app

    app = create_app(settings=RuntimeApiSettings(profile=profile), clock_monitor=monitor)
    items = TestClient(app).get("/mission/status").json()["preflight"]["items"]
    return next(item for item in items if item["key"] == "clock")


def test_aircraft_preflight_hard_gates_an_unsettled_clock():
    item = _preflight_clock_item(
        "hil", ChronyClockMonitor(enabled=True, tracking=lambda: UNSYNCED)
    )
    assert item["hard_gate"] is True and item["passed"] is False
    assert "may step" in item["detail"]


def test_simulation_preflight_reports_the_clock_as_not_applicable():
    item = _preflight_clock_item("sim")
    assert item["hard_gate"] is False and item["passed"] is True
