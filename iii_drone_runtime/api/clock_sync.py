"""Host clock settledness for aircraft preflight.

The onboard computer has no trusted RTC, so chrony steps its clock (makestep)
when it first reaches a time source. A step while flying moves every ROS
timestamp at once and, with PX4 uXRCE-DDS time synchronization, PX4's synced
timestamps too. Arming and mission activation therefore require that chrony is
already synchronized with only a small residual offset, which it corrects by
slewing rather than stepping.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import subprocess
import time
from typing import Any, Callable

# chrony slews up to 83 ms/s, so a residual offset below this is removed within
# about a second without a step.
DEFAULT_MAX_OFFSET_SECONDS = 0.1
DEFAULT_CACHE_SECONDS = 5.0
CHRONYC_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class ClockSyncState:
    applicable: bool
    settled: bool
    detail: str
    leap_status: str | None = None
    system_offset_seconds: float | None = None
    reference: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_chronyc_tracking() -> str:
    return subprocess.run(
        ["chronyc", "-c", "tracking"],
        check=True,
        capture_output=True,
        text=True,
        timeout=CHRONYC_TIMEOUT_SECONDS,
    ).stdout


def parse_chronyc_tracking(
    output: str, *, max_offset_seconds: float = DEFAULT_MAX_OFFSET_SECONDS
) -> ClockSyncState:
    """Judge `chronyc -c tracking` CSV output."""
    fields = output.strip().split(",")
    if len(fields) < 14:
        return ClockSyncState(
            applicable=True, settled=False, detail="unrecognized chronyc tracking output"
        )
    reference = fields[1] or fields[0]
    leap_status = fields[13]
    try:
        offset = float(fields[4])
    except ValueError:
        return ClockSyncState(
            applicable=True,
            settled=False,
            detail="unrecognized chronyc system time offset",
            leap_status=leap_status,
            reference=reference,
        )
    if leap_status != "Normal":
        detail = (
            f"chrony is not synchronized ({leap_status}); the clock may step "
            "when a time source appears"
        )
        settled = False
    elif abs(offset) > max_offset_seconds:
        detail = (
            f"clock is {offset:+.3f} s from {reference}; wait until the residual "
            f"offset is within {max_offset_seconds:.3f} s"
        )
        settled = False
    else:
        detail = f"synchronized to {reference}, offset {offset:+.6f} s"
        settled = True
    return ClockSyncState(
        applicable=True,
        settled=settled,
        detail=detail,
        leap_status=leap_status,
        system_offset_seconds=offset,
        reference=reference,
    )


class ChronyClockMonitor:
    """Cached chrony settledness; not applicable off the aircraft."""

    def __init__(
        self,
        *,
        enabled: bool,
        max_offset_seconds: float = DEFAULT_MAX_OFFSET_SECONDS,
        cache_seconds: float = DEFAULT_CACHE_SECONDS,
        tracking: Callable[[], str] = run_chronyc_tracking,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.enabled = enabled
        self.max_offset_seconds = max_offset_seconds
        self.cache_seconds = cache_seconds
        self._tracking = tracking
        self._monotonic = monotonic
        self._cached: tuple[float, ClockSyncState] | None = None

    def state(self) -> ClockSyncState:
        if not self.enabled:
            return ClockSyncState(
                applicable=False, settled=True, detail="simulation host clock"
            )
        now = self._monotonic()
        if self._cached is not None and now - self._cached[0] < self.cache_seconds:
            return self._cached[1]
        try:
            state = parse_chronyc_tracking(
                self._tracking(), max_offset_seconds=self.max_offset_seconds
            )
        except (OSError, subprocess.SubprocessError) as exc:
            state = ClockSyncState(
                applicable=True,
                settled=False,
                detail=f"chrony tracking unavailable: {exc}",
            )
        self._cached = (now, state)
        return state
