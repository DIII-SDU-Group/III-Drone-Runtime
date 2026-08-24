"""Async scheduling helpers for the runtime API process."""

from __future__ import annotations

import asyncio
from collections.abc import Callable


async def run_blocking_refresh_periodically(
    refresh: Callable[[], None],
    *,
    interval_seconds: float,
) -> None:
    """Run a blocking state refresh without stalling the API event loop."""

    while True:
        await asyncio.sleep(interval_seconds)
        await asyncio.to_thread(refresh)
