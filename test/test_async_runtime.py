import asyncio
import time

from iii_drone_runtime.async_runtime import run_blocking_refresh_periodically


def test_blocking_periodic_refresh_does_not_stall_event_loop():
    async def exercise():
        refresh_started = asyncio.Event()
        loop = asyncio.get_running_loop()

        def blocking_refresh():
            loop.call_soon_threadsafe(refresh_started.set)
            time.sleep(0.1)

        task = asyncio.create_task(
            run_blocking_refresh_periodically(blocking_refresh, interval_seconds=0.001)
        )
        try:
            await asyncio.wait_for(refresh_started.wait(), timeout=0.2)
            started = time.monotonic()
            await asyncio.sleep(0.01)
            assert time.monotonic() - started < 0.05
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(exercise())
