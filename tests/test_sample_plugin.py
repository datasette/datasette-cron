"""Test that simulates the sample debug plugin pattern."""

import asyncio

import pytest
import pytest_asyncio
from datasette.app import Datasette


@pytest_asyncio.fixture
async def ds():
    datasette = Datasette(
        memory=True,
        config={"permissions": {"datasette-cron-access": True}},
    )
    await datasette.invoke_startup()
    scheduler = datasette._cron_scheduler

    call_count = 0

    async def debug_handler(datasette, config):
        nonlocal call_count
        call_count += 1

    scheduler.register_handlers("debug", {"insert": debug_handler})

    await scheduler.add_task(
        name="debug-every-second",
        handler="debug:insert",
        schedule={"interval": 1},
        config={},
        overlap="skip",
    )

    scheduler.start()
    datasette._test_call_count = lambda: call_count
    yield datasette
    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_task_runs_automatically(ds):
    """Task with 1s interval should fire within a few seconds of startup."""
    # Give the scheduler loop time to tick
    for _ in range(30):
        await asyncio.sleep(0.2)
        if ds._test_call_count() > 0:
            break

    count = ds._test_call_count()
    print(f"Handler called {count} times")
    assert count >= 1, f"Expected handler to be called at least once, got {count}"
