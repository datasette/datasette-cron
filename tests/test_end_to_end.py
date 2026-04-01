"""End-to-end tests: register handler, add task, verify it actually executes on schedule."""
import asyncio

import pytest
import pytest_asyncio
from datasette.app import Datasette


@pytest_asyncio.fixture
async def ds_with_task():
    """Set up a Datasette with a handler and a 1-second interval task, then let the loop run."""
    datasette = Datasette(
        memory=True,
        config={"permissions": {"datasette-cron-access": True}},
    )
    await datasette.invoke_startup()
    scheduler = datasette._cron_scheduler

    calls = []

    async def counting_handler(datasette, config):
        calls.append(config)

    scheduler.register_handlers("test", {"counter": counting_handler})
    await scheduler.add_task(
        name="every-second",
        handler="test:counter",
        schedule={"interval": 1},
        config={"marker": "hello"},
        overlap="skip",
    )

    scheduler.start()
    datasette._test_calls = calls
    yield datasette
    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_task_actually_executes(ds_with_task):
    """A 1s interval task should have at least 1 run after 3 seconds."""
    await asyncio.sleep(3)
    calls = ds_with_task._test_calls
    assert len(calls) >= 1, f"Expected handler to be called, got {len(calls)} calls"
    assert calls[0] == {"marker": "hello"}


@pytest.mark.asyncio
async def test_task_executes_multiple_times(ds_with_task):
    """After 5 seconds a 1s interval task should have multiple runs."""
    await asyncio.sleep(5)
    calls = ds_with_task._test_calls
    assert len(calls) >= 3, f"Expected >=3 calls in 5s, got {len(calls)}"


@pytest.mark.asyncio
async def test_runs_recorded_in_db(ds_with_task):
    """Runs should be persisted in datasette_cron_runs."""
    await asyncio.sleep(3)
    scheduler = ds_with_task._cron_scheduler
    runs = await scheduler.internal_db.get_runs("every-second")
    assert len(runs) >= 1, f"Expected runs in DB, got {len(runs)}"
    assert runs[0]["status"] == "success"
    assert runs[0]["task_name"] == "every-second"


@pytest.mark.asyncio
async def test_task_status_updated(ds_with_task):
    """After execution, task's last_status should be 'success'."""
    await asyncio.sleep(3)
    scheduler = ds_with_task._cron_scheduler
    task = await scheduler.internal_db.get_task("every-second")
    assert task["last_status"] == "success"


@pytest.mark.asyncio
async def test_next_run_at_advances(ds_with_task):
    """next_run_at should advance after each execution."""
    scheduler = ds_with_task._cron_scheduler
    task_before = await scheduler.internal_db.get_task("every-second")
    next_before = task_before["next_run_at"]

    await asyncio.sleep(2)

    task_after = await scheduler.internal_db.get_task("every-second")
    next_after = task_after["next_run_at"]
    assert next_after != next_before, "next_run_at should have advanced"
