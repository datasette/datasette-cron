"""Test the full hook-based flow: handler registered via cron_register_handlers,
task created in a separate startup hook (exactly like the sample plugin)."""
import asyncio

import pytest
import pytest_asyncio
from datasette.app import Datasette
from datasette import hookimpl
from datasette.plugins import pm


class SamplePlugin:
    """Simulates the sample debug plugin loaded via --plugins-dir."""

    __name__ = "datasette_cron_test_sample"
    _calls = []

    @staticmethod
    @hookimpl
    def cron_register_handlers(datasette):
        async def test_handler(datasette, config):
            SamplePlugin._calls.append(config)

        return {
            "test-insert": test_handler,
        }

    @staticmethod
    @hookimpl
    def startup(datasette):
        async def inner():
            scheduler = datasette._cron_scheduler
            await scheduler.add_task(
                name="test-every-second",
                handler="test-insert",
                schedule={"interval": 1},
                config={"from": "hook"},
                overlap="skip",
            )
        return inner


@pytest_asyncio.fixture
async def ds_with_plugin():
    SamplePlugin._calls = []
    pm.register(SamplePlugin, name="test_sample_plugin")
    try:
        datasette = Datasette(
            memory=True,
            config={"permissions": {"datasette-cron-access": True}},
        )
        await datasette.invoke_startup()
        datasette._cron_scheduler.start()
        yield datasette
        await datasette._cron_scheduler.shutdown()
    finally:
        pm.unregister(SamplePlugin, name="test_sample_plugin")


@pytest.mark.asyncio
async def test_handler_registered_via_hook(ds_with_plugin):
    scheduler = ds_with_plugin._cron_scheduler
    # Handler should be in registry (bare name since we register without prefix too)
    assert "test-insert" in scheduler._handler_registry


@pytest.mark.asyncio
async def test_task_created_via_startup_hook(ds_with_plugin):
    scheduler = ds_with_plugin._cron_scheduler
    task = await scheduler.internal_db.get_task("test-every-second")
    assert task is not None
    assert task["handler"] == "test-insert"
    assert task["enabled"] == 1


@pytest.mark.asyncio
async def test_hook_task_executes_automatically(ds_with_plugin):
    """The task created via hooks should execute without any manual intervention."""
    await asyncio.sleep(3)
    assert len(SamplePlugin._calls) >= 1, (
        f"Expected handler called >=1 times, got {len(SamplePlugin._calls)}"
    )
    assert SamplePlugin._calls[0] == {"from": "hook"}


@pytest.mark.asyncio
async def test_hook_task_runs_recorded(ds_with_plugin):
    """Runs should appear in the DB."""
    await asyncio.sleep(3)
    scheduler = ds_with_plugin._cron_scheduler
    runs = await scheduler.internal_db.get_runs("test-every-second")
    assert len(runs) >= 1, f"Expected runs in DB, got {len(runs)}"
    for run in runs:
        assert run["status"] == "success"
