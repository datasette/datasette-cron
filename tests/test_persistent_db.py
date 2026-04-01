"""Test with a persistent internal DB file, simulating `just dev` with internal.db."""
import asyncio
import os
import tempfile

import pytest
import pytest_asyncio
from datasette.app import Datasette
from datasette import hookimpl
from datasette.plugins import pm


class PersistentTestPlugin:
    __name__ = "persistent_test_plugin"
    _calls = []

    @staticmethod
    @hookimpl
    def cron_register_handlers(datasette):
        async def handler(datasette, config):
            PersistentTestPlugin._calls.append(1)

        return {"persistent-handler": handler}

    @staticmethod
    @hookimpl
    def startup(datasette):
        async def inner():
            scheduler = datasette._cron_scheduler
            await scheduler.add_task(
                name="persistent-task",
                handler="persistent-handler",
                schedule={"interval": 1},
                config={},
                overlap="skip",
            )
        return inner


@pytest_asyncio.fixture
async def ds_persistent():
    PersistentTestPlugin._calls = []
    pm.register(PersistentTestPlugin, name="persistent_test")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            internal_path = os.path.join(tmpdir, "internal.db")
            db_path = os.path.join(tmpdir, "tmp.db")

            # Create the db file so Datasette finds it
            import sqlite3
            sqlite3.connect(db_path).close()

            datasette = Datasette(
                [db_path],
                internal=internal_path,
                config={"permissions": {"datasette-cron-access": True}},
            )
            await datasette.invoke_startup()
            datasette._cron_scheduler.start()
            yield datasette
            await datasette._cron_scheduler.shutdown()
    finally:
        pm.unregister(PersistentTestPlugin, name="persistent_test")


@pytest.mark.asyncio
async def test_persistent_task_registered(ds_persistent):
    scheduler = ds_persistent._cron_scheduler
    task = await scheduler.internal_db.get_task("persistent-task")
    assert task is not None
    assert task["handler"] == "persistent-handler"
    assert task["enabled"] == 1
    assert task["next_run_at"] is not None


@pytest.mark.asyncio
async def test_persistent_task_executes(ds_persistent):
    """Task must execute within 3 seconds — the core contract."""
    await asyncio.sleep(3)
    assert len(PersistentTestPlugin._calls) >= 1, (
        f"Handler never called! calls={len(PersistentTestPlugin._calls)}"
    )


@pytest.mark.asyncio
async def test_persistent_task_runs_in_db(ds_persistent):
    await asyncio.sleep(3)
    scheduler = ds_persistent._cron_scheduler
    runs = await scheduler.internal_db.get_runs("persistent-task")
    assert len(runs) >= 1, f"No runs in DB after 3s! runs={runs}"


@pytest.mark.asyncio
async def test_persistent_task_multiple_runs(ds_persistent):
    """After 5s with 1s interval, should have multiple successful runs."""
    await asyncio.sleep(5)
    assert len(PersistentTestPlugin._calls) >= 3, (
        f"Expected >=3 calls in 5s, got {len(PersistentTestPlugin._calls)}"
    )
    scheduler = ds_persistent._cron_scheduler
    runs = await scheduler.internal_db.get_runs("persistent-task")
    assert len(runs) >= 3
    for run in runs:
        assert run["status"] == "success", f"Run failed: {run}"
