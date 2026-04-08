"""Test loading the sample plugin exactly as --plugins-dir does it."""

import asyncio
import os
import tempfile

import pytest
import pytest_asyncio
from datasette.app import Datasette


SAMPLE_PLUGIN_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "samples", "datasette_cron_debug.py"
)


@pytest_asyncio.fixture
async def ds_plugins_dir():
    """Start Datasette with --plugins-dir pointing at samples/."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "tmp.db")
        internal_path = os.path.join(tmpdir, "internal.db")

        import sqlite3

        sqlite3.connect(db_path).close()

        samples_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "samples"
        )
        datasette = Datasette(
            [db_path],
            internal=internal_path,
            plugins_dir=samples_dir,
            config={"permissions": {"datasette-cron-access": True}},
        )
        await datasette.invoke_startup()
        if hasattr(datasette, "_cron_scheduler"):
            datasette._cron_scheduler.start()
        yield datasette
        scheduler = getattr(datasette, "_cron_scheduler", None)
        if scheduler:
            await scheduler.shutdown()


@pytest.mark.asyncio
async def test_sample_plugin_loaded(ds_plugins_dir):
    """The sample plugin should be loaded."""
    response = await ds_plugins_dir.client.get("/-/plugins.json")
    plugin_names = [p["name"] for p in response.json()]
    print(f"Plugins: {plugin_names}")
    # The plugins-dir module name is the filename
    assert any("cron_debug" in name for name in plugin_names)


@pytest.mark.asyncio
async def test_handler_registered_from_plugins_dir(ds_plugins_dir):
    scheduler = ds_plugins_dir._cron_scheduler
    handlers = list(scheduler._handler_registry.keys())
    print(f"Handlers: {handlers}")
    assert "debug-insert" in handlers


@pytest.mark.asyncio
async def test_task_created_from_plugins_dir(ds_plugins_dir):
    scheduler = ds_plugins_dir._cron_scheduler
    task = await scheduler.internal_db.get_task("debug-insert-every-second")
    print(f"Task: {task}")
    assert task is not None
    assert task.handler == "debug-insert"
    assert task.enabled == 1


@pytest.mark.asyncio
async def test_task_executes_from_plugins_dir(ds_plugins_dir):
    """The debug task should execute and produce runs within 3 seconds."""
    await asyncio.sleep(3)
    scheduler = ds_plugins_dir._cron_scheduler
    runs = await scheduler.internal_db.get_runs("debug-insert-every-second")
    print(f"Runs after 3s: {len(runs)}")
    for r in runs:
        print(f"  status={r.status} error={r.error_message}")
    assert len(runs) >= 1, "Expected runs, got none"


@pytest.mark.asyncio
async def test_cron_debug_table_populated(ds_plugins_dir):
    """The cron_debug table should have rows after the handler executes."""
    await asyncio.sleep(3)

    # Find the database that has cron_debug
    for db_name, db in ds_plugins_dir.databases.items():
        if db_name == "_internal":
            continue
        try:
            result = await db.execute("SELECT count(*) FROM cron_debug")
            count = result.single_value()
            print(f"cron_debug rows in {db_name}: {count}")
            if count > 0:
                return  # Success
        except Exception as e:
            print(f"  {db_name}: {e}")

    pytest.fail("cron_debug table has no rows in any database")
