"""Debug tests to isolate the router parameter passing issue."""
import pytest
import pytest_asyncio
from datasette.app import Datasette


@pytest_asyncio.fixture
async def ds():
    datasette = Datasette(memory=True, config={"permissions": {"datasette-cron-access": True}})
    await datasette.invoke_startup()
    scheduler = datasette._cron_scheduler

    async def handler(datasette, config):
        pass

    scheduler.register_handlers("test", {"h": handler})
    await scheduler.add_task(name="t", handler="test:h", schedule={"interval": 60})
    yield datasette
    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_get_tasks(ds):
    resp = await ds.client.get("/-/api/cron/tasks")
    assert resp.status_code == 200
    data = resp.json()
    assert "tasks" in data
    assert len(data["tasks"]) == 1


@pytest.mark.asyncio
async def test_get_runs(ds):
    resp = await ds.client.get("/-/api/cron/tasks/t/runs")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_post_trigger(ds):
    resp = await ds.client.post(
        "/-/api/cron/tasks/t/trigger",
        content=b"{}",
        headers={"content-type": "application/json"},
    )
    print(f"status={resp.status_code} body={resp.text[:300]}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_post_enable(ds):
    resp = await ds.client.post(
        "/-/api/cron/tasks/t/enable",
        content=b'{"enabled": false}',
        headers={"content-type": "application/json"},
    )
    print(f"status={resp.status_code} body={resp.text[:300]}")
    assert resp.status_code == 200
