from datasette.app import Datasette
import pytest


async def _setup_datasette_with_task():
    datasette = Datasette(memory=True)
    await datasette.invoke_startup()
    scheduler = datasette._cron_scheduler

    async def handler(datasette, config):
        pass

    scheduler.register_handlers("test", {"my-handler": handler})
    await scheduler.add_task(
        name="test-task",
        handler="test:my-handler",
        schedule={"interval": 3600},
        config={"key": "value"},
    )
    return datasette


@pytest.mark.asyncio
async def test_api_tasks_route_exists():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.get("/-/api/cron/tasks")
    # Route exists (200 or 403 depending on default permissions)
    assert response.status_code in (200, 403)
    await datasette._cron_scheduler.shutdown()


@pytest.mark.asyncio
async def test_cron_page_route_exists():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.get("/-/cron")
    assert response.status_code in (200, 403)
    await datasette._cron_scheduler.shutdown()


@pytest.mark.asyncio
async def test_cron_detail_route_exists():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.get("/-/cron/test-task")
    assert response.status_code in (200, 403)
    await datasette._cron_scheduler.shutdown()


@pytest.mark.asyncio
async def test_cron_detail_not_found():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.get("/-/cron/nonexistent")
    assert response.status_code in (403, 404)
    await datasette._cron_scheduler.shutdown()


@pytest.mark.asyncio
async def test_api_trigger_route_exists():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.post(
        "/-/api/cron/tasks/test-task/trigger",
        json={},
    )
    assert response.status_code in (200, 403)
    await datasette._cron_scheduler.shutdown()


@pytest.mark.asyncio
async def test_api_task_response_includes_schedule_seconds():
    """API responses expose schedule_seconds for interval tasks (used by the
    frontend to classify "continuous"), and None for non-interval schedules.
    """
    datasette = Datasette(
        memory=True,
        config={"permissions": {"datasette-cron-access": True}},
    )
    await datasette.invoke_startup()
    scheduler = datasette._cron_scheduler

    async def handler(datasette, config):
        pass

    scheduler.register_handlers("test", {"h": handler})
    await scheduler.add_task(
        name="every-5s", handler="test:h", schedule={"interval": 5}
    )
    await scheduler.add_task(
        name="daily", handler="test:h", schedule="0 8 * * *"
    )

    interval_resp = await datasette.client.get("/-/api/cron/tasks/every-5s")
    assert interval_resp.status_code == 200
    assert interval_resp.json()["schedule_seconds"] == 5

    cron_resp = await datasette.client.get("/-/api/cron/tasks/daily")
    assert cron_resp.status_code == 200
    assert cron_resp.json()["schedule_seconds"] is None

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_api_enable_route_exists():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.post(
        "/-/api/cron/tasks/test-task/enable",
        json={"enabled": False},
    )
    assert response.status_code in (200, 403)
    await datasette._cron_scheduler.shutdown()
