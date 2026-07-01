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
    await scheduler.add_task(name="daily", handler="test:h", schedule="0 8 * * *")

    interval_resp = await datasette.client.get("/-/api/cron/tasks/every-5s")
    assert interval_resp.status_code == 200
    assert interval_resp.json()["schedule_seconds"] == 5

    cron_resp = await datasette.client.get("/-/api/cron/tasks/daily")
    assert cron_resp.status_code == 200
    assert cron_resp.json()["schedule_seconds"] is None

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_api_response_field_sets():
    """Pin the exact key sets of the task and run API responses -- the
    frontend's generated types depend on these shapes."""
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
        name="shaped",
        handler="test:h",
        schedule={"interval": 3600},
        config={"key": "value"},
    )
    await scheduler.trigger_task("shaped")
    # Let the triggered execution record its run row.
    import asyncio

    for _ in range(50):
        runs = await scheduler.internal_db.get_runs("shaped")
        if runs and runs[0].status != "running":
            break
        await asyncio.sleep(0.05)

    task_resp = await datasette.client.get("/-/api/cron/tasks/shaped")
    assert task_resp.status_code == 200
    task = task_resp.json()
    assert set(task.keys()) == {
        "name",
        "handler",
        "config",
        "schedule_type",
        "schedule_config",
        "schedule_description",
        "schedule_seconds",
        "timezone",
        "overlap_policy",
        "retry_max",
        "retry_backoff",
        "enabled",
        "next_run_at",
        "last_run_at",
        "last_status",
    }
    assert task["name"] == "shaped"
    assert task["config"] == {"key": "value"}

    list_resp = await datasette.client.get("/-/api/cron/tasks")
    assert list_resp.status_code == 200
    assert set(list_resp.json()["tasks"][0].keys()) == set(task.keys())

    runs_resp = await datasette.client.get("/-/api/cron/tasks/shaped/runs")
    assert runs_resp.status_code == 200
    runs_json = runs_resp.json()["runs"]
    assert len(runs_json) >= 1
    assert set(runs_json[0].keys()) == {
        "id",
        "task_name",
        "started_at",
        "finished_at",
        "status",
        "error_message",
        "attempt",
        "duration_ms",
    }
    assert runs_json[0]["task_name"] == "shaped"

    await scheduler.shutdown()


async def _setup_allowed_datasette_with_task():
    datasette = Datasette(
        memory=True,
        config={"permissions": {"datasette-cron-access": True}},
    )
    await datasette.invoke_startup()
    scheduler = datasette._cron_scheduler

    async def handler(datasette, config):
        pass

    scheduler.register_handlers("test", {"my-handler": handler})
    await scheduler.add_task(
        name="test-task",
        handler="test:my-handler",
        schedule={"interval": 3600},
    )
    return datasette


@pytest.mark.asyncio
async def test_page_titles():
    """Pages set a browser tab title via the template's title block."""
    datasette = await _setup_allowed_datasette_with_task()

    index = await datasette.client.get("/-/cron")
    assert index.status_code == 200
    assert "<title>Cron Tasks" in index.text

    detail = await datasette.client.get("/-/cron/test-task")
    assert detail.status_code == 200
    assert "<title>Task: test-task" in detail.text

    await datasette._cron_scheduler.shutdown()


@pytest.mark.asyncio
async def test_missing_scheduler_yields_clear_error():
    """If startup failed (no datasette._cron_scheduler), routes that need the
    scheduler return a clear, actionable error instead of a bare
    AttributeError traceback."""
    datasette = await _setup_allowed_datasette_with_task()
    scheduler = datasette._cron_scheduler
    await scheduler.shutdown()
    del datasette._cron_scheduler

    for method, path in [
        ("GET", "/-/cron"),
        ("GET", "/-/cron/test-task"),
        ("POST", "/-/api/cron/tasks/test-task/trigger"),
        ("POST", "/-/api/cron/tasks/test-task/enable"),
    ]:
        if method == "GET":
            response = await datasette.client.get(path)
        else:
            body = {"enabled": True} if path.endswith("/enable") else {}
            response = await datasette.client.post(path, json=body)
        assert response.status_code == 500, path
        assert "datasette-cron startup did not complete" in response.text, path
        assert "AttributeError" not in response.text, path
        assert "no attribute" not in response.text, path


@pytest.mark.asyncio
async def test_api_enable_route_exists():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.post(
        "/-/api/cron/tasks/test-task/enable",
        json={"enabled": False},
    )
    assert response.status_code in (200, 403)
    await datasette._cron_scheduler.shutdown()
