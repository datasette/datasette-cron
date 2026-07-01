from datasette.app import Datasette
import json
import pytest
import re


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
async def test_api_trigger_takes_no_body():
    """The trigger endpoint takes no request body. Browser callers (with
    cookies) must still send a JSON content-type header so Datasette's CSRF
    protection skips the request -- pin that contract here."""
    datasette = await _setup_allowed_datasette_with_task()

    # Simulated browser call: cookie + JSON content-type, empty body.
    response = await datasette.client.post(
        "/-/api/cron/tasks/test-task/trigger",
        headers={"Content-Type": "application/json", "Cookie": "foo=bar"},
    )
    assert response.status_code == 200
    assert response.json()["ok"] is True

    # Unknown task still 404s.
    missing = await datasette.client.post(
        "/-/api/cron/tasks/nope/trigger",
        headers={"Content-Type": "application/json"},
    )
    assert missing.status_code == 404
    assert missing.json()["ok"] is False

    await datasette._cron_scheduler.shutdown()


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
    # Let the triggered execution record its run row. Reads here race the
    # execution's own writes on the shared in-memory internal DB, which can
    # transiently raise "database table is locked" -- retry until settled.
    import asyncio
    import sqlite3

    for _ in range(50):
        try:
            runs = await scheduler.internal_db.get_runs("shaped")
        except sqlite3.OperationalError:
            runs = []
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


@pytest.mark.asyncio
async def test_schedule_description_all_types_and_fallback():
    """The API and page layers derive schedule_description/schedule_seconds
    through the shared describe_schedule helper -- cover all three schedule
    types plus the malformed-config fallback in both layers."""
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
        name="interval-task", handler="test:h", schedule={"interval": 300}
    )
    await scheduler.add_task(name="cron-task", handler="test:h", schedule="0 8 * * *")
    await scheduler.add_task(
        name="rrule-task", handler="test:h", schedule={"rrule": "FREQ=DAILY"}
    )
    # A row with unparseable schedule_config, as if written by a buggy or
    # future version -- exercises the except-fallback branch.
    await scheduler.internal_db.upsert_task(
        name="broken-task",
        handler="test:h",
        config={},
        schedule_type="interval",
        schedule_config="not json",
    )

    expected = {
        "interval-task": ("every 5m", 300),
        "cron-task": ("cron: 0 8 * * *", None),
        "rrule-task": ("rrule: FREQ=DAILY", None),
        "broken-task": ("interval: not json", None),
    }

    # API layer
    api_resp = await datasette.client.get("/-/api/cron/tasks")
    assert api_resp.status_code == 200
    api_tasks = {t["name"]: t for t in api_resp.json()["tasks"]}
    for name, (description, seconds) in expected.items():
        assert api_tasks[name]["schedule_description"] == description, name
        assert api_tasks[name]["schedule_seconds"] == seconds, name

    # Page layer (page_data JSON embedded in the index page HTML)
    page_resp = await datasette.client.get("/-/cron")
    assert page_resp.status_code == 200
    for description, _ in expected.values():
        assert description in page_resp.text

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

    # Note: the detail page (/-/cron/<task>) is not listed — it renders
    # purely from the internal DB and no longer touches the scheduler.
    for method, path in [
        ("GET", "/-/cron"),
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

    # The detail page renders from the internal DB alone and still works.
    detail = await datasette.client.get("/-/cron/test-task")
    assert detail.status_code == 200


def _embedded_page_data(html: str) -> dict:
    match = re.search(
        r'<script type="application/json" id="pageData">(.*?)</script>',
        html,
        re.DOTALL,
    )
    assert match is not None
    return json.loads(match.group(1))


@pytest.mark.asyncio
async def test_page_data_handlers_only_on_index():
    """The index page embeds the registered-handlers list in its page data
    (rendered as a debugging aid for handler-ref typos); the detail page no
    longer ships the unused field."""
    datasette = await _setup_allowed_datasette_with_task()

    index = await datasette.client.get("/-/cron")
    assert index.status_code == 200
    # Membership, not equality: sample plugins loaded by other test modules
    # register handlers in the process-global plugin registry.
    assert "test:my-handler" in _embedded_page_data(index.text)["handlers"]

    detail = await datasette.client.get("/-/cron/test-task")
    assert detail.status_code == 200
    assert "handlers" not in _embedded_page_data(detail.text)

    await datasette._cron_scheduler.shutdown()


@pytest.mark.asyncio
async def test_api_enable_route_exists():
    datasette = await _setup_datasette_with_task()
    response = await datasette.client.post(
        "/-/api/cron/tasks/test-task/enable",
        json={"enabled": False},
    )
    assert response.status_code in (200, 403)
    await datasette._cron_scheduler.shutdown()
