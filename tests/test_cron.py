import asyncio
import json

from datasette.app import Datasette
import pytest

from datasette_cron.models import CronTask
from datasette_cron.schedules import IntervalSchedule, CronSchedule, parse_schedule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_scheduler(**ds_kwargs):
    """Return (datasette, scheduler) with migrations applied."""
    ds = Datasette(
        memory=True,
        config={"permissions": {"datasette-cron-access": True}},
        **ds_kwargs,
    )
    await ds.invoke_startup()
    scheduler = ds._cron_scheduler
    return ds, scheduler


# ---------------------------------------------------------------------------
# 1. add_task() creates task in DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_task_creates_task_in_db():
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"noop": noop})

    await scheduler.add_task(
        name="my-task",
        handler="test:noop",
        schedule={"interval": 3600},
        config={"key": "value"},
    )

    task = await scheduler.internal_db.get_task("my-task")
    assert task is not None
    assert task.name == "my-task"
    assert task.handler == "test:noop"
    assert task.schedule_type == "interval"
    assert json.loads(task.config) == {"key": "value"}
    assert task.enabled == 1
    assert task.next_run_at is not None
    assert task.overlap_policy == "skip"
    assert task.retry_max == 0
    assert task.retry_backoff == "exponential"

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 2. add_task() upsert preserves next_run_at
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_task_upsert_preserves_next_run_at():
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"handler": noop})

    await scheduler.add_task(
        name="upsert-task",
        handler="test:handler",
        schedule={"interval": 60},
    )

    task1 = await scheduler.internal_db.get_task("upsert-task")
    original_next_run = task1.next_run_at

    # Call again with different schedule -- next_run_at should be preserved
    await scheduler.add_task(
        name="upsert-task",
        handler="test:handler",
        schedule={"interval": 120},
    )

    task2 = await scheduler.internal_db.get_task("upsert-task")
    assert task2.next_run_at == original_next_run
    # But schedule_config should reflect the new interval
    assert json.loads(task2.schedule_config)["seconds"] == 120

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 3. remove_task() removes from DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_task_removes_from_db():
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"rm": noop})

    await scheduler.add_task(
        name="remove-me",
        handler="test:rm",
        schedule={"interval": 60},
    )
    assert (await scheduler.internal_db.get_task("remove-me")) is not None

    await scheduler.remove_task("remove-me")
    assert (await scheduler.internal_db.get_task("remove-me")) is None

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 4. trigger_task() runs handler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_task_runs_handler():
    ds, scheduler = await _make_scheduler()

    triggered = asyncio.Event()

    async def handler(datasette, config):
        triggered.set()

    scheduler.register_handlers("test", {"trigger-handler": handler})

    await scheduler.add_task(
        name="trigger-test",
        handler="test:trigger-handler",
        schedule={"interval": 99999},
    )

    await scheduler.trigger_task("trigger-test")
    await asyncio.wait_for(triggered.wait(), timeout=5.0)
    assert triggered.is_set()

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_trigger_task_not_found():
    ds, scheduler = await _make_scheduler()

    with pytest.raises(ValueError, match="Task not found"):
        await scheduler.trigger_task("nonexistent")

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_trigger_task_handler_not_found():
    ds, scheduler = await _make_scheduler()

    # Insert a task whose handler is not registered
    await scheduler.internal_db.upsert_task(
        name="orphan-task",
        handler="missing:handler",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 60}),
        next_run_at="2099-01-01T00:00:00",
    )

    with pytest.raises(ValueError, match="Handler not found"):
        await scheduler.trigger_task("orphan-task")

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 5. enable_task() / disable_task()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enable_disable_task():
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"toggle": noop})

    await scheduler.add_task(
        name="toggle-task",
        handler="test:toggle",
        schedule={"interval": 60},
    )

    # Disable
    await scheduler.disable_task("toggle-task")
    task = await scheduler.internal_db.get_task("toggle-task")
    assert task.enabled == 0

    # Enable
    await scheduler.enable_task("toggle-task")
    task = await scheduler.internal_db.get_task("toggle-task")
    assert task.enabled == 1

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 6. register_handlers() with prefix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_handlers_with_prefix():
    ds, scheduler = await _make_scheduler()

    async def handler_a(datasette, config):
        pass

    async def handler_b(datasette, config):
        pass

    scheduler.register_handlers(
        "myplugin",
        {
            "do-stuff": handler_a,
            "do-other": handler_b,
        },
    )

    # Prefixed keys must exist
    assert scheduler.get_handler("myplugin:do-stuff") is handler_a
    assert scheduler.get_handler("myplugin:do-other") is handler_b

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 7. get_handler() by prefixed and bare name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_handler_prefixed_and_bare():
    ds, scheduler = await _make_scheduler()

    async def my_fn(datasette, config):
        pass

    scheduler.register_handlers("plug", {"action": my_fn})

    # Prefixed lookup
    assert scheduler.get_handler("plug:action") is my_fn
    # Bare lookup (convenience alias)
    assert scheduler.get_handler("action") is my_fn
    # Non-existent
    assert scheduler.get_handler("no-such-handler") is None

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_get_handler_bare_name_no_overwrite():
    """If two plugins register the same bare name, first one wins."""
    ds, scheduler = await _make_scheduler()

    async def fn_a(datasette, config):
        pass

    async def fn_b(datasette, config):
        pass

    scheduler.register_handlers("alpha", {"run": fn_a})
    scheduler.register_handlers("beta", {"run": fn_b})

    # Bare name keeps the first registration
    assert scheduler.get_handler("run") is fn_a
    # But prefixed names are always correct
    assert scheduler.get_handler("alpha:run") is fn_a
    assert scheduler.get_handler("beta:run") is fn_b

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 8. InternalDB.get_task() returns dict or None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_internal_db_get_task_returns_dict():
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    await idb.upsert_task(
        name="t1",
        handler="h",
        config={"a": 1},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2099-01-01T00:00:00",
    )

    result = await idb.get_task("t1")
    assert isinstance(result, CronTask)
    assert result.name == "t1"
    assert result.handler == "h"

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_internal_db_get_task_returns_none():
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    result = await idb.get_task("does-not-exist")
    assert result is None

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 9. InternalDB.get_due_tasks() filters correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_internal_db_get_due_tasks_filters():
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    # Task with next_run_at in the past (due)
    await idb.upsert_task(
        name="due-task",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2000-01-01T00:00:00",
    )

    # Task with next_run_at in the far future (not due)
    await idb.upsert_task(
        name="future-task",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2099-01-01T00:00:00",
    )

    # Disabled task with past next_run_at (should NOT appear)
    await idb.upsert_task(
        name="disabled-task",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2000-01-01T00:00:00",
    )
    await idb.update_task("disabled-task", enabled=0)

    # Task with no next_run_at (should NOT appear)
    await idb.upsert_task(
        name="no-next-run",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at=None,
    )

    due = await idb.get_due_tasks()
    due_names = [t.name for t in due]

    assert "due-task" in due_names
    assert "future-task" not in due_names
    assert "disabled-task" not in due_names
    assert "no-next-run" not in due_names

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 10. InternalDB.update_task() allowed/disallowed fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_internal_db_update_task_allowed_fields():
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    await idb.upsert_task(
        name="upd",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2099-01-01T00:00:00",
    )

    # Update with allowed fields
    await idb.update_task(
        "upd", enabled=0, handler="new-handler", last_status="success"
    )
    task = await idb.get_task("upd")
    assert task.enabled == 0
    assert task.handler == "new-handler"
    assert task.last_status == "success"

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_internal_db_update_task_disallowed_field():
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    await idb.upsert_task(
        name="upd2",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2099-01-01T00:00:00",
    )

    with pytest.raises(ValueError, match="Cannot update field"):
        await idb.update_task("upd2", created_at="2000-01-01T00:00:00")

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_internal_db_update_task_config_serialized():
    """config values should be JSON-serialised when updating."""
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    await idb.upsert_task(
        name="cfg-task",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2099-01-01T00:00:00",
    )

    await idb.update_task("cfg-task", config={"x": 42})
    task = await idb.get_task("cfg-task")
    assert json.loads(task.config) == {"x": 42}

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# 11. IntervalSchedule basics
# ---------------------------------------------------------------------------


def test_interval_schedule_next_run():
    from datetime import datetime

    sched = IntervalSchedule(seconds=300)
    now = datetime(2025, 1, 1, 12, 0, 0)
    nxt = sched.next_run(now)
    assert nxt == datetime(2025, 1, 1, 12, 5, 0)


def test_interval_schedule_negative_raises():
    with pytest.raises(ValueError, match="positive"):
        IntervalSchedule(seconds=-1)


def test_interval_schedule_describe():
    assert IntervalSchedule(seconds=30).describe() == "every 30s"
    assert IntervalSchedule(seconds=120).describe() == "every 2m"
    assert IntervalSchedule(seconds=3600).describe() == "every 1h"
    assert IntervalSchedule(seconds=90000).describe() == "every 1d 1h"


def test_interval_schedule_to_dict():
    sched = IntervalSchedule(seconds=60)
    assert sched.to_dict() == {"seconds": 60}
    assert sched.schedule_type == "interval"


def test_parse_schedule_interval():
    sched = parse_schedule({"interval": 600})
    assert isinstance(sched, IntervalSchedule)
    assert sched.seconds == 600


def test_parse_schedule_cron():
    sched = parse_schedule("*/5 * * * *")
    assert isinstance(sched, CronSchedule)
    assert sched.expression == "*/5 * * * *"


def test_parse_schedule_invalid():
    with pytest.raises(ValueError):
        parse_schedule(12345)


# ---------------------------------------------------------------------------
# 12. Handler execution records run in datasette_cron_runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handler_execution_records_success_run():
    ds, scheduler = await _make_scheduler()

    triggered = asyncio.Event()

    async def handler(datasette, config):
        triggered.set()

    scheduler.register_handlers("test", {"run-handler": handler})

    await scheduler.add_task(
        name="run-record-task",
        handler="test:run-handler",
        schedule={"interval": 99999},
    )

    await scheduler.trigger_task("run-record-task")
    await asyncio.wait_for(triggered.wait(), timeout=5.0)

    # Give a moment for the DB writes to flush
    await asyncio.sleep(0.2)

    runs = await scheduler.internal_db.get_runs("run-record-task")
    assert len(runs) >= 1

    latest = runs[0]
    assert latest.task_name == "run-record-task"
    assert latest.status == "success"
    assert latest.started_at is not None
    assert latest.finished_at is not None
    assert latest.duration_ms is not None
    assert latest.duration_ms >= 0

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_handler_execution_records_error_run():
    ds, scheduler = await _make_scheduler()

    async def failing_handler(datasette, config):
        raise RuntimeError("boom")

    scheduler.register_handlers("test", {"fail-handler": failing_handler})

    await scheduler.add_task(
        name="fail-record-task",
        handler="test:fail-handler",
        schedule={"interval": 99999},
    )

    await scheduler.trigger_task("fail-record-task")
    # Wait for execution to complete (no event to wait on, so brief sleep)
    await asyncio.sleep(0.5)

    runs = await scheduler.internal_db.get_runs("fail-record-task")
    assert len(runs) >= 1

    latest = runs[0]
    assert latest.task_name == "fail-record-task"
    assert latest.status == "error"
    assert "boom" in latest.error_message

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_sync_handler_execution():
    """Sync handlers should also work."""
    ds, scheduler = await _make_scheduler()

    results = []

    def sync_handler(datasette, config):
        results.append(config)

    scheduler.register_handlers("test", {"sync-h": sync_handler})

    await scheduler.add_task(
        name="sync-task",
        handler="test:sync-h",
        schedule={"interval": 99999},
        config={"val": 123},
    )

    await scheduler.trigger_task("sync-task")
    await asyncio.sleep(0.5)

    assert len(results) == 1
    assert results[0] == {"val": 123}

    runs = await scheduler.internal_db.get_runs("sync-task")
    assert len(runs) >= 1
    assert runs[0].status == "success"

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# Additional: plugin installed, scheduler starts
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-cron" in installed_plugins


@pytest.mark.asyncio
async def test_scheduler_starts_on_startup():
    ds, scheduler = await _make_scheduler()
    scheduler.start()
    assert scheduler._loop_task is not None
    assert not scheduler._loop_task.done()
    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# InternalDB.record_run_start / record_run_success / record_run_error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_run_lifecycle():
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    await idb.upsert_task(
        name="lifecycle-task",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 10}),
        next_run_at="2099-01-01T00:00:00",
    )

    run_id = await idb.record_run_start("lifecycle-task", attempt=1)
    assert isinstance(run_id, int)

    # Check it exists as running
    runs = await idb.get_runs("lifecycle-task")
    assert len(runs) == 1
    assert runs[0].status == "running"

    # Mark success
    await idb.record_run_success(run_id, duration_ms=42)
    runs = await idb.get_runs("lifecycle-task")
    assert runs[0].status == "success"
    assert runs[0].duration_ms == 42
    assert runs[0].finished_at is not None

    # Start another and mark error
    run_id2 = await idb.record_run_start("lifecycle-task", attempt=2)
    await idb.record_run_error(run_id2, "something broke", duration_ms=10)
    runs = await idb.get_runs("lifecycle-task")
    assert len(runs) == 2
    # Find the error run by id
    error_run = [r for r in runs if r.id == run_id2][0]
    assert error_run.status == "error"
    assert error_run.error_message == "something broke"

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# InternalDB.get_all_tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_tasks():
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    for letter in ("b", "a", "c"):
        await idb.upsert_task(
            name=f"task-{letter}",
            handler="h",
            config={},
            schedule_type="interval",
            schedule_config=json.dumps({"seconds": 10}),
            next_run_at="2099-01-01T00:00:00",
        )

    tasks = await idb.get_all_tasks()
    names = [t.name for t in tasks]
    assert names == ["task-a", "task-b", "task-c"]  # ordered by name

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# Scheduler.update_task via public API
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_update_task():
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"h": noop})

    await scheduler.add_task(
        name="upd-task",
        handler="test:h",
        schedule={"interval": 60},
        config={"old": True},
    )

    await scheduler.update_task("upd-task", config={"new": True}, overlap="cancel")
    task = await scheduler.internal_db.get_task("upd-task")
    assert json.loads(task.config) == {"new": True}
    assert task.overlap_policy == "cancel"

    await scheduler.shutdown()


# ---------------------------------------------------------------------------
# Regression tests for scheduling bugs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_second_interval_schedule():
    """Schedule with interval=1 must produce schedule_config with seconds=1."""
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"tick": noop})

    await scheduler.add_task(
        name="one-sec",
        handler="test:tick",
        schedule={"interval": 1},
    )

    task = await scheduler.internal_db.get_task("one-sec")
    schedule_config = json.loads(task.schedule_config)
    assert schedule_config == {"seconds": 1}
    assert task.schedule_type == "interval"

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_upsert_updates_schedule_config():
    """Calling add_task twice with different schedule must update schedule_config in DB."""
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"resched": noop})

    await scheduler.add_task(
        name="resched-task",
        handler="test:resched",
        schedule={"interval": 300},
    )

    task1 = await scheduler.internal_db.get_task("resched-task")
    assert json.loads(task1.schedule_config)["seconds"] == 300

    # Re-add with a different interval
    await scheduler.add_task(
        name="resched-task",
        handler="test:resched",
        schedule={"interval": 1},
    )

    task2 = await scheduler.internal_db.get_task("resched-task")
    assert json.loads(task2.schedule_config)["seconds"] == 1

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_task_with_past_next_run_at_is_due():
    """A task whose next_run_at is in the past must be returned by get_due_tasks."""
    ds, scheduler = await _make_scheduler()
    idb = scheduler.internal_db

    await idb.upsert_task(
        name="past-task",
        handler="h",
        config={},
        schedule_type="interval",
        schedule_config=json.dumps({"seconds": 60}),
        next_run_at="2000-01-01T00:00:00",
    )

    due = await idb.get_due_tasks()
    due_names = [t.name for t in due]
    assert "past-task" in due_names

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_handler_called_with_correct_config():
    """When a task fires, the handler must receive the exact config dict from add_task."""
    ds, scheduler = await _make_scheduler()

    received = asyncio.Event()
    received_config = {}

    async def capture_handler(datasette, config):
        received_config.update(config)
        received.set()

    scheduler.register_handlers("test", {"capture": capture_handler})

    expected_config = {"db": "mydb", "table": "events", "nested": {"a": [1, 2, 3]}}

    await scheduler.add_task(
        name="config-check",
        handler="test:capture",
        schedule={"interval": 99999},
        config=expected_config,
    )

    await scheduler.trigger_task("config-check")
    await asyncio.wait_for(received.wait(), timeout=5.0)

    assert received_config == expected_config

    await scheduler.shutdown()
