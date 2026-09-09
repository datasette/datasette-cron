"""
Tests for datasette-cron's OpenTelemetry instrumentation, using the
in-memory exporter fixtures imported from core's telemetry testing kit in
conftest.py.
"""

import asyncio

import pytest
from datasette import hookimpl
from datasette.app import Datasette
from datasette.plugins import pm
from datasette.telemetry import SCHEMA_URL, linked_root_span_kwargs
from opentelemetry.trace import StatusCode

from datasette_cron.scheduler import _lag_seconds, _utcnow
from datasette_cron.telemetry import tracer

PAST = "2000-01-01T00:00:00"


async def _make_scheduler(**ds_kwargs):
    ds = Datasette(
        memory=True,
        config={"permissions": {"datasette-cron-access": True}},
        **ds_kwargs,
    )
    await ds.invoke_startup()
    return ds, ds._cron_scheduler


async def _drain(scheduler):
    "Await every in-flight execution so its spans have finished."
    tasks = [t for ts in scheduler._running_tasks.values() for t in ts]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _spans_named(otel_spans, name):
    return [s for s in otel_spans.get_finished_spans() if s.name == name]


def _one_span(otel_spans, name):
    spans = _spans_named(otel_spans, name)
    assert len(spans) == 1, f"expected one {name!r} span, got {len(spans)}"
    return spans[0]


def test_tracer_emits_under_own_scope(otel_spans):
    with tracer.start_as_current_span("probe"):
        pass
    spans = otel_spans.get_finished_spans()
    assert len(spans) == 1
    (span,) = spans
    assert span.name == "probe"
    assert span.instrumentation_scope.name == "datasette_cron"
    assert span.instrumentation_scope.schema_url == SCHEMA_URL


def test_linked_root_span_kwargs_links_current_span(otel_spans):
    with tracer.start_as_current_span("cause") as cause:
        kwargs = linked_root_span_kwargs()
    assert len(kwargs["links"]) == 1
    (link,) = kwargs["links"]
    assert link.context.span_id == cause.get_span_context().span_id


def test_linked_root_span_kwargs_without_current_span(otel_spans):
    kwargs = linked_root_span_kwargs()
    assert kwargs["links"] == []


# --- datasette_cron.run / attempt / backoff -------------------------------


@pytest.mark.asyncio
async def test_scheduled_run_span(otel_spans):
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"noop": noop})
    await scheduler.add_task(
        name="t", handler="test:noop", schedule={"interval": 99999}
    )
    await scheduler.internal_db.update_next_run("t", PAST)
    otel_spans.clear()

    await scheduler._tick()
    await _drain(scheduler)

    run = _one_span(otel_spans, "datasette_cron.run")
    assert run.parent is None
    assert run.attributes["datasette_cron.task"] == "t"
    assert run.attributes["datasette_cron.handler"] == "test:noop"
    assert run.attributes["datasette.plugin"] == "test"
    assert run.attributes["code.function"].endswith("noop")
    assert run.attributes["datasette_cron.trigger"] == "scheduled"
    assert run.attributes["datasette_cron.max_attempts"] == 1
    assert run.attributes["datasette_cron.scheduled_at"] == PAST
    assert run.attributes["datasette_cron.lag"] >= 0
    assert run.attributes["datasette_cron.attempts"] == 1
    assert run.attributes["datasette_cron.status"] == "success"
    assert "error.type" not in run.attributes
    assert run.status.status_code is StatusCode.UNSET

    attempt = _one_span(otel_spans, "datasette_cron.attempt")
    assert attempt.parent.span_id == run.context.span_id
    assert attempt.attributes["datasette_cron.attempt"] == 1
    assert attempt.attributes["datasette_cron.handler.async"] is True
    rows = (
        await ds.get_internal_database().execute("SELECT id FROM datasette_cron_runs")
    ).rows
    assert attempt.attributes["datasette_cron.run_id"] == rows[0]["id"]

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_manual_trigger_via_http_links_request_span(otel_spans):
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"noop": noop})
    await scheduler.add_task(
        name="t", handler="test:noop", schedule={"interval": 99999}
    )
    otel_spans.clear()

    response = await ds.client.post("/-/api/cron/tasks/t/trigger", json={})
    assert response.status_code == 200
    await _drain(scheduler)

    run = _one_span(otel_spans, "datasette_cron.run")
    assert run.parent is None
    assert run.attributes["datasette_cron.trigger"] == "manual"
    assert "datasette_cron.scheduled_at" not in run.attributes
    assert len(run.links) == 1
    request_spans = [
        s
        for s in otel_spans.get_finished_spans()
        if s.context.span_id == run.links[0].context.span_id
    ]
    assert len(request_spans) == 1
    assert request_spans[0].name.startswith("POST")

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_retry_spans(otel_spans, monkeypatch):
    ds, scheduler = await _make_scheduler()
    monkeypatch.setattr(
        type(scheduler), "_backoff_delay", staticmethod(lambda strategy, attempt: 0.01)
    )
    calls = 0

    async def flaky(datasette, config):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first attempt fails")

    scheduler.register_handlers("test", {"flaky": flaky})
    await scheduler.add_task(
        name="t",
        handler="test:flaky",
        schedule={"interval": 99999},
        retry={"max_retries": 1},
    )
    otel_spans.clear()

    await scheduler.trigger_task("t")
    await _drain(scheduler)

    run = _one_span(otel_spans, "datasette_cron.run")
    assert run.attributes["datasette_cron.status"] == "success"
    assert run.attributes["datasette_cron.attempts"] == 2
    assert run.attributes["datasette_cron.max_attempts"] == 2
    assert "error.type" not in run.attributes
    assert run.status.status_code is StatusCode.UNSET

    first, second = _spans_named(otel_spans, "datasette_cron.attempt")
    assert first.attributes["datasette_cron.attempt"] == 1
    assert first.attributes["error.type"] == "RuntimeError"
    assert first.status.status_code is StatusCode.ERROR
    assert any(e.name == "exception" for e in first.events)
    assert second.attributes["datasette_cron.attempt"] == 2
    assert second.status.status_code is StatusCode.UNSET

    backoff = _one_span(otel_spans, "datasette_cron.backoff")
    assert backoff.parent.span_id == run.context.span_id
    assert backoff.attributes["datasette_cron.backoff_delay"] == 0.01

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_final_failure_run_span(otel_spans):
    ds, scheduler = await _make_scheduler()

    async def broken(datasette, config):
        raise ValueError("boom")

    scheduler.register_handlers("test", {"broken": broken})
    await scheduler.add_task(
        name="t", handler="test:broken", schedule={"interval": 99999}
    )
    otel_spans.clear()

    await scheduler.trigger_task("t")
    await _drain(scheduler)

    run = _one_span(otel_spans, "datasette_cron.run")
    assert run.attributes["datasette_cron.status"] == "error"
    assert run.attributes["datasette_cron.attempts"] == 1
    assert run.attributes["error.type"] == "ValueError"
    assert run.status.status_code is StatusCode.ERROR
    assert run.status.description == "boom"

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_cancelled_run_span(otel_spans):
    ds, scheduler = await _make_scheduler()
    in_handler = asyncio.Event()

    async def hang(datasette, config):
        in_handler.set()
        await asyncio.sleep(60)

    scheduler.register_handlers("test", {"hang": hang})
    await scheduler.add_task(
        name="t", handler="test:hang", schedule={"interval": 99999}
    )
    otel_spans.clear()

    await scheduler.trigger_task("t")
    await asyncio.wait_for(in_handler.wait(), timeout=2.0)
    await scheduler.shutdown()

    run = _one_span(otel_spans, "datasette_cron.run")
    assert run.attributes["datasette_cron.status"] == "cancelled"
    assert run.attributes["error.type"] == "CancelledError"
    assert run.status.status_code is StatusCode.ERROR

    attempt = _one_span(otel_spans, "datasette_cron.attempt")
    assert attempt.attributes["error.type"] == "CancelledError"
    assert attempt.status.status_code is StatusCode.ERROR


@pytest.mark.asyncio
async def test_sync_handler_marked_on_attempt_span(otel_spans):
    ds, scheduler = await _make_scheduler()

    def sync_handler(datasette, config):
        pass

    scheduler.register_handlers("test", {"sync": sync_handler})
    await scheduler.add_task(
        name="t", handler="test:sync", schedule={"interval": 99999}
    )
    otel_spans.clear()

    await scheduler.trigger_task("t")
    await _drain(scheduler)

    attempt = _one_span(otel_spans, "datasette_cron.attempt")
    assert attempt.attributes["datasette_cron.handler.async"] is False

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_handler_queries_nest_under_attempt_span(otel_spans):
    ds, scheduler = await _make_scheduler()

    async def queries(datasette, config):
        await datasette.get_internal_database().execute("SELECT 1")

    scheduler.register_handlers("test", {"queries": queries})
    await scheduler.add_task(
        name="t", handler="test:queries", schedule={"interval": 99999}
    )
    otel_spans.clear()

    await scheduler.trigger_task("t")
    await _drain(scheduler)

    attempt = _one_span(otel_spans, "datasette_cron.attempt")
    children = [
        s
        for s in _spans_named(otel_spans, "db.query")
        if s.parent is not None and s.parent.span_id == attempt.context.span_id
    ]
    # The handler's own SELECT plus the runs-table bookkeeping writes all
    # parent to the attempt span, in the same trace as the run.
    assert any(
        s.attributes.get("db.query.text", "").startswith("SELECT 1") for s in children
    )
    run = _one_span(otel_spans, "datasette_cron.run")
    assert all(s.context.trace_id == run.context.trace_id for s in children)

    await scheduler.shutdown()


# --- datasette_cron.register_handlers -------------------------------------


def _register_handlers_span_for(otel_spans, plugin_name):
    spans = [
        s
        for s in _spans_named(otel_spans, "datasette_cron.register_handlers")
        if s.attributes.get("datasette.plugin") == plugin_name
    ]
    assert len(spans) == 1
    return spans[0]


@pytest.mark.asyncio
async def test_register_handlers_span(otel_spans):
    class TwoHandlersPlugin:
        @staticmethod
        @hookimpl
        def cron_register_handlers(datasette):
            async def one(datasette, config):
                pass

            async def two(datasette, config):
                pass

            return {"one": one, "two": two}

    pm.register(TwoHandlersPlugin, name="test_two_handlers_plugin")
    try:
        ds, scheduler = await _make_scheduler()
    finally:
        pm.unregister(TwoHandlersPlugin, name="test_two_handlers_plugin")

    span = _register_handlers_span_for(otel_spans, "TwoHandlersPlugin")
    assert span.attributes["datasette_cron.handlers"] == 2
    assert span.status.status_code is StatusCode.UNSET
    assert "TwoHandlersPlugin:one" in scheduler.list_handlers()

    # Child of core's startup span, not a root.
    startup = _one_span(otel_spans, "datasette.startup")
    assert span.parent is not None
    assert span.parent.span_id == startup.context.span_id

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_register_handlers_span_plugin_raises(otel_spans):
    class BrokenPlugin:
        @staticmethod
        @hookimpl
        def cron_register_handlers(datasette):
            raise RuntimeError("registration broken")

    pm.register(BrokenPlugin, name="test_broken_plugin")
    try:
        ds, scheduler = await _make_scheduler()
    finally:
        pm.unregister(BrokenPlugin, name="test_broken_plugin")

    # Startup completed despite the raise.
    assert scheduler is not None

    span = _register_handlers_span_for(otel_spans, "BrokenPlugin")
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes["error.type"] == "RuntimeError"
    assert any(e.name == "exception" for e in span.events)
    assert "datasette_cron.handlers" not in span.attributes

    await scheduler.shutdown()


# --- datasette_cron.tick --------------------------------------------------


@pytest.mark.asyncio
async def test_tick_span_emitted_by_loop(otel_spans):
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"noop": noop})
    await scheduler.add_task(
        name="t", handler="test:noop", schedule={"interval": 99999}
    )
    await scheduler.internal_db.update_next_run("t", PAST)
    otel_spans.clear()

    await ds.start_background_tasks()
    await asyncio.sleep(0.3)
    await ds.invoke_shutdown()

    ticks = _spans_named(otel_spans, "datasette_cron.tick")
    assert ticks, "expected at least one tick span"
    first = ticks[0]
    assert first.parent is None
    assert first.attributes["datasette_cron.due"] == 1
    assert first.attributes["datasette_cron.spawned"] == 1
    assert first.attributes["datasette_cron.skipped"] == 0
    assert first.attributes["datasette_cron.cancelled"] == 0
    assert first.attributes["datasette_cron.disabled"] == 0
    assert first.attributes["datasette_cron.sleep"] > 0
    assert first.status.status_code is StatusCode.UNSET

    # The loop's own queries nest under the tick span instead of being
    # orphan roots.
    tick_children = [
        s
        for s in _spans_named(otel_spans, "db.query")
        if s.parent is not None and s.parent.span_id == first.context.span_id
    ]
    assert tick_children

    # The scheduled run's link points back at the tick that spawned it.
    run = _one_span(otel_spans, "datasette_cron.run")
    assert len(run.links) == 1
    assert run.links[0].context.span_id == first.context.span_id


@pytest.mark.asyncio
async def test_tick_span_error_status(otel_spans, monkeypatch):
    ds, scheduler = await _make_scheduler()

    async def explode():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(scheduler.internal_db, "get_due_tasks", explode)
    otel_spans.clear()

    await ds.start_background_tasks()
    await asyncio.sleep(0.1)
    await ds.invoke_shutdown()

    tick = _spans_named(otel_spans, "datasette_cron.tick")[0]
    assert tick.status.status_code is StatusCode.ERROR
    assert "datasette_cron.sleep" not in tick.attributes


@pytest.mark.asyncio
async def test_tick_stats_overlap_skip():
    ds, scheduler = await _make_scheduler()
    in_handler = asyncio.Event()
    release = asyncio.Event()

    async def slow(datasette, config):
        in_handler.set()
        await release.wait()

    scheduler.register_handlers("test", {"slow": slow})
    await scheduler.add_task(
        name="t", handler="test:slow", schedule={"interval": 99999}, overlap="skip"
    )
    await scheduler.internal_db.update_next_run("t", PAST)

    stats = await scheduler._tick()
    assert (stats.due, stats.spawned, stats.skipped) == (1, 1, 0)
    await asyncio.wait_for(in_handler.wait(), timeout=2.0)

    await scheduler.internal_db.update_next_run("t", PAST)
    stats = await scheduler._tick()
    assert (stats.due, stats.spawned, stats.skipped) == (1, 0, 1)

    release.set()
    await _drain(scheduler)
    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_tick_stats_overlap_cancel():
    ds, scheduler = await _make_scheduler()
    in_handler = asyncio.Event()

    async def slow(datasette, config):
        in_handler.set()
        await asyncio.sleep(60)

    scheduler.register_handlers("test", {"slow": slow})
    await scheduler.add_task(
        name="t", handler="test:slow", schedule={"interval": 99999}, overlap="cancel"
    )
    await scheduler.internal_db.update_next_run("t", PAST)

    stats = await scheduler._tick()
    assert (stats.spawned, stats.cancelled) == (1, 0)
    await asyncio.wait_for(in_handler.wait(), timeout=2.0)

    await scheduler.internal_db.update_next_run("t", PAST)
    stats = await scheduler._tick()
    assert (stats.due, stats.spawned, stats.cancelled) == (1, 1, 1)

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_tick_stats_disabled_missing_handler():
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"noop": noop})
    await scheduler.add_task(
        name="t", handler="test:noop", schedule={"interval": 99999}
    )
    await scheduler.internal_db.update_next_run("t", PAST)
    scheduler._handler_registry.clear()

    stats = await scheduler._tick()
    assert (stats.due, stats.spawned, stats.disabled) == (1, 0, 1)
    task = await scheduler.internal_db.get_task("t")
    assert task.enabled is False

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_run_span_is_root_with_link_not_child(otel_spans):
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"noop": noop})
    await scheduler.add_task(
        name="t", handler="test:noop", schedule={"interval": 99999}
    )
    await scheduler.internal_db.update_next_run("t", PAST)
    otel_spans.clear()

    with tracer.start_as_current_span("outer") as outer:
        await scheduler._tick()
        await _drain(scheduler)

    run = _one_span(otel_spans, "datasette_cron.run")
    assert run.parent is None
    assert len(run.links) == 1
    assert run.links[0].context.span_id == outer.get_span_context().span_id

    await scheduler.shutdown()


# --- metrics: run.duration, run.lag, attempts, overlaps -------------------


def test_lag_seconds():
    now = _utcnow()
    assert _lag_seconds(PAST, now) > 0
    assert _lag_seconds("2099-01-01T00:00:00", now) == 0.0


@pytest.mark.asyncio
async def test_metrics_successful_scheduled_run(otel_metrics):
    ds, scheduler = await _make_scheduler()

    async def noop(datasette, config):
        pass

    scheduler.register_handlers("test", {"noop": noop})
    await scheduler.add_task(
        name="m1", handler="test:noop", schedule={"interval": 99999}
    )
    await scheduler.internal_db.update_next_run("m1", PAST)

    await scheduler._tick()
    await _drain(scheduler)

    otel_metrics.collect()
    duration = otel_metrics.point(
        "datasette_cron.run.duration",
        {
            "datasette_cron.task": "m1",
            "datasette_cron.handler": "test:noop",
            "datasette_cron.status": "success",
            "datasette_cron.trigger": "scheduled",
        },
    )
    assert duration.count == 1
    lag = otel_metrics.point("datasette_cron.run.lag", {"datasette_cron.task": "m1"})
    assert lag.count == 1
    assert lag.sum >= 0
    attempt_point = otel_metrics.point(
        "datasette_cron.attempts",
        {
            "datasette_cron.task": "m1",
            "datasette_cron.status": "success",
            "datasette_cron.retry": False,
        },
    )
    assert attempt_point.value == 1
    assert otel_metrics.points("datasette_cron.overlaps") == []

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_metrics_retry(otel_metrics, monkeypatch):
    ds, scheduler = await _make_scheduler()
    monkeypatch.setattr(
        type(scheduler), "_backoff_delay", staticmethod(lambda strategy, attempt: 0.01)
    )
    calls = 0

    async def flaky(datasette, config):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first attempt fails")

    scheduler.register_handlers("test", {"flaky": flaky})
    await scheduler.add_task(
        name="m2",
        handler="test:flaky",
        schedule={"interval": 99999},
        retry={"max_retries": 1},
    )

    await scheduler.trigger_task("m2")
    await _drain(scheduler)

    otel_metrics.collect()
    failed = otel_metrics.point(
        "datasette_cron.attempts",
        {
            "datasette_cron.task": "m2",
            "datasette_cron.status": "error",
            "datasette_cron.retry": False,
        },
    )
    assert failed.value == 1
    retried = otel_metrics.point(
        "datasette_cron.attempts",
        {
            "datasette_cron.task": "m2",
            "datasette_cron.status": "success",
            "datasette_cron.retry": True,
        },
    )
    assert retried.value == 1
    durations = otel_metrics.points(
        "datasette_cron.run.duration", {"datasette_cron.task": "m2"}
    )
    assert sorted(p.attributes["datasette_cron.status"] for p in durations) == [
        "error",
        "success",
    ]
    assert all(p.attributes["datasette_cron.trigger"] == "manual" for p in durations)

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_metrics_overlap_skip_and_cancel(otel_metrics):
    ds, scheduler = await _make_scheduler()
    release = asyncio.Event()
    started = asyncio.Event()

    async def slow(datasette, config):
        started.set()
        await release.wait()

    scheduler.register_handlers("test", {"slow": slow})
    await scheduler.add_task(
        name="m3", handler="test:slow", schedule={"interval": 99999}, overlap="skip"
    )
    await scheduler.add_task(
        name="m4", handler="test:slow", schedule={"interval": 99999}, overlap="cancel"
    )

    for name in ("m3", "m4"):
        await scheduler.internal_db.update_next_run(name, PAST)
    await scheduler._tick()
    await asyncio.wait_for(started.wait(), timeout=2.0)
    for name in ("m3", "m4"):
        await scheduler.internal_db.update_next_run(name, PAST)
    await scheduler._tick()

    release.set()
    await _drain(scheduler)

    otel_metrics.collect()
    skip = otel_metrics.point(
        "datasette_cron.overlaps",
        {"datasette_cron.task": "m3", "datasette_cron.overlap_policy": "skip"},
    )
    assert skip.value == 1
    cancel = otel_metrics.point(
        "datasette_cron.overlaps",
        {"datasette_cron.task": "m4", "datasette_cron.overlap_policy": "cancel"},
    )
    assert cancel.value == 1

    await scheduler.shutdown()
