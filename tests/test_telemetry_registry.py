"""
Two-way conformance between `datasette_cron/telemetry_registry.py` and what
the scheduler actually emits.

This is the test that makes the generated README reference trustworthy.
`just telemetry-doc-check` guarantees the docs match the registry; this
guarantees the registry matches the code. It checks both directions:

- **emitted but not registered** - instrumentation added without
  documenting it, so the reference silently omits it.
- **registered but never emitted** - the reference describes a signal that
  no longer exists, which is worse, because a reader will build a
  dashboard on it.

Neither direction can catch a *rename*, because the call sites take their
names from the registry - so the literal names live here too, spelled out,
and are asserted against the registry. That table is the one place in the
codebase where signal-name literals belong.

The privacy walk plants sentinel values in the workload - a secret in a
task's config, a handler return value, a distinctive schedule string, an
exception message - and asserts none leaks into any signal (design rule:
"never recorded, anywhere"; the exception message is permitted in span
status/events but banned from metrics).
"""

import asyncio

import pytest

pytest.importorskip("opentelemetry.sdk")

from datasette import hookimpl
from datasette.app import Datasette
from datasette.plugins import pm
from datasette.telemetry_testing import (
    assert_metrics_conform,
    assert_metrics_covered,
    assert_no_forbidden_values,
    assert_package_never_imports_sdk,
    assert_spans_conform,
    assert_spans_covered,
)

from datasette_cron import telemetry_registry as reg
from datasette_cron.scheduler import Scheduler

PAST = "2000-01-01T00:00:00"
SCOPE = "datasette_cron"

CONFIG_SECRET = "cfg-secret-hunter2-XYZZY"
RETURN_SENTINEL = "handler-return-sentinel-XYZZY"
EXC_MESSAGE = "exc-message-sentinel-XYZZY"
# A distinctive fragment of one task's schedule_config.
SCHEDULE_SENTINEL = "43 4 * * SAT"


def test_package_never_imports_the_sdk():
    # Front-loaded by conftest's pytest_collection_modifyitems - the
    # helper's docstring documents a macOS/CPython 3.13 fork+exec crash
    # when subprocess-spawning tests run late in a thread-heavy process.
    assert_package_never_imports_sdk("datasette_cron")


# The names as they appear on the wire, written out rather than read from
# the registry. If a registry change makes one of these fail, that change
# is renaming something a user's dashboards depend on - a decision to take
# deliberately, here, not a line to re-derive.
EXPECTED_ATTRIBUTES = {
    "datasette_cron.run": {
        "datasette_cron.task",
        "datasette_cron.handler",
        "datasette.plugin",
        "code.function",
        "datasette_cron.trigger",
        "datasette_cron.max_attempts",
        "datasette_cron.scheduled_at",
        "datasette_cron.lag",
        "datasette_cron.attempts",
        "datasette_cron.status",
        "error.type",
    },
    "datasette_cron.attempt": {
        "datasette_cron.attempt",
        "datasette_cron.run_id",
        "datasette_cron.handler.async",
        "error.type",
    },
    "datasette_cron.backoff": {"datasette_cron.backoff_delay"},
    "datasette_cron.tick": {
        "datasette_cron.due",
        "datasette_cron.spawned",
        "datasette_cron.skipped",
        "datasette_cron.cancelled",
        "datasette_cron.disabled",
        "datasette_cron.sleep",
    },
    "datasette_cron.register_handlers": {
        "datasette.plugin",
        "datasette_cron.handlers",
        "error.type",
    },
}

EXPECTED_METRIC_ATTRIBUTES = {
    "datasette_cron.run.duration": {
        "datasette_cron.task",
        "datasette_cron.handler",
        "datasette_cron.status",
        "datasette_cron.trigger",
    },
    "datasette_cron.run.lag": {"datasette_cron.task"},
    "datasette_cron.attempts": {
        "datasette_cron.task",
        "datasette_cron.handler",
        "datasette_cron.status",
        "datasette_cron.retry",
    },
    "datasette_cron.overlaps": {
        "datasette_cron.task",
        "datasette_cron.overlap_policy",
    },
    "datasette_cron.runs.active": {"datasette_cron.task"},
    "datasette_cron.tick.age": set(),
    "datasette_cron.tasks": {
        "datasette_cron.enabled",
        "datasette_cron.last_status",
    },
}


def test_registry_matches_expected_literal_names():
    assert {str(s) for s in reg.SPANS} == set(EXPECTED_ATTRIBUTES)
    for span in reg.SPANS:
        assert {str(a) for a in span.attributes} == EXPECTED_ATTRIBUTES[str(span)], str(
            span
        )
    assert {str(m) for m in reg.METRICS} == set(EXPECTED_METRIC_ATTRIBUTES)
    for metric in reg.METRICS:
        assert {str(a) for a in metric.attributes} == EXPECTED_METRIC_ATTRIBUTES[
            str(metric)
        ], str(metric)


def test_histograms_declare_buckets():
    # Units and kinds are checked instrument-vs-registry by
    # assert_metrics_conform; this catches an entry that forgot to declare
    # buckets at all.
    for metric in reg.METRICS:
        if metric.kind == reg.HISTOGRAM:
            assert metric.buckets, f"{metric} declares no bucket boundaries"


@pytest.mark.asyncio
async def test_registry_conformance(otel_spans, otel_metrics, monkeypatch):
    monkeypatch.setattr(
        Scheduler, "_backoff_delay", staticmethod(lambda strategy, attempt: 0.01)
    )
    flaky_calls = 0
    hang = asyncio.Event()

    class ConformancePlugin:
        @staticmethod
        @hookimpl
        def cron_register_handlers(datasette):
            async def ok(datasette, config):
                return RETURN_SENTINEL

            async def flaky(datasette, config):
                nonlocal flaky_calls
                flaky_calls += 1
                if flaky_calls == 1:
                    raise RuntimeError(EXC_MESSAGE)
                return RETURN_SENTINEL

            def sync_ok(datasette, config):
                return RETURN_SENTINEL

            async def slow(datasette, config):
                await hang.wait()

            return {"ok": ok, "flaky": flaky, "sync": sync_ok, "slow": slow}

    pm.register(ConformancePlugin, name="cron_conformance_plugin")
    try:
        ds = Datasette(
            memory=True,
            config={"permissions": {"datasette-cron-access": True}},
        )
        await ds.invoke_startup()
        scheduler = ds._cron_scheduler
        interval = {"interval": 90001}
        secret_config = {"api_key": CONFIG_SECRET}
        p = "ConformancePlugin"
        await scheduler.add_task(
            name="wf-ok", handler=f"{p}:ok", schedule=interval, config=secret_config
        )
        await scheduler.add_task(
            name="wf-cron",
            handler=f"{p}:ok",
            schedule=SCHEDULE_SENTINEL,
            config=secret_config,
        )
        await scheduler.add_task(
            name="wf-retry",
            handler=f"{p}:flaky",
            schedule=interval,
            config=secret_config,
            retry={"max_retries": 1},
        )
        await scheduler.add_task(name="wf-sync", handler=f"{p}:sync", schedule=interval)
        await scheduler.add_task(
            name="wf-skip", handler=f"{p}:slow", schedule=interval, overlap="skip"
        )
        await scheduler.add_task(
            name="wf-cancel", handler=f"{p}:slow", schedule=interval, overlap="cancel"
        )
        await scheduler.add_task(
            name="wf-missing", handler="nowhere:missing", schedule=interval
        )

        # Scheduled path, via the real loop: wf-ok fires (scheduled run,
        # lag), wf-missing gets disabled, and every loop iteration emits a
        # tick span and refreshes the tasks-gauge snapshot.
        await scheduler.internal_db.update_next_run("wf-ok", PAST)
        await scheduler.internal_db.update_next_run("wf-missing", PAST)
        await ds.start_background_tasks()
        await asyncio.sleep(0.3)

        # Manual path over HTTP: run span linked to core's request span.
        response = await ds.client.post("/-/api/cron/tasks/wf-sync/trigger", json={})
        assert response.status_code == 200

        # Retry path: failed attempt, backoff span, successful attempt.
        await scheduler.trigger_task("wf-retry")

        # Overlap paths: first tick spawns both slow runs, second tick
        # skips one and cancels/respawns the other.
        for name in ("wf-skip", "wf-cancel"):
            await scheduler.internal_db.update_next_run(name, PAST)
        await scheduler._tick()
        for _ in range(100):
            if scheduler.is_running("wf-skip") and scheduler.is_running("wf-cancel"):
                break
            await asyncio.sleep(0.01)
        for name in ("wf-skip", "wf-cancel"):
            await scheduler.internal_db.update_next_run(name, PAST)
        await scheduler._tick()
        await asyncio.sleep(0.2)

        # One collect(), while the slow runs are still in flight so the
        # runs.active gauge has something to observe. Delta temporality:
        # an earlier collect() would drain the measurements.
        otel_metrics.collect()

        hang.set()
        pending = [t for ts in scheduler._running_tasks.values() for t in ts]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await ds.invoke_shutdown()
    finally:
        pm.unregister(ConformancePlugin, name="cron_conformance_plugin")

    finished = otel_spans.get_finished_spans()
    assert_spans_conform(reg.SPANS, finished, scope_name=SCOPE)
    assert_spans_covered(reg.SPANS, finished, scope_name=SCOPE)
    assert_metrics_conform(reg.METRICS, otel_metrics, scope_name=SCOPE)
    assert_metrics_covered(reg.METRICS, otel_metrics, scope_name=SCOPE)

    # Privacy walk, design.md's "never recorded, anywhere" list.
    # scope_name deliberately unset: a leak through core's signals is
    # still a leak.
    assert_no_forbidden_values(
        {CONFIG_SECRET, RETURN_SENTINEL, SCHEDULE_SENTINEL},
        finished_spans=finished,
        collector=otel_metrics,
    )
    # The exception message is deliberately on span status and exception
    # events; it is banned from metrics.
    assert_no_forbidden_values({EXC_MESSAGE}, collector=otel_metrics)
