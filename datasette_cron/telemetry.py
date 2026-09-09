"""
OpenTelemetry integration for datasette-cron.

Like Datasette core, this plugin depends on `opentelemetry-api` only. It
never creates a `TracerProvider` or a `MeterProvider`, never configures an
exporter, and never touches sampling - that is the responsibility of
whoever is running Datasette (an `opentelemetry-instrument` agent, or a
test harness). With no provider installed every span is a
`NonRecordingSpan` and every instrument is a no-op; core's page benchmarks
put that overhead below run-to-run variation, and this plugin emits far
fewer signals per minute than a single table page does.

The tracer and meter live under their own `datasette_cron` instrumentation
scope, versioned with the plugin - not core's `datasette` scope - so a
consumer can filter and version the two libraries independently. Context
propagation is via contextvars, so parenting works across scopes: this
plugin's spans nest inside core's and vice versa.

For root-with-link spans (a run caused by a tick or an HTTP request but
not contained by it) use `datasette.telemetry.linked_root_span_kwargs()`;
there is deliberately no local helper.
"""

import threading
import time
import weakref
from collections import Counter
from importlib.metadata import version

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace as otel_trace

# Core's SCHEMA_URL comment explains why it is 1.29.0 and not the latest:
# it is a claim about the spellings on the wire. The only semconv names
# this plugin emits are `error.type` and `code.function`, and
# `code.function` is the 1.29 spelling (renamed `code.function.name` in
# 1.30). Importing the URL keeps the two libraries making the same claim.
from datasette.telemetry import SCHEMA_URL

from .telemetry_registry import (
    ENABLED,
    LAST_STATUS,
    M_ATTEMPTS,
    M_OVERLAPS,
    M_RUN_DURATION,
    M_RUN_LAG,
    M_RUNS_ACTIVE,
    M_TASKS,
    M_TICK_AGE,
    TASK,
)

__version__ = version("datasette-cron")

tracer = otel_trace.get_tracer("datasette_cron", __version__, schema_url=SCHEMA_URL)
meter = otel_metrics.get_meter("datasette_cron", __version__, schema_url=SCHEMA_URL)

# Module-level instruments are safe before any provider exists: _ProxyMeter
# instruments forward retroactively once one is installed. The registry
# descriptions are Markdown for the generated docs; the exported
# description strings here are short plain sentences.

run_duration = meter.create_histogram(
    M_RUN_DURATION,
    unit=M_RUN_DURATION.unit,
    description="Duration of one attempt of a cron task run, the handler call only",
    explicit_bucket_boundaries_advisory=M_RUN_DURATION.buckets,
)

run_lag = meter.create_histogram(
    M_RUN_LAG,
    unit=M_RUN_LAG.unit,
    description="Seconds between a task's scheduled slot and the tick that fired it",
    explicit_bucket_boundaries_advisory=M_RUN_LAG.buckets,
)

attempts = meter.create_counter(
    M_ATTEMPTS,
    unit=M_ATTEMPTS.unit,
    description="Attempts at running a cron task's handler, by outcome",
)

overlaps = meter.create_counter(
    M_OVERLAPS,
    unit=M_OVERLAPS.unit,
    description="Due runs that found an earlier run of the same task in flight",
)


# --- Observable gauges ----------------------------------------------------
#
# Mirrors core's _live_datasettes plumbing (a pattern, not an import: its
# WeakSet holds Datasette instances, ours holds schedulers). The callbacks
# run on the SDK's metric collection thread: they read in-memory state
# only - no awaits, no SQLite, no lock shared with the event loop.
# Iterating list(...) copies of dicts mutated on the event loop is the
# accepted race, same as core's len() on _pending_execute_futures.

_live_schedulers = weakref.WeakSet()
_live_schedulers_lock = threading.Lock()


def register_scheduler(scheduler):
    "Track a scheduler so the observable gauges report on it."
    with _live_schedulers_lock:
        _live_schedulers.add(scheduler)


def unregister_scheduler(scheduler):
    "Stop reporting on a scheduler; called from its shutdown."
    with _live_schedulers_lock:
        _live_schedulers.discard(scheduler)


def _live():
    with _live_schedulers_lock:
        return list(_live_schedulers)


def observe_runs_active(options=None):
    for scheduler in _live():
        for name, tasks in list(scheduler._running_tasks.items()):
            active = sum(1 for t in list(tasks) if not t.done())
            if active:
                yield otel_metrics.Observation(active, {TASK: name})


def observe_tick_age(options=None):
    now = time.monotonic()
    for scheduler in _live():
        if scheduler._last_tick_finished is not None:
            yield otel_metrics.Observation(now - scheduler._last_tick_finished, {})


def observe_tasks(options=None):
    for scheduler in _live():
        counts = Counter(
            (bool(task.enabled), task.last_status or "none")
            for task in scheduler._task_snapshot
        )
        for (enabled, last_status), n in counts.items():
            yield otel_metrics.Observation(
                n, {ENABLED: enabled, LAST_STATUS: last_status}
            )


runs_active_gauge = meter.create_observable_gauge(
    M_RUNS_ACTIVE,
    callbacks=[observe_runs_active],
    unit=M_RUNS_ACTIVE.unit,
    description="Cron task executions currently in flight",
)

tick_age_gauge = meter.create_observable_gauge(
    M_TICK_AGE,
    callbacks=[observe_tick_age],
    unit=M_TICK_AGE.unit,
    description="Seconds since the scheduler loop last finished a tick",
)

tasks_gauge = meter.create_observable_gauge(
    M_TASKS,
    callbacks=[observe_tasks],
    unit=M_TASKS.unit,
    description="Registered cron tasks by enabled state and last run status",
)
