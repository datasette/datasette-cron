"""
The single source of truth for every span and metric datasette-cron emits.

Three things read this module, which is the point of it existing:

1. **The instrumentation itself.** The entries subclass `str` (they are
   instances of core's public registry classes), so a registry entry *is*
   the string OpenTelemetry wants, and a typo is an `ImportError` instead
   of a silently misnamed signal.

2. **The documentation.** `scripts/telemetry-doc.py` renders the README's
   telemetry reference from these definitions, so the docs cannot drift
   from the code. Descriptions are Markdown.

3. **A conformance test.** `tests/test_telemetry_registry.py` runs a real
   workload, collects every span and metric actually emitted under the
   `datasette_cron` scope, and compares both directions:
   emitted-but-unregistered catches instrumentation added without
   documentation; registered-but-never-emitted catches documentation
   describing something that no longer exists.
"""

from datasette.telemetry_registry import (
    COUNTER,
    HISTOGRAM,
    Attribute,
    MetricName,
    SpanName,
)


# --- Attributes -----------------------------------------------------------
#
# Shared attributes are defined once and referenced by every span or metric
# that sets them. The first three reuse core's keys - not its `Attribute`
# instances, which carry core-specific prose - so a query across the
# `datasette` and `datasette_cron` scopes joins on the same key.

PLUGIN = Attribute(
    "datasette.plugin",
    "Name of the plugin the handler belongs to - the `plugin` half of the "
    "handler reference. Core's key, reused so cross-scope queries join.",
)
CODE_FUNCTION = Attribute(
    "code.function",
    "Qualified name of the handler function, "
    "`{module}.{qualname}`. The semconv 1.29 spelling (renamed "
    "`code.function.name` in 1.30), matching core's schema URL.",
)
ERROR_TYPE = Attribute(
    "error.type",
    "Exception class name when the work failed; `CancelledError` when it "
    "was cancelled. Never the exception message.",
    optional=True,
)
TASK = Attribute(
    "datasette_cron.task",
    "Task name. Set by plugin code, so bounded and safe as a metric dimension.",
)
HANDLER = Attribute(
    "datasette_cron.handler",
    "Handler reference, `plugin:name`.",
)
STATUS = Attribute(
    "datasette_cron.status",
    "How the run ended.",
    values={"success", "error", "cancelled"},
)
TRIGGER = Attribute(
    "datasette_cron.trigger",
    "What started the run.",
    values={"scheduled", "manual"},
)
MAX_ATTEMPTS = Attribute(
    "datasette_cron.max_attempts",
    "How many attempts this run was allowed: the task's `retry_max + 1`.",
)
ATTEMPTS = Attribute(
    "datasette_cron.attempts",
    "How many attempts were actually made, set when the run ends. Less "
    "than `datasette_cron.max_attempts` when an attempt succeeded early "
    "or the run was cancelled.",
)
SCHEDULED_AT = Attribute(
    "datasette_cron.scheduled_at",
    "The `next_run_at` slot the scheduler fired for, as a naive-UTC ISO "
    "string. Scheduled runs only; a manual trigger has no slot.",
    optional=True,
)
LAG = Attribute(
    "datasette_cron.lag",
    "Seconds between `datasette_cron.scheduled_at` and the tick that "
    "fired it. Scheduled runs only.",
    optional=True,
)
ATTEMPT = Attribute(
    "datasette_cron.attempt",
    "1-based attempt number within the run.",
)
RUN_ID = Attribute(
    "datasette_cron.run_id",
    "The `datasette_cron_runs.id` row recording this attempt, joining the "
    "trace to the run history the UI shows.",
)
HANDLER_ASYNC = Attribute(
    "datasette_cron.handler.async",
    "`True` if the handler returned a coroutine. A sync handler blocks "
    "the event loop for its whole duration, and this attribute is the "
    "only place that becomes visible.",
)
BACKOFF_DELAY = Attribute(
    "datasette_cron.backoff_delay",
    "The jittered delay in seconds slept before the next attempt.",
)
DUE = Attribute(
    "datasette_cron.due",
    "How many tasks `get_due_tasks` returned for this tick.",
)
SPAWNED = Attribute(
    "datasette_cron.spawned",
    "How many executions this tick started.",
)
SKIPPED = Attribute(
    "datasette_cron.skipped",
    "Due tasks skipped because `overlap_policy=skip` found a run already in flight.",
)
CANCELLED = Attribute(
    "datasette_cron.cancelled",
    "Due tasks whose in-flight runs this tick cancelled because "
    "`overlap_policy=cancel`, before starting the new run.",
)
DISABLED = Attribute(
    "datasette_cron.disabled",
    "Due tasks this tick disabled because their handler is not registered.",
)
SLEEP = Attribute(
    "datasette_cron.sleep",
    "Seconds the loop decided to wait before the next tick, capped at 60.",
)
HANDLERS = Attribute(
    "datasette_cron.handlers",
    "Number of handlers the plugin returned.",
)
RETRY = Attribute(
    "datasette_cron.retry",
    '`True` for attempt 2 onwards - separating "flaky, recovers" from '
    '"broken" in the attempts counter. A boolean, so bounded without a '
    "declared enum.",
)
OVERLAP_POLICY = Attribute(
    "datasette_cron.overlap_policy",
    "What the task's overlap policy did about the run already in flight: "
    "`skip` dropped the new run, `cancel` cancelled the old one.",
    values={"skip", "cancel"},
)


# --- Spans ----------------------------------------------------------------

RUN = SpanName(
    "datasette_cron.run",
    "One span per execution of a task, covering every attempt and every "
    "backoff sleep between them. A **root span** in its own trace, with an "
    "OpenTelemetry link back to the span that caused it - the "
    "`datasette_cron.tick` iteration for a scheduled run, core's HTTP "
    "request span for a manual trigger. A run outlives the tick or request "
    "that spawned it, so a link records the causation without asserting "
    "containment (the same shape core uses for `block=False` writes). "
    "Span status is `ERROR` when the last attempt failed or the run was "
    "cancelled; a failed attempt that was then retried successfully leaves "
    "the run span unset, with the failure visible on the attempt span.",
    (
        TASK,
        HANDLER,
        PLUGIN,
        CODE_FUNCTION,
        TRIGGER,
        MAX_ATTEMPTS,
        SCHEDULED_AT,
        LAG,
        ATTEMPTS,
        STATUS,
        ERROR_TYPE,
    ),
)

ATTEMPT_SPAN = SpanName(
    "datasette_cron.attempt",
    "One attempt at running the handler, child of `datasette_cron.run`. "
    "Wraps the bookkeeping write that opens the runs-table row, the "
    "handler call itself, and the write that closes the row - so the "
    "span's duration is the same start-to-finished window the runs table "
    "shows, and the handler's own `db.query` spans (core's) nest here "
    "automatically. Status is `ERROR` on failure or cancellation, with "
    "the stack trace recorded as an exception event.",
    (ATTEMPT, RUN_ID, HANDLER_ASYNC, ERROR_TYPE),
)

BACKOFF = SpanName(
    "datasette_cron.backoff",
    "The sleep between two attempts of a retried run, child of "
    "`datasette_cron.run` and sibling of the attempt spans. Exists so the "
    "gap in a retried trace is labelled rather than mysterious, and so "
    "retry timing can be inspected without a metric.",
    (BACKOFF_DELAY,),
)

TICK = SpanName(
    "datasette_cron.tick",
    "One iteration of the scheduler loop: reading due tasks, spawning "
    "executions and computing the next sleep. A root span - the loop runs "
    "as a supervised background task with no ambient span - and the "
    "parent every loop-owned `db.query` nests under. Emitted **at least "
    "once a minute per process**, including no-op ticks, with the outcome "
    "attributes saying so: suppressing quiet ticks would destroy the "
    '"is the loop still running?" signal. Operators who find one span a '
    "minute noisy can drop it by name in a `SpanProcessor` (or a sampler "
    "keyed on span name), filtering for spans named `datasette_cron.tick` "
    "with no error status. Status is `ERROR` when the tick raised - the "
    "loop logs and continues, and this span is how an operator notices "
    "that happening repeatedly. The wait between ticks and the error "
    "sleep are outside the span, so its duration means work.",
    (DUE, SPAWNED, SKIPPED, CANCELLED, DISABLED, SLEEP),
)

REGISTER_HANDLERS = SpanName(
    "datasette_cron.register_handlers",
    "One plugin's `cron_register_handlers` implementation running during "
    "the `startup` hook. Child of whatever is current - core's "
    "`datasette.startup` span today. The registration loop swallows and "
    "logs a plugin's exception so one buggy plugin cannot take down the "
    "scheduler; a red span in the startup trace is the signal that log "
    "line is not, because a plugin that fails here boots a Datasette "
    "whose tasks silently get disabled on first tick.",
    (PLUGIN, HANDLERS, ERROR_TYPE),
)

SPANS: tuple[SpanName, ...] = (RUN, ATTEMPT_SPAN, BACKOFF, TICK, REGISTER_HANDLERS)


# --- Metrics --------------------------------------------------------------

# Histograms are in seconds with explicit boundaries, following core's
# reasoning (OTel's default boundaries assume milliseconds). Core's shared
# DURATION_BUCKETS tops out at 10 s, tuned for SQLite reads; cron jobs run
# for minutes, so these boundary lists are deliberately our own, reaching
# an hour. Published in the generated docs - an operator writing a
# histogram_quantile() query needs to know them.
RUN_DURATION_BUCKETS = (0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600)
LAG_BUCKETS = (0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600)

M_RUN_DURATION = MetricName(
    "datasette_cron.run.duration",
    HISTOGRAM,
    "s",
    "Duration of one attempt: the handler call only, the same number "
    "written to the runs table's `duration_ms`. A retried run records two "
    "measurements; backoff sleeps are visible as their own span, not "
    "folded in here. By `datasette_cron.task` this is the dashboard; by "
    "`datasette_cron.status` it separates a slow success from a slow "
    "failure. Recorded inside the attempt span, so exemplars link each "
    "bucket to a trace.",
    (TASK, HANDLER, STATUS, TRIGGER),
    buckets=RUN_DURATION_BUCKETS,
)

M_RUN_LAG = MetricName(
    "datasette_cron.run.lag",
    HISTOGRAM,
    "s",
    "Seconds between a task's scheduled slot (`next_run_at`) and the tick "
    "that fired it - scheduler promptness. It climbs when a sync handler "
    "starves the event loop, when thread-pool contention slows the due "
    "query, or after a restart. Recorded once per due task, spawned or "
    "not, so an overlap-starved task still shows its slot drifting.",
    (TASK,),
    buckets=LAG_BUCKETS,
)

M_ATTEMPTS = MetricName(
    "datasette_cron.attempts",
    COUNTER,
    "{attempt}",
    "Attempts at running a task's handler, by outcome. With "
    "`datasette_cron.status=error` per task this is the alert; with "
    '`datasette_cron.retry=true` it separates "flaky, recovers" from '
    '"broken". Deliberately no `error.type` dimension: a handler can '
    "raise anything, so the class name is not a bounded value set here.",
    (TASK, HANDLER, STATUS, RETRY),
)

M_OVERLAPS = MetricName(
    "datasette_cron.overlaps",
    COUNTER,
    "{run}",
    "Due runs that found an earlier run of the same task still in flight, "
    "with what the overlap policy did about it. Sustained above zero for "
    "a task means its runtime exceeds its interval, which "
    "`datasette_cron.run.duration` alone does not tell you.",
    (TASK, OVERLAP_POLICY),
)

METRICS: tuple[MetricName, ...] = (M_RUN_DURATION, M_RUN_LAG, M_ATTEMPTS, M_OVERLAPS)
