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

from datasette.telemetry_registry import Attribute, MetricName, SpanName

__all__ = [
    "Attribute",
    "MetricName",
    "SpanName",
    "PLUGIN",
    "CODE_FUNCTION",
    "ERROR_TYPE",
    "TASK",
    "HANDLER",
    "STATUS",
    "TRIGGER",
    "SPANS",
    "METRICS",
]


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


# --- Spans ----------------------------------------------------------------

SPANS: tuple[SpanName, ...] = ()


# --- Metrics --------------------------------------------------------------

METRICS: tuple[MetricName, ...] = ()
