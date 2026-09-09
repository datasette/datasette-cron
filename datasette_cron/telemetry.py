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

from importlib.metadata import version

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace as otel_trace

# Core's SCHEMA_URL comment explains why it is 1.29.0 and not the latest:
# it is a claim about the spellings on the wire. The only semconv names
# this plugin emits are `error.type` and `code.function`, and
# `code.function` is the 1.29 spelling (renamed `code.function.name` in
# 1.30). Importing the URL keeps the two libraries making the same claim.
from datasette.telemetry import SCHEMA_URL

__version__ = version("datasette-cron")

tracer = otel_trace.get_tracer("datasette_cron", __version__, schema_url=SCHEMA_URL)
meter = otel_metrics.get_meter("datasette_cron", __version__, schema_url=SCHEMA_URL)
