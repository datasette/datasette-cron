# Importing the fixture names registers them: session-scoped autouse
# tracer/meter providers with in-memory synchronous export (silently
# skipped when the SDK is not installed), and a per-test reset that drains
# the exporter and reader. See the kit's docstrings for the
# once-per-process/ProxyTracer reasoning.
from datasette.telemetry_testing import (  # noqa: F401
    MetricsCollector,
    otel_metrics,
    otel_meter_provider,
    otel_provider,
    otel_reset,
    otel_spans,
)
