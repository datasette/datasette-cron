"""
Smoke tests for the telemetry scaffolding: the tracer emits under our own
instrumentation scope, and core's root-with-link helper is importable and
wired up. The helper's own behaviour is tested in core's suite.
"""

from datasette.telemetry import SCHEMA_URL, linked_root_span_kwargs

from datasette_cron.telemetry import tracer


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
