"""The plugin's own configuration: the ``plugins: datasette-cron:`` block.

Validated once at startup. A typo'd key or a bad value fails startup with a
pydantic ValidationError naming the field, rather than being silently
ignored.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, field_validator

if TYPE_CHECKING:
    from datasette.app import Datasette


class CronConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        title="datasette-cron plugin config",
        use_attribute_docstrings=True,
    )

    trace_url: str | None = None
    """URL template linking a run's stored trace to a tracing UI, e.g.
    ``http://localhost:16686/trace/{trace_id}``. ``{trace_id}`` is required
    and ``{span_id}`` is optional. When unset, the detail page shows a
    click-to-copy trace id prefix instead of a link."""

    @field_validator("trace_url")
    @classmethod
    def _require_trace_id_placeholder(cls, value: str | None) -> str | None:
        if value is not None and "{trace_id}" not in value:
            raise ValueError("must contain a {trace_id} placeholder")
        return value


def load_config(datasette: Datasette) -> CronConfig:
    return CronConfig.model_validate(datasette.plugin_config("datasette-cron") or {})
