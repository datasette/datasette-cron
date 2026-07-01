from __future__ import annotations

from pydantic import BaseModel

from .models import CronTask
from .schedules import describe_schedule


class TaskSummary(BaseModel):
    name: str
    handler: str
    schedule_type: str
    schedule_description: str
    # Raw seconds for interval schedules; None otherwise. The frontend uses
    # this to classify "continuous" (< 10s) without parsing schedule_description.
    schedule_seconds: float | None
    timezone: str | None
    enabled: bool
    next_run_at: str | None
    last_run_at: str | None
    last_status: str | None


# The single run wire model — shared by the page-data layer and the JSON API
# (routes/api.py). The CronRun dataclass in models.py is the DB-row
# representation; this is its serialized shape.
class RunSummary(BaseModel):
    id: int
    task_name: str
    started_at: str
    finished_at: str | None
    status: str
    error_message: str | None
    attempt: int
    duration_ms: int | None


def task_to_summary(task: CronTask) -> TaskSummary:
    """Build the shared task wire model from a DB row, deriving the
    human-readable schedule description (used by pages and the API)."""
    description, schedule_seconds = describe_schedule(
        task.schedule_type, task.schedule_config, task.timezone
    )
    return TaskSummary(
        name=task.name,
        handler=task.handler,
        schedule_type=task.schedule_type,
        schedule_description=description,
        schedule_seconds=schedule_seconds,
        timezone=task.timezone,
        enabled=task.enabled,
        next_run_at=task.next_run_at,
        last_run_at=task.last_run_at,
        last_status=task.last_status,
    )


class IndexPageData(BaseModel):
    tasks: list[TaskSummary]
    handlers: list[str]


class DetailPageData(BaseModel):
    task: TaskSummary
    runs: list[RunSummary]


__exports__ = [IndexPageData, DetailPageData]
