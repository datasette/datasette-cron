from __future__ import annotations

from pydantic import BaseModel


class TaskSummary(BaseModel):
    name: str
    handler: str
    schedule_type: str
    schedule_description: str
    timezone: str | None
    enabled: bool
    next_run_at: str | None
    last_run_at: str | None
    last_status: str | None


class RunSummary(BaseModel):
    id: int
    task_name: str
    started_at: str
    finished_at: str | None
    status: str
    error_message: str | None
    attempt: int
    duration_ms: int | None


class IndexPageData(BaseModel):
    tasks: list[TaskSummary]
    handlers: list[str]


class DetailPageData(BaseModel):
    task: TaskSummary
    runs: list[RunSummary]
    handlers: list[str]


__exports__ = [IndexPageData, DetailPageData]
