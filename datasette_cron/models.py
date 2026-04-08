from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CronTask:
    name: str
    handler: str
    config: dict
    schedule_type: str
    schedule_config: str
    timezone: str | None
    overlap_policy: str
    retry_max: int
    retry_backoff: str
    enabled: bool
    next_run_at: str | None
    last_run_at: str | None
    last_status: str | None
    created_at: str
    updated_at: str


@dataclass
class CronRun:
    id: int
    task_name: str
    started_at: str
    finished_at: str | None
    status: str
    error_message: str | None
    attempt: int
    duration_ms: int | None
