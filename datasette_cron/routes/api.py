from typing import Annotated

from dataclasses import asdict

from datasette import Response
from datasette_plugin_router import Body
from pydantic import BaseModel

from ..router import router, require_permission, get_scheduler
from ..internal_db import InternalDB
from ..models import CronTask
from ..page_data import RunSummary, TaskSummary
from ..schedules import IntervalSchedule, schedule_from_db


# --- Response Models ---


# Superset of the page-data TaskSummary: the API additionally exposes the
# task's raw config and retry/overlap settings.
class TaskResponse(TaskSummary):
    config: dict
    schedule_config: str
    overlap_policy: str
    retry_max: int
    retry_backoff: str


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]


class RunListResponse(BaseModel):
    # The API and page-data layers share one run wire model.
    runs: list[RunSummary]


class TriggerResponse(BaseModel):
    ok: bool
    message: str


class EnableResponse(BaseModel):
    ok: bool
    enabled: bool


# --- Request Models ---


class TriggerRequest(BaseModel):
    pass


class EnableRequest(BaseModel):
    enabled: bool


# --- Helpers ---


def _task_to_response(task: CronTask) -> dict:
    try:
        sched = schedule_from_db(
            task.schedule_type, task.schedule_config, task.timezone
        )
        description = sched.describe()
        schedule_seconds = (
            sched.seconds if isinstance(sched, IntervalSchedule) else None
        )
    except Exception:
        description = f"{task.schedule_type}: {task.schedule_config}"
        schedule_seconds = None

    import json

    config = task.config
    if isinstance(config, str):
        config = json.loads(config)

    return {
        "name": task.name,
        "handler": task.handler,
        "config": config,
        "schedule_type": task.schedule_type,
        "schedule_config": task.schedule_config,
        "schedule_description": description,
        "schedule_seconds": schedule_seconds,
        "timezone": task.timezone,
        "overlap_policy": task.overlap_policy,
        "retry_max": task.retry_max,
        "retry_backoff": task.retry_backoff,
        "enabled": task.enabled,
        "next_run_at": task.next_run_at,
        "last_run_at": task.last_run_at,
        "last_status": task.last_status,
    }


# --- Routes ---


@router.GET(r"/-/api/cron/tasks$", output=TaskListResponse)
async def api_tasks(datasette, request):
    await require_permission(datasette, request)
    db = InternalDB(datasette.get_internal_database())
    tasks = await db.get_all_tasks()
    return Response.json({"tasks": [_task_to_response(t) for t in tasks]})


@router.GET(r"/-/api/cron/tasks/(?P<task_name>[^/]+)$", output=TaskResponse)
async def api_task(datasette, request, task_name: str):
    await require_permission(datasette, request)
    db = InternalDB(datasette.get_internal_database())
    task = await db.get_task(task_name)
    if not task:
        return Response.json({"error": "not found"}, status=404)
    return Response.json(_task_to_response(task))


@router.GET(r"/-/api/cron/tasks/(?P<task_name>[^/]+)/runs$", output=RunListResponse)
async def api_task_runs(datasette, request, task_name: str):
    await require_permission(datasette, request)
    db = InternalDB(datasette.get_internal_database())
    runs = await db.get_runs(task_name)
    return Response.json({"runs": [RunSummary(**asdict(r)).model_dump() for r in runs]})


@router.POST(r"/-/api/cron/tasks/(?P<task_name>[^/]+)/trigger$", output=TriggerResponse)
async def api_trigger_task(
    datasette,
    request,
    task_name: str,
    body: Annotated[TriggerRequest, Body()],
):
    await require_permission(datasette, request)
    scheduler = get_scheduler(datasette)
    try:
        await scheduler.trigger_task(task_name)
        return Response.json({"ok": True, "message": f"Task {task_name} triggered"})
    except ValueError as e:
        return Response.json({"ok": False, "message": str(e)}, status=404)


@router.POST(r"/-/api/cron/tasks/(?P<task_name>[^/]+)/enable$", output=EnableResponse)
async def api_enable_task(
    datasette,
    request,
    task_name: str,
    body: Annotated[EnableRequest, Body()],
):
    await require_permission(datasette, request)
    scheduler = get_scheduler(datasette)
    if body.enabled:
        await scheduler.enable_task(task_name)
    else:
        await scheduler.disable_task(task_name)
    return Response.json({"ok": True, "enabled": body.enabled})
