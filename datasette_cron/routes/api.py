from typing import Annotated

from datasette import Response
from datasette_plugin_router import Body
from pydantic import BaseModel

from ..router import router, require_permission
from ..internal_db import InternalDB
from ..schedules import schedule_from_db


# --- Response Models ---

class TaskResponse(BaseModel):
    name: str
    handler: str
    config: dict
    schedule_type: str
    schedule_config: str
    schedule_description: str
    timezone: str | None
    overlap_policy: str
    retry_max: int
    retry_backoff: str
    enabled: bool
    next_run_at: str | None
    last_run_at: str | None
    last_status: str | None


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]


class RunResponse(BaseModel):
    id: int
    task_name: str
    started_at: str
    finished_at: str | None
    status: str
    error_message: str | None
    attempt: int
    duration_ms: int | None


class RunListResponse(BaseModel):
    runs: list[RunResponse]


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

def _task_to_response(task: dict) -> dict:
    try:
        sched = schedule_from_db(task["schedule_type"], task["schedule_config"], task["timezone"])
        description = sched.describe()
    except Exception:
        description = f"{task['schedule_type']}: {task['schedule_config']}"

    import json
    config = task["config"]
    if isinstance(config, str):
        config = json.loads(config)

    return {
        "name": task["name"],
        "handler": task["handler"],
        "config": config,
        "schedule_type": task["schedule_type"],
        "schedule_config": task["schedule_config"],
        "schedule_description": description,
        "timezone": task["timezone"],
        "overlap_policy": task["overlap_policy"],
        "retry_max": task["retry_max"],
        "retry_backoff": task["retry_backoff"],
        "enabled": bool(task["enabled"]),
        "next_run_at": task["next_run_at"],
        "last_run_at": task["last_run_at"],
        "last_status": task["last_status"],
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
    return Response.json({"runs": [dict(r) for r in runs]})


@router.POST(r"/-/api/cron/tasks/(?P<task_name>[^/]+)/trigger$", output=TriggerResponse)
async def api_trigger_task(
    datasette, request, task_name: str,
    body: Annotated[TriggerRequest, Body()],
):
    await require_permission(datasette, request)
    scheduler = datasette._cron_scheduler
    try:
        await scheduler.trigger_task(task_name)
        return Response.json({"ok": True, "message": f"Task {task_name} triggered"})
    except ValueError as e:
        return Response.json({"ok": False, "message": str(e)}, status=404)


@router.POST(r"/-/api/cron/tasks/(?P<task_name>[^/]+)/enable$", output=EnableResponse)
async def api_enable_task(
    datasette, request, task_name: str,
    body: Annotated[EnableRequest, Body()],
):
    await require_permission(datasette, request)
    scheduler = datasette._cron_scheduler
    if body.enabled:
        await scheduler.enable_task(task_name)
    else:
        await scheduler.disable_task(task_name)
    return Response.json({"ok": True, "enabled": body.enabled})
