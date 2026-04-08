from datasette import Response

from ..router import router, require_permission
from ..page_data import IndexPageData, DetailPageData, TaskSummary, RunSummary
from ..internal_db import InternalDB
from ..models import CronRun, CronTask
from ..schedules import schedule_from_db


def _task_to_summary(task: CronTask) -> TaskSummary:
    try:
        sched = schedule_from_db(
            task.schedule_type, task.schedule_config, task.timezone
        )
        description = sched.describe()
    except Exception:
        description = f"{task.schedule_type}: {task.schedule_config}"

    return TaskSummary(
        name=task.name,
        handler=task.handler,
        schedule_type=task.schedule_type,
        schedule_description=description,
        timezone=task.timezone,
        enabled=bool(task.enabled),
        next_run_at=task.next_run_at,
        last_run_at=task.last_run_at,
        last_status=task.last_status,
    )


def _run_to_summary(run: CronRun) -> RunSummary:
    return RunSummary(
        id=run.id,
        task_name=run.task_name,
        started_at=run.started_at,
        finished_at=run.finished_at,
        status=run.status,
        error_message=run.error_message,
        attempt=run.attempt,
        duration_ms=run.duration_ms,
    )


@router.GET(r"/-/cron$")
async def cron_index(datasette, request):
    await require_permission(datasette, request)
    db = InternalDB(datasette.get_internal_database())
    tasks = await db.get_all_tasks()
    scheduler = datasette._cron_scheduler
    handler_names = sorted(scheduler._handler_registry.keys())

    page_data = IndexPageData(
        tasks=[_task_to_summary(t) for t in tasks],
        handlers=handler_names,
    )

    return Response.html(
        await datasette.render_template(
            "cron_base.html",
            {
                "page_title": "Cron Tasks",
                "entrypoint": "src/pages/index/index.ts",
                "page_data": page_data.model_dump(),
            },
        )
    )


@router.GET(r"/-/cron/(?P<task_name>[^/]+)$")
async def cron_detail(datasette, request, task_name: str):
    await require_permission(datasette, request)
    db = InternalDB(datasette.get_internal_database())
    task = await db.get_task(task_name)
    if not task:
        return Response.text("Task not found", status=404)

    runs = await db.get_runs(task_name, limit=50)
    scheduler = datasette._cron_scheduler
    handler_names = sorted(scheduler._handler_registry.keys())

    page_data = DetailPageData(
        task=_task_to_summary(task),
        runs=[_run_to_summary(r) for r in runs],
        handlers=handler_names,
    )

    return Response.html(
        await datasette.render_template(
            "cron_base.html",
            {
                "page_title": f"Task: {task_name}",
                "entrypoint": "src/pages/detail/index.ts",
                "page_data": page_data.model_dump(),
            },
        )
    )
