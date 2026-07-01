from dataclasses import asdict

from datasette import Response

from ..router import router, require_permission, get_scheduler
from ..page_data import IndexPageData, DetailPageData, RunSummary, task_to_summary
from ..internal_db import InternalDB


@router.GET(r"/-/cron$")
async def cron_index(datasette, request):
    await require_permission(datasette, request)
    db = InternalDB(datasette.get_internal_database())
    tasks = await db.get_all_tasks()
    scheduler = get_scheduler(datasette)
    handler_names = scheduler.list_handlers()

    page_data = IndexPageData(
        tasks=[task_to_summary(t) for t in tasks],
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

    page_data = DetailPageData(
        task=task_to_summary(task),
        runs=[RunSummary(**asdict(r)) for r in runs],
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
