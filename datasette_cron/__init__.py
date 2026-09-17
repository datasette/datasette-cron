from datasette import hookimpl
from datasette.permissions import Action
from datasette.plugins import pm
from datasette_vite import vite_entry
from sqlite_utils import Database as SqliteUtilsDatabase

import logging

from .hookspecs import cron_register_handlers as cron_register_handlers
from .internal_migrations import internal_migrations
from .router import router, ACCESS_ACTION
from .scheduler import Scheduler

logger = logging.getLogger("datasette_cron")

# Register our hookspec so other plugins can implement it
pm.add_hookspecs(__import__(__name__ + ".hookspecs", fromlist=["hookspecs"]))

# Import route modules to trigger registration on the shared router
from .routes import pages, api  # noqa: E402

_ = (pages, api)


@hookimpl
def register_routes():
    return router.routes()


@hookimpl
def extra_template_vars(datasette):
    entry = vite_entry(
        datasette=datasette,
        plugin_package="datasette_cron",
    )
    return {"datasette_cron_vite_entry": entry}


@hookimpl
def register_actions(datasette):
    return [
        Action(name=ACCESS_ACTION, description="Access datasette-cron admin"),
    ]


@hookimpl
def menu_links(datasette, actor, request):
    async def inner():
        if await datasette.allowed(action=ACCESS_ACTION, actor=actor):
            return [
                {"href": datasette.urls.path("/-/cron"), "label": "Cron Tasks"},
            ]

    return inner


@hookimpl(tryfirst=True)
def startup(datasette):
    """Set up DB schema and scheduler instance so other plugins can add_task in their startup."""

    async def inner():
        # Apply migrations
        def migrate(connection):
            db = SqliteUtilsDatabase(connection)
            internal_migrations.apply(db)

        await datasette.get_internal_database().execute_write_fn(migrate)

        # Build scheduler (but don't start the loop yet -- that happens after all startups)
        scheduler = Scheduler(datasette)
        datasette._cron_scheduler = scheduler

        # Reconcile runs orphaned by a crashed previous process. Safe here
        # because core only launches supervised background tasks (including
        # our scheduler loop, registered below) after every plugin's startup
        # hook has completed, so nothing can genuinely be running yet.
        await scheduler.internal_db.mark_orphaned_runs_abandoned()

        # Collect handlers from all plugins. We catch per-plugin exceptions
        # so one buggy plugin doesn't take down everyone else's scheduler,
        # but we log with traceback so the failure is visible.
        for plugin in pm.get_plugins():
            if not hasattr(plugin, "cron_register_handlers"):
                continue
            module = getattr(plugin, "__name__", "") or getattr(
                plugin, "__module__", ""
            )
            plugin_name = module.replace("datasette_", "").split(".")[0] or "unknown"
            try:
                result = plugin.cron_register_handlers(datasette=datasette)
                if result and isinstance(result, dict):
                    scheduler.register_handlers(plugin_name, result)
            except Exception:
                logger.exception(
                    "Plugin %r raised while registering cron handlers",
                    plugin_name,
                )

        # Register the loop as a supervised background task. Core launches
        # it only after every plugin's startup hook (this one included) has
        # finished, so any downstream plugin that calls
        # scheduler.add_task(...) from its own startup hook is guaranteed to
        # have run first -- no first-request fallback required.
        datasette.add_background_task(scheduler.run, name="datasette-cron")

    return inner


@hookimpl
def shutdown(datasette):
    async def inner():
        scheduler = getattr(datasette, "_cron_scheduler", None)
        if scheduler:
            await scheduler.shutdown()

    return inner
