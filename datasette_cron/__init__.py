import os

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
        vite_dev_path=os.environ.get("DATASETTE_CRON_VITE_PATH"),
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

        # Collect handlers from all plugins
        # Use pluggy's caller info to determine plugin names
        hook_callers = pm.parse_hookimpl_opts
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
                pass

    return inner


@hookimpl
def asgi_wrapper(datasette):
    def wrapper(app):
        _scheduler_started = False

        async def asgi_app(scope, receive, send):
            nonlocal _scheduler_started

            if scope["type"] == "lifespan":

                async def wrapped_receive():
                    message = await receive()
                    if message["type"] == "lifespan.shutdown":
                        scheduler = getattr(datasette, "_cron_scheduler", None)
                        if scheduler:
                            try:
                                await scheduler.shutdown()
                            except Exception:
                                pass
                    return message

                await app(scope, wrapped_receive, send)
            else:
                # Start the scheduler loop on the first non-lifespan request,
                # after all startup hooks have completed.
                if not _scheduler_started:
                    _scheduler_started = True
                    scheduler = getattr(datasette, "_cron_scheduler", None)
                    if scheduler:
                        scheduler.start()

                await app(scope, receive, send)

        return asgi_app

    return wrapper
