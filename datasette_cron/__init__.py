import logging

from datasette import hookimpl
from datasette.permissions import Action
from datasette.plugins import pm
from datasette_vite import vite_entry
from opentelemetry.trace import Status, StatusCode
from sqlite_utils import Database as SqliteUtilsDatabase

from .hookspecs import cron_register_handlers as cron_register_handlers
from .internal_migrations import internal_migrations
from .router import ACCESS_ACTION, router
from .scheduler import Scheduler
from . import telemetry
from .telemetry import tracer
from .telemetry_registry import ERROR_TYPE, HANDLERS, PLUGIN, REGISTER_HANDLERS

logger = logging.getLogger("datasette_cron")

# Register our hookspec so other plugins can implement it
pm.add_hookspecs(__import__(__name__ + ".hookspecs", fromlist=["hookspecs"]))

# Import route modules to trigger registration on the shared router
from .routes import api, pages  # noqa: E402

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
        telemetry.register_scheduler(scheduler)

        # Optional trace_url plugin config: a template that turns a run's
        # stored trace id into a tracing-UI link on the detail page, e.g.
        # "http://localhost:16686/trace/{trace_id}". Validated once here.
        config = datasette.plugin_config("datasette-cron") or {}
        trace_url = config.get("trace_url")
        if trace_url and "{trace_id}" not in trace_url:
            logger.warning(
                "datasette-cron trace_url %r has no {trace_id} placeholder, ignoring",
                trace_url,
            )
            trace_url = None
        datasette._cron_trace_url = trace_url

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
            # Child of whatever is current (core's datasette.startup span),
            # not a root: a red span in the startup trace is the signal the
            # swallowed-and-logged exception below is not.
            with tracer.start_as_current_span(REGISTER_HANDLERS) as span:
                span.set_attribute(PLUGIN, plugin_name)
                try:
                    result = plugin.cron_register_handlers(datasette=datasette)
                    handlers = result if result and isinstance(result, dict) else {}
                    span.set_attribute(HANDLERS, len(handlers))
                    if handlers:
                        scheduler.register_handlers(plugin_name, handlers)
                except Exception as e:
                    span.set_attribute(ERROR_TYPE, type(e).__name__)
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
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
