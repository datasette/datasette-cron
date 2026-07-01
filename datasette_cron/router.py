from datasette_plugin_router import Router

router = Router()

ACCESS_ACTION = "datasette-cron-access"


async def require_permission(datasette, request):
    """Call at the top of any route handler to enforce ACCESS_ACTION."""
    from datasette.utils.asgi import Forbidden

    if not await datasette.allowed(action=ACCESS_ACTION, actor=request.actor):
        raise Forbidden("Permission denied")


class SchedulerNotAvailable(Exception):
    """Raised when datasette._cron_scheduler is missing (startup failed)."""


def get_scheduler(datasette):
    """Return the Scheduler instance, or raise a clear, actionable error.

    The scheduler is attached to the Datasette instance by the startup hook;
    if that hook failed (e.g. a migration error) every route would otherwise
    surface an opaque AttributeError.
    """
    scheduler = getattr(datasette, "_cron_scheduler", None)
    if scheduler is None:
        raise SchedulerNotAvailable(
            "datasette-cron startup did not complete -- the scheduler is not "
            "available. Check the server logs for errors during startup."
        )
    return scheduler
