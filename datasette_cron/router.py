from datasette_plugin_router import Router

router = Router()

ACCESS_ACTION = "datasette-cron-access"


async def require_permission(datasette, request):
    """Call at the top of any route handler to enforce ACCESS_ACTION."""
    from datasette.utils.asgi import Forbidden

    if not await datasette.allowed(action=ACCESS_ACTION, actor=request.actor):
        raise Forbidden("Permission denied")
