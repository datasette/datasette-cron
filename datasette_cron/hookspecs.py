from pluggy import HookspecMarker

hookspec = HookspecMarker("datasette")


@hookspec
def cron_register_handlers(datasette):
    """Return a dict of {name: callable} handler functions for cron tasks.

    Each handler receives (datasette, config) where config is a dict.
    Handlers can be sync or async.
    """
