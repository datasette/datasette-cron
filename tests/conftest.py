async def start_scheduler(datasette):
    """Call after invoke_startup() to start the scheduler loop in tests.
    In production, this is triggered by the first ASGI request via asgi_wrapper."""
    await datasette.invoke_startup()
    scheduler = datasette._cron_scheduler
    scheduler.start()
    return scheduler
