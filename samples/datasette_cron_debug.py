"""
Sample plugin that uses datasette-cron to insert a row into a debug table every second.

Install: copy this file into your plugins directory, or set DATASETTE_LOAD_PLUGINS=datasette_cron_debug

Usage:
    datasette tmp.db --plugins-dir=samples/
"""

from datasette import hookimpl
from datetime import datetime


async def insert_debug_row(datasette, config):
    """Insert a timestamped row into the debug table."""
    db_name = config.get("database", "_memory")
    db = datasette.get_database(db_name)
    await db.execute_write(
        "INSERT INTO cron_debug (timestamp, message) VALUES (?, ?)",
        [datetime.now().isoformat(), "tick from datasette-cron-debug"],
    )


@hookimpl
def cron_register_handlers(datasette):
    return {
        "debug-insert": insert_debug_row,
    }


@hookimpl
def startup(datasette):
    async def inner():
        # Create the debug table in the first available mutable database
        db_name = None
        for name, db in datasette.databases.items():
            if name != "_internal" and db.is_mutable:
                db_name = name
                break

        if db_name is None:
            return

        db = datasette.get_database(db_name)
        await db.execute_write(
            """
            CREATE TABLE IF NOT EXISTS cron_debug (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
        )

        # Register a task that runs every second
        scheduler = datasette._cron_scheduler
        await scheduler.add_task(
            name="debug-insert-every-second",
            handler="debug-insert",
            schedule={"interval": 1},
            config={"database": db_name},
            overlap="skip",
        )

    return inner
