from sqlite_utils import Database
from sqlite_migrate import Migrations

internal_migrations = Migrations("datasette-cron.internal")


@internal_migrations()
def m001_initial(db: Database):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS datasette_cron_tasks (
            name TEXT PRIMARY KEY,
            handler TEXT NOT NULL,
            config TEXT NOT NULL DEFAULT '{}',
            schedule_type TEXT NOT NULL,
            schedule_config TEXT NOT NULL,
            timezone TEXT,
            overlap_policy TEXT NOT NULL DEFAULT 'skip',
            retry_max INTEGER NOT NULL DEFAULT 0,
            retry_backoff TEXT NOT NULL DEFAULT 'exponential',
            enabled INTEGER NOT NULL DEFAULT 1,
            next_run_at TEXT,
            last_run_at TEXT,
            last_status TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now'))
        );

        CREATE TABLE IF NOT EXISTS datasette_cron_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT NOT NULL REFERENCES datasette_cron_tasks(name) ON DELETE CASCADE,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            error_message TEXT,
            attempt INTEGER NOT NULL DEFAULT 1,
            duration_ms INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_datasette_cron_runs_task_started
            ON datasette_cron_runs(task_name, started_at DESC);
    """)
