from __future__ import annotations

import json

from .models import CronRun, CronTask


class InternalDB:
    def __init__(self, internal_db):
        self.db = internal_db

    async def upsert_task(
        self,
        name: str,
        handler: str,
        config: dict,
        schedule_type: str,
        schedule_config: str,
        timezone: str | None = None,
        overlap_policy: str = "skip",
        retry_max: int = 0,
        retry_backoff: str = "exponential",
        next_run_at: str | None = None,
    ) -> None:
        def write(conn):
            conn.execute(
                """
                INSERT INTO datasette_cron_tasks
                    (name, handler, config, schedule_type, schedule_config,
                     timezone, overlap_policy, retry_max, retry_backoff, next_run_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    handler = excluded.handler,
                    config = excluded.config,
                    schedule_type = excluded.schedule_type,
                    schedule_config = excluded.schedule_config,
                    timezone = excluded.timezone,
                    overlap_policy = excluded.overlap_policy,
                    retry_max = excluded.retry_max,
                    retry_backoff = excluded.retry_backoff,
                    next_run_at = COALESCE(datasette_cron_tasks.next_run_at, excluded.next_run_at),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f', 'now')
                """,
                [
                    name,
                    handler,
                    json.dumps(config),
                    schedule_type,
                    schedule_config,
                    timezone,
                    overlap_policy,
                    retry_max,
                    retry_backoff,
                    next_run_at,
                ],
            )

        await self.db.execute_write_fn(write)

    async def update_task(self, name: str, **kwargs) -> None:
        allowed = {
            "handler",
            "config",
            "schedule_type",
            "schedule_config",
            "timezone",
            "overlap_policy",
            "retry_max",
            "retry_backoff",
            "enabled",
            "next_run_at",
            "last_run_at",
            "last_status",
        }
        sets = []
        params = []
        for key, value in kwargs.items():
            if key not in allowed:
                raise ValueError(f"Cannot update field: {key}")
            if key == "config":
                value = json.dumps(value)
            sets.append(f"{key} = ?")
            params.append(value)
        sets.append("updated_at = strftime('%Y-%m-%dT%H:%M:%f', 'now')")
        params.append(name)

        def write(conn):
            conn.execute(
                f"UPDATE datasette_cron_tasks SET {', '.join(sets)} WHERE name = ?",
                params,
            )

        await self.db.execute_write_fn(write)

    async def delete_task(self, name: str) -> None:
        def write(conn):
            conn.execute("DELETE FROM datasette_cron_tasks WHERE name = ?", [name])

        await self.db.execute_write_fn(write)

    def _row_to_task(self, row) -> CronTask:
        d = dict(row)
        return CronTask(**d)

    def _row_to_run(self, row) -> CronRun:
        d = dict(row)
        return CronRun(**d)

    async def get_task(self, name: str) -> CronTask | None:
        result = await self.db.execute(
            "SELECT * FROM datasette_cron_tasks WHERE name = ?", [name]
        )
        row = result.first()
        return self._row_to_task(row) if row else None

    async def get_all_tasks(self) -> list[CronTask]:
        result = await self.db.execute(
            "SELECT * FROM datasette_cron_tasks ORDER BY name"
        )
        return [self._row_to_task(r) for r in result.rows]

    async def get_due_tasks(self) -> list[CronTask]:
        result = await self.db.execute(
            """
            SELECT * FROM datasette_cron_tasks
            WHERE enabled = 1 AND next_run_at IS NOT NULL
              AND next_run_at <= strftime('%Y-%m-%dT%H:%M:%f', 'now')
            ORDER BY next_run_at
            """,
        )
        return [self._row_to_task(r) for r in result.rows]

    async def update_next_run(
        self, name: str, next_run_at: str, last_status: str | None = None
    ) -> None:
        def write(conn):
            if last_status:
                conn.execute(
                    """UPDATE datasette_cron_tasks
                    SET next_run_at = ?, last_run_at = strftime('%Y-%m-%dT%H:%M:%f', 'now'),
                        last_status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%f', 'now')
                    WHERE name = ?""",
                    [next_run_at, last_status, name],
                )
            else:
                conn.execute(
                    """UPDATE datasette_cron_tasks
                    SET next_run_at = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%f', 'now')
                    WHERE name = ?""",
                    [next_run_at, name],
                )

        await self.db.execute_write_fn(write)

    async def record_run_start(self, task_name: str, attempt: int = 1) -> int:
        def write(conn):
            cursor = conn.execute(
                """INSERT INTO datasette_cron_runs (task_name, started_at, status, attempt)
                VALUES (?, strftime('%Y-%m-%dT%H:%M:%f', 'now'), 'running', ?)""",
                [task_name, attempt],
            )
            return cursor.lastrowid

        return await self.db.execute_write_fn(write)

    async def record_run_success(self, run_id: int, duration_ms: int) -> None:
        def write(conn):
            conn.execute(
                """UPDATE datasette_cron_runs
                SET status = 'success', finished_at = strftime('%Y-%m-%dT%H:%M:%f', 'now'),
                    duration_ms = ?
                WHERE id = ?""",
                [duration_ms, run_id],
            )

        await self.db.execute_write_fn(write)

    async def record_run_error(
        self, run_id: int, error_message: str, duration_ms: int
    ) -> None:
        def write(conn):
            conn.execute(
                """UPDATE datasette_cron_runs
                SET status = 'error', finished_at = strftime('%Y-%m-%dT%H:%M:%f', 'now'),
                    error_message = ?, duration_ms = ?
                WHERE id = ?""",
                [error_message, duration_ms, run_id],
            )

        await self.db.execute_write_fn(write)

    async def get_runs(
        self, task_name: str, limit: int = 50, offset: int = 0
    ) -> list[CronRun]:
        result = await self.db.execute(
            """SELECT * FROM datasette_cron_runs
            WHERE task_name = ?
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?""",
            [task_name, limit, offset],
        )
        return [self._row_to_run(r) for r in result.rows]

    async def get_all_runs(self, limit: int = 50, offset: int = 0) -> list[CronRun]:
        result = await self.db.execute(
            """SELECT * FROM datasette_cron_runs
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?""",
            [limit, offset],
        )
        return [self._row_to_run(r) for r in result.rows]
