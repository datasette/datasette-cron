"""Throwaway seed plugin for the doc-screenshot harness (NOT shipped).

Loaded via ``datasette --plugins-dir`` from frontend/scripts/screenshots.mjs.
There is no HTTP task-create API — tasks are registered by plugins — so the
harness seeds through the real plugin surface instead:

1. ``cron_register_handlers`` — four demo handlers. The module filename
   (``datasette_demo.py``) determines the registry prefix: datasette-cron
   strips ``datasette_`` from the module name, so these register as
   ``demo:refresh-feeds`` etc. (see plugin_name in datasette_cron/__init__.py).

2. ``startup`` — creates four demo tasks through ``scheduler.add_task()``
   (interval, cron-with-timezone, hourly-with-retries, disabled) and inserts
   a fixed run history directly into ``datasette_cron_runs``.

Determinism: run timestamps are coarse offsets from "now" (-35m / -2h / -1d…)
chosen mid-bucket so relative times render identically across runs; the
harness's freezeVolatile pins them as a boundary guard. Every schedule's next
fire is >= 5 minutes out, so nothing executes while shots are captured and
the history stays exactly as seeded. Seeding is gated on existing demo runs
so a re-run against the same internal DB does not duplicate rows.
"""

import asyncio
from datetime import datetime, timedelta, timezone

from datasette import hookimpl


# ---------------------------------------------------------------------------
# Demo handlers. Registered for real (they show up in the "Registered
# handlers" chip list) but never actually executed during a screenshot run.


async def refresh_feeds(datasette, config):
    await asyncio.sleep(0.05)


async def nightly_report(datasette, config):
    pass


async def flaky_import(datasette, config):
    raise ConnectionError("feed host unreachable")


async def weekly_digest(datasette, config):
    pass


@hookimpl
def cron_register_handlers(datasette):
    return {
        "refresh-feeds": refresh_feeds,
        "nightly-report": nightly_report,
        "flaky-import": flaky_import,
        "weekly-digest": weekly_digest,
    }


# ---------------------------------------------------------------------------
# Seeded tasks + run history.

REFRESH = "demo:refresh-feeds"
NIGHTLY = "demo:nightly-report"
FLAKY = "demo:flaky-import"
WEEKLY = "demo:weekly-digest"


def _ts(now, **offset):
    """`now` minus an offset, formatted like SQLite's %Y-%m-%dT%H:%M:%f."""
    return (now - timedelta(**offset)).isoformat(timespec="milliseconds")


def _run(now, task, offset, duration_ms, status, error=None, attempt=1):
    """Build one datasette_cron_runs row as an insert tuple.

    (task_name, started_at, finished_at, status, error_message, attempt,
    duration_ms). An abandoned run has no duration; its finished_at is the
    reconciling startup an hour later.
    """
    started = now - timedelta(**offset)
    if duration_ms is None:
        finished = started + timedelta(hours=1)
    else:
        finished = started + timedelta(milliseconds=duration_ms)
    return (
        task,
        started.isoformat(timespec="milliseconds"),
        finished.isoformat(timespec="milliseconds"),
        status,
        error,
        attempt,
        duration_ms,
    )


def _runs(now):
    """The fixed run history.

    Offsets sit mid-bucket (never a few seconds shy of a unit boundary) so
    the rendered relative times are stable between seed and capture. Retry
    attempts are seconds apart, with attempt 1 the oldest.
    """
    timeout = "TimeoutError: import source timed out after 30s"
    conn_err = "ConnectionError: feed host unreachable"
    return [
        # demo:refresh-feeds — healthy 5-minute interval task.
        _run(now, REFRESH, dict(minutes=40), 203, "success"),
        _run(now, REFRESH, dict(minutes=35), 184, "success"),
        # demo:nightly-report — daily cron, succeeded the last two mornings.
        _run(now, NIGHTLY, dict(hours=49), 2987, "success"),
        _run(now, NIGHTLY, dict(hours=25), 3241, "success"),
        # demo:flaky-import — the interesting one: an abandoned run (crashed
        # process), a timeout recovered on retry, a clean run, and a fresh
        # failure that exhausted all three attempts (retry_max=2).
        _run(now, FLAKY, dict(hours=26), None, "abandoned"),
        _run(now, FLAKY, dict(hours=8, seconds=34), 1876, "error", timeout, 1),
        _run(now, FLAKY, dict(hours=8), 1543, "success", None, 2),
        _run(now, FLAKY, dict(hours=5), 1412, "success"),
        _run(now, FLAKY, dict(hours=2, seconds=16), 2148, "error", conn_err, 1),
        _run(now, FLAKY, dict(hours=2, seconds=8), 2033, "error", conn_err, 2),
        _run(now, FLAKY, dict(hours=2), 1967, "error", conn_err, 3),
        # demo:weekly-digest — disabled; one old success.
        _run(now, WEEKLY, dict(days=3), 45210, "success"),
    ]


@hookimpl
def startup(datasette):
    async def inner():
        # datasette-cron's startup hook is tryfirst, so the scheduler and the
        # migrated schema both exist by the time this runs.
        scheduler = datasette._cron_scheduler

        await scheduler.add_task(
            name=REFRESH,
            handler="demo:refresh-feeds",
            schedule={"interval": 300},
        )
        await scheduler.add_task(
            name=NIGHTLY,
            handler="demo:nightly-report",
            schedule="0 8 * * *",
            timezone="America/New_York",
        )
        await scheduler.add_task(
            name=FLAKY,
            handler="demo:flaky-import",
            schedule={"interval": 3600},
            retry={"max_retries": 2},
        )
        await scheduler.add_task(
            name=WEEKLY,
            handler="demo:weekly-digest",
            schedule="0 6 * * 1",
        )
        await scheduler.set_enabled(WEEKLY, False)

        internal = datasette.get_internal_database()

        # Gate: don't duplicate history when re-run against the same DB.
        existing = await internal.execute(
            "SELECT count(*) FROM datasette_cron_runs WHERE task_name LIKE 'demo:%'"
        )
        if existing.first()[0]:
            return

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = _runs(now)

        def write(conn):
            conn.executemany(
                """
                INSERT INTO datasette_cron_runs
                    (task_name, started_at, finished_at, status,
                     error_message, attempt, duration_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        await internal.execute_write_fn(write)

        # Make the task rows' last-run summaries match the seeded history.
        db = scheduler.internal_db
        await db.update_task(
            REFRESH, last_run_at=_ts(now, minutes=35), last_status="success"
        )
        await db.update_task(
            NIGHTLY, last_run_at=_ts(now, hours=25), last_status="success"
        )
        await db.update_task(FLAKY, last_run_at=_ts(now, hours=2), last_status="error")
        await db.update_task(
            WEEKLY, last_run_at=_ts(now, days=3), last_status="success"
        )

    return inner
