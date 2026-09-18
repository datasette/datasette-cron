from __future__ import annotations

import abc
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from croniter import croniter


class Schedule(abc.ABC):
    @abc.abstractmethod
    def next_run(self, after: datetime) -> datetime:
        """Return the next run time (naive UTC) after the given datetime.

        `after` is a *naive UTC* datetime. Implementations that convert to
        another timezone must attach `timezone.utc` first
        (`after.replace(tzinfo=timezone.utc)`) — calling `.astimezone()` on
        the naive value would misinterpret it as server-local time.
        """

    @abc.abstractmethod
    def describe(self) -> str:
        """Human-readable description of the schedule."""

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """Serialize to a dict for DB storage."""

    @property
    @abc.abstractmethod
    def schedule_type(self) -> str:
        """Return 'cron' or 'interval'."""


class CronSchedule(Schedule):
    def __init__(self, expression: str, tz: ZoneInfo | None = None):
        self.expression = expression
        self.tz = tz
        # Validate
        croniter(expression)

    @property
    def schedule_type(self) -> str:
        return "cron"

    def next_run(self, after: datetime) -> datetime:
        if self.tz:
            local_after = after.replace(tzinfo=timezone.utc).astimezone(self.tz)
            cron = croniter(self.expression, local_after)
            local_next = cron.get_next(datetime)
            return local_next.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            cron = croniter(self.expression, after)
            return cron.get_next(datetime)

    def describe(self) -> str:
        tz_str = f" ({self.tz})" if self.tz else ""
        return f"cron: {self.expression}{tz_str}"

    def to_dict(self) -> dict:
        return {"expression": self.expression}


class IntervalSchedule(Schedule):
    def __init__(self, seconds: float):
        if seconds <= 0:
            raise ValueError("Interval must be positive")
        self.seconds = seconds

    @property
    def schedule_type(self) -> str:
        return "interval"

    def next_run(self, after: datetime) -> datetime:
        return after + timedelta(seconds=self.seconds)

    def describe(self) -> str:
        s = self.seconds
        if s < 60:
            return f"every {s:.0f}s"
        elif s < 3600:
            return f"every {s / 60:.0f}m"
        elif s < 86400:
            h = int(s // 3600)
            m = int((s % 3600) // 60)
            return f"every {h}h {m}m" if m else f"every {h}h"
        else:
            d = int(s // 86400)
            h = int((s % 86400) // 3600)
            return f"every {d}d {h}h" if h else f"every {d}d"

    def to_dict(self) -> dict:
        return {"seconds": self.seconds}


def parse_schedule(schedule, tz_str: str | None = None) -> Schedule:
    """Parse a schedule definition into a Schedule object.

    Args:
        schedule: One of:
            - A cron string like "0 8 * * *"
            - A dict with "interval" key (seconds)
        tz_str: Optional IANA timezone string
    """
    tz = ZoneInfo(tz_str) if tz_str else None

    if isinstance(schedule, str):
        return CronSchedule(schedule, tz=tz)
    elif isinstance(schedule, dict):
        if "interval" in schedule:
            return IntervalSchedule(schedule["interval"])
    raise ValueError(f"Cannot parse schedule: {schedule!r}")


def schedule_from_db(
    schedule_type: str, schedule_config: str, tz_str: str | None = None
) -> Schedule:
    """Reconstruct a Schedule from DB columns."""
    config = json.loads(schedule_config)
    tz = ZoneInfo(tz_str) if tz_str else None

    if schedule_type == "cron":
        return CronSchedule(config["expression"], tz=tz)
    elif schedule_type == "interval":
        return IntervalSchedule(config["seconds"])
    else:
        raise ValueError(f"Unknown schedule type: {schedule_type}")


def describe_schedule(
    schedule_type: str, schedule_config: str, tz_str: str | None = None
) -> tuple[str, float | None]:
    """Derive (human description, interval seconds or None) from a task's
    stored schedule columns.

    `seconds` is only set for interval schedules — the frontend uses it to
    classify "continuous" tasks. Falls back to a raw "type: config" string
    when the stored schedule cannot be parsed.
    """
    try:
        sched = schedule_from_db(schedule_type, schedule_config, tz_str)
        seconds = sched.seconds if isinstance(sched, IntervalSchedule) else None
        return sched.describe(), seconds
    except Exception:
        return f"{schedule_type}: {schedule_config}", None
