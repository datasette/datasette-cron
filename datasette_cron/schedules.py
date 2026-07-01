from __future__ import annotations

import abc
import json
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from croniter import croniter
from dateutil.rrule import rrulestr


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
        """Return 'cron', 'interval', or 'rrule'."""


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
    def __init__(self, seconds: float, anchor: datetime | None = None):
        if seconds <= 0:
            raise ValueError("Interval must be positive")
        self.seconds = seconds
        self.anchor = anchor

    @property
    def schedule_type(self) -> str:
        return "interval"

    def next_run(self, after: datetime) -> datetime:
        if self.anchor:
            # Compute next aligned run from anchor
            elapsed = (after - self.anchor).total_seconds()
            periods = int(elapsed / self.seconds) + 1
            return self.anchor + timedelta(seconds=self.seconds * periods)
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
        d: dict = {"seconds": self.seconds}
        if self.anchor:
            d["anchor"] = self.anchor.isoformat()
        return d


class RRuleSchedule(Schedule):
    def __init__(self, rrule_str: str, tz: ZoneInfo | None = None):
        self.rrule_str = rrule_str
        self.tz = tz
        # Parse once and keep the rule. An embedded DTSTART is authoritative:
        # reusing the parsed rule preserves COUNT/UNTIL semantics instead of
        # re-anchoring the rule at "now" on every next_run call.
        self._rule = rrulestr(rrule_str)  # also validates
        self._has_dtstart = "DTSTART" in rrule_str.upper()
        if not self._has_dtstart and self._is_bounded(self._rule):
            # Without an anchor, a COUNT/UNTIL rule would be re-anchored at
            # "now" on every scheduler tick — COUNT would never exhaust and
            # UNTIL would drift. Fail at construction with a clear message
            # rather than silently mis-scheduling.
            raise ValueError(
                "rrule with COUNT or UNTIL requires an explicit DTSTART "
                "(otherwise the rule would be re-anchored at every check)"
            )

    @staticmethod
    def _is_bounded(rule) -> bool:
        # rrulestr returns either an rrule or an rruleset wrapping rrules.
        inner = getattr(rule, "_rrule", None)
        rules = list(inner) if inner is not None else [rule]
        return any(
            getattr(r, "_count", None) is not None
            or getattr(r, "_until", None) is not None
            for r in rules
        )

    @staticmethod
    def _rule_dtstart(rule) -> datetime | None:
        dt = getattr(rule, "_dtstart", None)
        if dt is None:
            for r in getattr(rule, "_rrule", None) or []:
                dt = getattr(r, "_dtstart", None)
                if dt is not None:
                    break
        return dt

    @property
    def schedule_type(self) -> str:
        return "rrule"

    def next_run(self, after: datetime) -> datetime:
        if self._has_dtstart:
            return self._next_run_anchored(after)

        # No DTSTART: relative-to-now scheduling. Re-parse with the current
        # time as the anchor (dateutil would otherwise anchor at parse time,
        # i.e. process start). Only valid for unbounded rules — bounded ones
        # are rejected in __init__.
        if self.tz:
            local_after = after.replace(tzinfo=timezone.utc).astimezone(self.tz)
        else:
            local_after = after

        rule = rrulestr(self.rrule_str, dtstart=local_after)
        next_dt = rule.after(local_after)
        if next_dt is None:
            # Fallback: far future
            return after + timedelta(days=365)

        if self.tz:
            return next_dt.astimezone(timezone.utc).replace(tzinfo=None)
        return next_dt.replace(tzinfo=None) if next_dt.tzinfo else next_dt

    def _next_run_anchored(self, after: datetime) -> datetime:
        """next_run for rules with an embedded DTSTART, using the stored rule.

        Compares in the same naive/aware space as the rule's occurrences so
        dateutil never mixes naive and aware datetimes.
        """
        anchor = self._rule_dtstart(self._rule)
        if anchor is not None and anchor.tzinfo is not None:
            # Aware DTSTART (e.g. DTSTART;TZID=...): compare in aware UTC.
            next_dt = self._rule.after(after.replace(tzinfo=timezone.utc))
            if next_dt is None:
                return after + timedelta(days=365)
            return next_dt.astimezone(timezone.utc).replace(tzinfo=None)

        # Naive DTSTART: occurrences are naive, interpreted in the schedule's
        # timezone (or UTC when no timezone is configured).
        if self.tz:
            local_after = (
                after.replace(tzinfo=timezone.utc)
                .astimezone(self.tz)
                .replace(tzinfo=None)
            )
            next_dt = self._rule.after(local_after)
            if next_dt is None:
                return after + timedelta(days=365)
            return (
                next_dt.replace(tzinfo=self.tz)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )

        next_dt = self._rule.after(after)
        if next_dt is None:
            return after + timedelta(days=365)
        return next_dt

    def describe(self) -> str:
        tz_str = f" ({self.tz})" if self.tz else ""
        return f"rrule: {self.rrule_str}{tz_str}"

    def to_dict(self) -> dict:
        return {"rrule": self.rrule_str}


def parse_schedule(schedule, tz_str: str | None = None) -> Schedule:
    """Parse a schedule definition into a Schedule object.

    Args:
        schedule: One of:
            - A cron string like "0 8 * * *"
            - A dict with "interval" key (seconds)
            - A dict with "rrule" key (RFC 5545 string)
        tz_str: Optional IANA timezone string
    """
    tz = ZoneInfo(tz_str) if tz_str else None

    if isinstance(schedule, str):
        return CronSchedule(schedule, tz=tz)
    elif isinstance(schedule, dict):
        if "interval" in schedule:
            anchor = None
            if "anchor" in schedule:
                anchor = datetime.fromisoformat(schedule["anchor"])
            return IntervalSchedule(schedule["interval"], anchor=anchor)
        elif "rrule" in schedule:
            return RRuleSchedule(schedule["rrule"], tz=tz)
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
        anchor = None
        if "anchor" in config:
            anchor = datetime.fromisoformat(config["anchor"])
        return IntervalSchedule(config["seconds"], anchor=anchor)
    elif schedule_type == "rrule":
        return RRuleSchedule(config["rrule"], tz=tz)
    else:
        raise ValueError(f"Unknown schedule type: {schedule_type}")


def add_jitter(next_run: datetime, schedule: Schedule) -> datetime:
    """Add jitter to prevent thundering herd."""
    if isinstance(schedule, IntervalSchedule):
        max_jitter = min(schedule.seconds * 0.1, 30.0)
    else:
        max_jitter = 5.0  # Small jitter for cron/rrule
    jitter = random.uniform(0, max_jitter)
    return next_run + timedelta(seconds=jitter)
