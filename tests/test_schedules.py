import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from datasette_cron.schedules import (
    CronSchedule,
    IntervalSchedule,
    RRuleSchedule,
    parse_schedule,
    schedule_from_db,
    add_jitter,
)


class TestCronSchedule:
    def test_next_run_daily(self):
        sched = CronSchedule("0 8 * * *")
        after = datetime(2026, 3, 30, 7, 0, 0)
        next_run = sched.next_run(after)
        assert next_run == datetime(2026, 3, 30, 8, 0, 0)

    def test_next_run_already_past(self):
        sched = CronSchedule("0 8 * * *")
        after = datetime(2026, 3, 30, 9, 0, 0)
        next_run = sched.next_run(after)
        assert next_run == datetime(2026, 3, 31, 8, 0, 0)

    def test_next_run_with_timezone(self):
        tz = ZoneInfo("America/Los_Angeles")
        sched = CronSchedule("0 8 * * *", tz=tz)
        # 3pm UTC = 8am PST, so after 3:01pm UTC the next is tomorrow
        after = datetime(2026, 3, 30, 15, 1, 0)
        next_run = sched.next_run(after)
        # Next 8am PST = next day 3pm UTC (PDT in March)
        assert next_run.hour == 15
        assert next_run.day == 31

    def test_describe(self):
        sched = CronSchedule("0 8 * * *")
        assert "0 8 * * *" in sched.describe()

    def test_to_dict(self):
        sched = CronSchedule("*/5 * * * *")
        assert sched.to_dict() == {"expression": "*/5 * * * *"}

    def test_invalid_expression(self):
        with pytest.raises(Exception):
            CronSchedule("not a cron")


class TestIntervalSchedule:
    def test_next_run(self):
        sched = IntervalSchedule(60)
        after = datetime(2026, 3, 30, 12, 0, 0)
        next_run = sched.next_run(after)
        assert next_run == datetime(2026, 3, 30, 12, 1, 0)

    def test_next_run_with_anchor(self):
        anchor = datetime(2026, 1, 1, 0, 0, 0)
        sched = IntervalSchedule(3600, anchor=anchor)
        after = datetime(2026, 1, 1, 2, 30, 0)
        next_run = sched.next_run(after)
        assert next_run == datetime(2026, 1, 1, 3, 0, 0)

    def test_negative_interval_raises(self):
        with pytest.raises(ValueError):
            IntervalSchedule(-1)

    def test_describe_seconds(self):
        assert "30s" in IntervalSchedule(30).describe()

    def test_describe_minutes(self):
        assert "5m" in IntervalSchedule(300).describe()

    def test_describe_hours(self):
        assert "2h" in IntervalSchedule(7200).describe()

    def test_to_dict(self):
        sched = IntervalSchedule(120)
        assert sched.to_dict() == {"seconds": 120}


class TestRRuleSchedule:
    def test_next_run_weekly(self):
        sched = RRuleSchedule("FREQ=WEEKLY;BYDAY=MO")
        # Tuesday
        after = datetime(2026, 3, 31, 10, 0, 0)
        next_run = sched.next_run(after)
        # Next Monday
        assert next_run.weekday() == 0  # Monday
        assert next_run > after

    def test_describe(self):
        sched = RRuleSchedule("FREQ=DAILY")
        assert "FREQ=DAILY" in sched.describe()

    def test_to_dict(self):
        sched = RRuleSchedule("FREQ=DAILY")
        assert sched.to_dict() == {"rrule": "FREQ=DAILY"}

    def test_embedded_dtstart_is_authoritative(self):
        sched = RRuleSchedule("DTSTART:20260101T080000\nRRULE:FREQ=DAILY")
        # Before the anchor: first occurrence is DTSTART itself
        assert sched.next_run(datetime(2025, 12, 1)) == datetime(2026, 1, 1, 8, 0)
        # After the anchor: occurrences stay phased on the 8:00 anchor
        assert sched.next_run(datetime(2026, 1, 5, 12, 0)) == datetime(2026, 1, 6, 8, 0)
        # Deterministic: same `after` gives the same answer (no re-anchoring)
        assert sched.next_run(datetime(2026, 1, 5, 12, 0)) == sched.next_run(
            datetime(2026, 1, 5, 12, 0)
        )

    def test_bounded_count_with_dtstart_exhausts(self):
        sched = RRuleSchedule("DTSTART:20260101T080000\nRRULE:FREQ=DAILY;COUNT=2")
        first = sched.next_run(datetime(2025, 12, 31))
        assert first == datetime(2026, 1, 1, 8, 0)
        second = sched.next_run(first)
        assert second == datetime(2026, 1, 2, 8, 0)
        # Exhausted: falls back to far-future instead of repeating forever
        after_last = sched.next_run(second)
        assert after_last == second + timedelta(days=365)

    def test_bounded_rule_without_dtstart_rejected(self):
        with pytest.raises(ValueError, match="DTSTART"):
            RRuleSchedule("FREQ=DAILY;COUNT=3")
        with pytest.raises(ValueError, match="DTSTART"):
            RRuleSchedule("FREQ=DAILY;UNTIL=20270101T000000")

    def test_embedded_naive_dtstart_with_tz(self):
        # Naive DTSTART is interpreted in the schedule's timezone
        sched = RRuleSchedule(
            "DTSTART:20260101T080000\nRRULE:FREQ=DAILY",
            tz=ZoneInfo("America/New_York"),
        )
        # 2026-01-05 12:00 UTC is 07:00 EST; next 8am ET is 13:00 UTC same day
        next_run = sched.next_run(datetime(2026, 1, 5, 12, 0))
        assert next_run.tzinfo is None
        assert next_run == datetime(2026, 1, 5, 13, 0)

    def test_embedded_aware_dtstart_returns_naive_utc(self):
        # Aware DTSTART (TZID) with no schedule tz configured
        sched = RRuleSchedule(
            "DTSTART;TZID=America/New_York:20260101T080000\nRRULE:FREQ=DAILY"
        )
        # Next 8am ET after 2026-01-05 00:00 UTC is 2026-01-05 13:00 UTC
        next_run = sched.next_run(datetime(2026, 1, 5, 0, 0))
        assert next_run.tzinfo is None
        assert next_run == datetime(2026, 1, 5, 13, 0)

    def test_unbounded_no_dtstart_stays_relative(self):
        # Regression guard for the no-DTSTART path: still relative to `after`
        sched = RRuleSchedule("FREQ=WEEKLY;BYDAY=MO")
        after = datetime(2026, 3, 31, 10, 0, 0)  # Tuesday
        next_run = sched.next_run(after)
        assert next_run == datetime(2026, 4, 6, 10, 0, 0)  # next Monday


class TestParseSchedule:
    def test_parse_cron_string(self):
        sched = parse_schedule("0 8 * * *")
        assert isinstance(sched, CronSchedule)

    def test_parse_interval_dict(self):
        sched = parse_schedule({"interval": 60})
        assert isinstance(sched, IntervalSchedule)
        assert sched.seconds == 60

    def test_parse_rrule_dict(self):
        sched = parse_schedule({"rrule": "FREQ=DAILY"})
        assert isinstance(sched, RRuleSchedule)

    def test_parse_with_timezone(self):
        sched = parse_schedule("0 8 * * *", tz_str="America/New_York")
        assert isinstance(sched, CronSchedule)
        assert sched.tz == ZoneInfo("America/New_York")

    def test_parse_invalid(self):
        with pytest.raises(ValueError):
            parse_schedule(12345)


class TestScheduleFromDb:
    def test_cron_from_db(self):
        sched = schedule_from_db("cron", '{"expression": "0 8 * * *"}')
        assert isinstance(sched, CronSchedule)
        assert sched.expression == "0 8 * * *"

    def test_interval_from_db(self):
        sched = schedule_from_db("interval", '{"seconds": 300}')
        assert isinstance(sched, IntervalSchedule)
        assert sched.seconds == 300

    def test_rrule_from_db(self):
        sched = schedule_from_db("rrule", '{"rrule": "FREQ=WEEKLY"}')
        assert isinstance(sched, RRuleSchedule)

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError):
            schedule_from_db("unknown", "{}")


@contextmanager
def _forced_process_tz(tz_name):
    """Temporarily force the process-local timezone (POSIX only)."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = tz_name
    time.tzset()
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old
        time.tzset()


@pytest.mark.skipif(sys.platform == "win32", reason="time.tzset() is POSIX-only")
class TestNaiveUtcContract:
    """next_run receives naive UTC; tz-aware schedules must not misread it as
    server-local time (regression: naive .astimezone() uses the process TZ)."""

    def test_cron_tz_schedule_ignores_process_tz(self):
        with _forced_process_tz("Asia/Tokyo"):
            sched = parse_schedule("0 8 * * *", tz_str="America/New_York")
            # 2026-07-01 20:36 UTC is 16:36 EDT — today's 8am ET already passed,
            # so the next 8am ET is 2026-07-02 12:00 UTC.
            now = datetime(2026, 7, 1, 20, 36)
            next_run = sched.next_run(now)
            assert next_run.tzinfo is None
            assert next_run == datetime(2026, 7, 2, 12, 0)
            assert next_run > now

    def test_rrule_tz_schedule_ignores_process_tz(self):
        with _forced_process_tz("Asia/Tokyo"):
            sched = parse_schedule(
                {"rrule": "FREQ=DAILY;BYHOUR=8;BYMINUTE=0;BYSECOND=0"},
                tz_str="America/New_York",
            )
            now = datetime(2026, 7, 1, 20, 36)
            next_run = sched.next_run(now)
            assert next_run.tzinfo is None
            assert next_run == datetime(2026, 7, 2, 12, 0)
            assert next_run > now

    def test_rrule_no_tz_still_returns_naive(self):
        with _forced_process_tz("Asia/Tokyo"):
            sched = RRuleSchedule("FREQ=DAILY;BYHOUR=8;BYMINUTE=0;BYSECOND=0")
            now = datetime(2026, 7, 1, 20, 36)
            next_run = sched.next_run(now)
            assert next_run.tzinfo is None
            assert next_run == datetime(2026, 7, 2, 8, 0)


class TestJitter:
    def test_jitter_adds_time(self):
        sched = IntervalSchedule(60)
        base = datetime(2026, 1, 1, 0, 0, 0)
        jittered = add_jitter(base, sched)
        assert jittered >= base
        # Max jitter for 60s interval is min(6, 30) = 6 seconds
        assert jittered <= base + timedelta(seconds=6.1)

    def test_jitter_capped_at_30(self):
        sched = IntervalSchedule(86400)
        base = datetime(2026, 1, 1, 0, 0, 0)
        jittered = add_jitter(base, sched)
        assert jittered <= base + timedelta(seconds=30.1)


def test_describe_schedule_helper():
    from datasette_cron.schedules import describe_schedule

    assert describe_schedule("interval", '{"seconds": 300}') == ("every 5m", 300)
    assert describe_schedule("cron", '{"expression": "0 8 * * *"}') == (
        "cron: 0 8 * * *",
        None,
    )
    assert describe_schedule("rrule", '{"rrule": "FREQ=DAILY"}') == (
        "rrule: FREQ=DAILY",
        None,
    )
    # Timezone is included in the description for tz-aware schedules
    desc, seconds = describe_schedule(
        "cron", '{"expression": "0 8 * * *"}', "America/New_York"
    )
    assert desc == "cron: 0 8 * * * (America/New_York)"
    assert seconds is None
    # Unparseable stored config falls back to "type: config"
    assert describe_schedule("interval", "not json") == ("interval: not json", None)
    assert describe_schedule("nonsense", "{}") == ("nonsense: {}", None)
