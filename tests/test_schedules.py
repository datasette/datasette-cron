import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from datasette_cron.schedules import (
    CronSchedule,
    IntervalSchedule,
    parse_schedule,
    schedule_from_db,
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
        with pytest.raises(ValueError):
            CronSchedule("not a cron")


class TestIntervalSchedule:
    def test_next_run(self):
        sched = IntervalSchedule(60)
        after = datetime(2026, 3, 30, 12, 0, 0)
        next_run = sched.next_run(after)
        assert next_run == datetime(2026, 3, 30, 12, 1, 0)

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


class TestParseSchedule:
    def test_parse_cron_string(self):
        sched = parse_schedule("0 8 * * *")
        assert isinstance(sched, CronSchedule)

    def test_parse_interval_dict(self):
        sched = parse_schedule({"interval": 60})
        assert isinstance(sched, IntervalSchedule)
        assert sched.seconds == 60

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


def test_describe_schedule_helper():
    from datasette_cron.schedules import describe_schedule

    assert describe_schedule("interval", '{"seconds": 300}') == ("every 5m", 300)
    assert describe_schedule("cron", '{"expression": "0 8 * * *"}') == (
        "cron: 0 8 * * *",
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
