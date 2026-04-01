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
