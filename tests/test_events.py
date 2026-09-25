import asyncio

import pytest
from datasette import hookimpl as _hookimpl
from datasette.plugins import pm

from datasette_cron.events import RunFinishedEvent

from .test_cron import _cancel_in_flight, _make_scheduler


@pytest.fixture
def capture():
    """Registers a `track_event` hookimpl that appends every tracked event
    to the yielded list, unregistering it again on teardown."""
    events = []

    class CapturingPlugin:
        __name__ = "datasette_test_events_capture"

        @staticmethod
        @_hookimpl
        def track_event(datasette, event):
            events.append(event)

    pm.register(CapturingPlugin, name="test_events_capture")
    try:
        yield events
    finally:
        pm.unregister(name="test_events_capture")


async def _wait_for_run_events(capture, count, timeout=5.0):
    """Poll until `count` RunFinishedEvents have been captured, then return
    them. Polling (rather than a fixed sleep) keeps the overlapping-run
    tests deterministic without depending on how long a run takes."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        run_events = [e for e in capture if isinstance(e, RunFinishedEvent)]
        if len(run_events) >= count:
            return run_events
        assert loop.time() < deadline, (
            f"Timed out waiting for {count} RunFinishedEvents, got {len(run_events)}"
        )
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_failing_handler_emits_one_error_event(capture):
    ds, scheduler = await _make_scheduler()

    async def failing_handler(datasette, config):
        raise RuntimeError("boom")

    scheduler.register_handlers("test", {"fail-handler": failing_handler})
    await scheduler.add_task(
        name="fail-record-task",
        handler="test:fail-handler",
        schedule={"interval": 99999},
    )

    await scheduler.trigger_task("fail-record-task")
    await asyncio.sleep(0.5)

    run_events = [e for e in capture if isinstance(e, RunFinishedEvent)]
    assert len(run_events) == 1
    event = run_events[0]
    assert event.status == "error"
    assert event.alert_target() == ("cron-task", "test", "fail-record-task")
    assert event.dedupe_key == "fail-record-task:error"

    runs = await scheduler.internal_db.get_runs("fail-record-task")
    assert event.run_id == runs[0].id
    assert event.trace_id == runs[0].trace_id

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_succeeding_handler_on_fresh_task_emits_nothing(capture):
    ds, scheduler = await _make_scheduler()

    async def ok_handler(datasette, config):
        pass

    scheduler.register_handlers("test", {"ok-handler": ok_handler})
    await scheduler.add_task(
        name="ok-task",
        handler="test:ok-handler",
        schedule={"interval": 99999},
    )

    # last_status is None on a never-run task.
    await scheduler.trigger_task("ok-task")
    await asyncio.sleep(0.5)

    assert capture == []

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_fail_then_succeed_then_succeed(capture):
    ds, scheduler = await _make_scheduler()

    should_fail = True

    async def flaky_handler(datasette, config):
        if should_fail:
            raise RuntimeError("boom")

    scheduler.register_handlers("test", {"flaky": flaky_handler})
    await scheduler.add_task(
        name="flaky-task",
        handler="test:flaky",
        schedule={"interval": 99999},
    )

    # 1. Fails -- emits status=="error".
    await scheduler.trigger_task("flaky-task")
    await asyncio.sleep(0.5)
    assert len(capture) == 1
    assert capture[0].status == "error"

    # 2. Recovers -- emits status=="success" (transition out of error).
    should_fail = False
    await scheduler.trigger_task("flaky-task")
    await asyncio.sleep(0.5)
    assert len(capture) == 2
    assert capture[1].status == "success"

    # 3. Routine success -- emits nothing more.
    await scheduler.trigger_task("flaky-task")
    await asyncio.sleep(0.5)
    assert len(capture) == 2

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_cancel_emits_cancelled_event(capture):
    ds, scheduler = await _make_scheduler()

    in_handler = asyncio.Event()

    async def long_handler(datasette, config):
        in_handler.set()
        await asyncio.sleep(60)

    scheduler.register_handlers("test", {"long": long_handler})
    await scheduler.add_task(
        name="cancel-task",
        handler="test:long",
        schedule={"interval": 99999},
    )

    await scheduler.trigger_task("cancel-task")
    await asyncio.wait_for(in_handler.wait(), timeout=2.0)

    # remove_task cancels any in-flight execution for the task.
    await scheduler.remove_task("cancel-task")
    await asyncio.sleep(0.5)

    run_events = [e for e in capture if isinstance(e, RunFinishedEvent)]
    assert len(run_events) == 1
    assert run_events[0].status == "cancelled"

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_success_after_cancel_emits_recovery_event(capture):
    """cancel -> success is a recovery, same as error -> success.

    Only reachable because a cancelled run now writes last_status =
    "cancelled"; while the cancel branch recorded an error the
    `previous == "cancelled"` arm of the transition rule was dead code.
    """
    ds, scheduler = await _make_scheduler()

    blocking = True
    entered = asyncio.Event()

    async def sometimes_blocking_handler(datasette, config):
        if blocking:
            entered.set()
            await asyncio.sleep(60)

    scheduler.register_handlers("test", {"maybe-block": sometimes_blocking_handler})
    await scheduler.add_task(
        name="recover-after-cancel",
        handler="test:maybe-block",
        schedule={"interval": 99999},
    )

    # 1. Cancelled mid-handler -- emits status=="cancelled".
    await scheduler.trigger_task("recover-after-cancel")
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    await _cancel_in_flight(scheduler, "recover-after-cancel")

    run_events = await _wait_for_run_events(capture, 1)
    assert run_events[0].status == "cancelled"

    task = await scheduler.internal_db.get_task("recover-after-cancel")
    assert task.last_status == "cancelled"

    # 2. Recovers -- emits status=="success" (transition out of cancelled).
    blocking = False
    await scheduler.trigger_task("recover-after-cancel")
    run_events = await _wait_for_run_events(capture, 2)
    assert run_events[1].status == "success"

    # 3. Routine success -- emits nothing more.
    await scheduler.trigger_task("recover-after-cancel")
    await asyncio.sleep(0.5)
    assert len([e for e in capture if isinstance(e, RunFinishedEvent)]) == 2

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_manual_trigger_records_actor(capture):
    ds, scheduler = await _make_scheduler()

    async def failing_handler(datasette, config):
        raise RuntimeError("boom")

    scheduler.register_handlers("test", {"fail-handler": failing_handler})
    await scheduler.add_task(
        name="actor-task",
        handler="test:fail-handler",
        schedule={"interval": 99999},
    )

    await scheduler.trigger_task("actor-task", actor_id="alex")
    await asyncio.sleep(0.5)

    run_events = [e for e in capture if isinstance(e, RunFinishedEvent)]
    assert len(run_events) == 1
    assert run_events[0].actor == {"id": "alex"}
    assert run_events[0].concerns == ["alex"]

    await scheduler.shutdown()


def _gated_failing_handler(slots):
    """Return a handler whose Nth call sets `entered[N]`, waits for
    `release[N]`, then fails -- so a test can interleave two runs of one
    task and decide which finishes first. `slots` is (entered, release)."""
    entered, release = slots
    calls = 0

    async def handler(datasette, config):
        nonlocal calls
        index = calls
        calls += 1
        entered[index].set()
        await release[index].wait()
        raise RuntimeError("boom")

    return handler


def _two_slots():
    return ([asyncio.Event(), asyncio.Event()], [asyncio.Event(), asyncio.Event()])


@pytest.mark.asyncio
async def test_scheduled_run_does_not_steal_an_overlapping_actor(capture):
    """A scheduled run that overlaps a manual one must not take its actor.

    The actor travels with the execution, so the run order cannot change who
    each event is attributed to. Here the scheduled run is spawned first and
    finishes first, while Alex's manual run is still in flight.
    """
    ds, scheduler = await _make_scheduler()

    slots = _two_slots()
    entered, release = slots
    scheduler.register_handlers("test", {"gated": _gated_failing_handler(slots)})
    await scheduler.add_task(
        name="overlap-actor-task",
        handler="test:gated",
        schedule={"interval": 99999},
        overlap="allow",
    )

    # Run 0 -- scheduled: force the task due and run a single tick.
    await scheduler.internal_db.update_next_run(
        "overlap-actor-task", "2000-01-01T00:00:00"
    )
    await scheduler._tick()
    await asyncio.wait_for(entered[0].wait(), timeout=2.0)

    # Run 1 -- manual: Alex presses "Run now" mid-flight (force ignores
    # overlap_policy, so both runs are live at once).
    await scheduler.trigger_task("overlap-actor-task", actor_id="alex")
    await asyncio.wait_for(entered[1].wait(), timeout=2.0)

    # The scheduled run finishes first. Its event must carry no actor...
    release[0].set()
    run_events = await _wait_for_run_events(capture, 1)
    assert run_events[0].actor is None
    assert run_events[0].concerns == []

    # ...and Alex's own run must still carry Alex when it finishes second.
    release[1].set()
    run_events = await _wait_for_run_events(capture, 2)
    assert run_events[1].actor == {"id": "alex"}
    assert run_events[1].concerns == ["alex"]

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_two_manual_triggers_keep_their_own_actors(capture):
    """Two users triggering the same task must not swap actors.

    Alex triggers first and finishes first; Bob triggers second. Each event
    is attributed to the person who started that run.
    """
    ds, scheduler = await _make_scheduler()

    slots = _two_slots()
    entered, release = slots
    scheduler.register_handlers("test", {"gated": _gated_failing_handler(slots)})
    await scheduler.add_task(
        name="two-actor-task",
        handler="test:gated",
        schedule={"interval": 99999},
        overlap="allow",
    )

    await scheduler.trigger_task("two-actor-task", actor_id="alex")
    await asyncio.wait_for(entered[0].wait(), timeout=2.0)
    await scheduler.trigger_task("two-actor-task", actor_id="bob")
    await asyncio.wait_for(entered[1].wait(), timeout=2.0)

    release[0].set()
    run_events = await _wait_for_run_events(capture, 1)
    assert run_events[0].concerns == ["alex"]

    release[1].set()
    run_events = await _wait_for_run_events(capture, 2)
    assert run_events[1].concerns == ["bob"]

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_scheduled_run_after_manual_trigger_has_no_actor(capture):
    """An actor must not linger past the run it triggered.

    A later scheduled run of the same task is nobody's doing, so its event
    carries no actor and concerns nobody.
    """
    ds, scheduler = await _make_scheduler()

    async def failing_handler(datasette, config):
        raise RuntimeError("boom")

    scheduler.register_handlers("test", {"fail-handler": failing_handler})
    await scheduler.add_task(
        name="later-scheduled-task",
        handler="test:fail-handler",
        schedule={"interval": 99999},
    )

    await scheduler.trigger_task("later-scheduled-task", actor_id="alex")
    run_events = await _wait_for_run_events(capture, 1)
    assert run_events[0].actor == {"id": "alex"}

    # Now let the schedule fire it: force the task due and run one tick.
    await scheduler.internal_db.update_next_run(
        "later-scheduled-task", "2000-01-01T00:00:00"
    )
    await scheduler._tick()
    run_events = await _wait_for_run_events(capture, 2)
    assert run_events[1].actor is None
    assert run_events[1].concerns == []

    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_raising_consumer_does_not_change_run_status(capture):
    ds, scheduler = await _make_scheduler()

    class RaisingPlugin:
        __name__ = "datasette_test_raising_consumer"

        @staticmethod
        @_hookimpl
        def track_event(datasette, event):
            if isinstance(event, RunFinishedEvent):
                raise RuntimeError("consumer bug")

    pm.register(RaisingPlugin, name="test_raising_consumer")
    try:

        async def failing_handler(datasette, config):
            raise RuntimeError("boom")

        scheduler.register_handlers("test", {"fail-handler": failing_handler})
        await scheduler.add_task(
            name="raising-consumer-task",
            handler="test:fail-handler",
            schedule={"interval": 99999},
        )

        await scheduler.trigger_task("raising-consumer-task")
        await asyncio.sleep(0.5)

        runs = await scheduler.internal_db.get_runs("raising-consumer-task")
        assert runs[0].status == "error"
        assert "boom" in runs[0].error_message

        task = await scheduler.internal_db.get_task("raising-consumer-task")
        assert task.last_status == "error"

        await scheduler.shutdown()
    finally:
        pm.unregister(name="test_raising_consumer")
