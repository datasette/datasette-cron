from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from datasette.telemetry import linked_root_span_kwargs
from opentelemetry.trace import Status, StatusCode

from .internal_db import InternalDB
from .models import CronTask
from .schedules import add_jitter, parse_schedule, schedule_from_db
from . import telemetry
from .telemetry import tracer
from .telemetry_registry import (
    ATTEMPT,
    ATTEMPT_SPAN,
    ATTEMPTS,
    BACKOFF,
    BACKOFF_DELAY,
    CANCELLED,
    CODE_FUNCTION,
    DISABLED,
    DUE,
    ERROR_TYPE,
    HANDLER,
    HANDLER_ASYNC,
    LAG,
    MAX_ATTEMPTS,
    OVERLAP_POLICY,
    PLUGIN,
    RETRY,
    RUN,
    RUN_ID,
    SCHEDULED_AT,
    SKIPPED,
    SLEEP,
    SPAWNED,
    STATUS,
    TASK,
    TICK,
    TRIGGER,
)

logger = logging.getLogger("datasette_cron")


def _utcnow() -> datetime:
    """Current UTC time as a naive datetime (matching SQLite's datetime('now'))."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _lag_seconds(next_run_at: str, now: datetime) -> float:
    """Seconds `now` is past the scheduled `next_run_at` slot, clamped to 0.

    Both sides are naive UTC (`_utcnow` strips tzinfo, and next_run_at is
    stored that way), so the subtraction is correct.
    """
    return max((now - datetime.fromisoformat(next_run_at)).total_seconds(), 0.0)


@dataclass
class TickStats:
    """What one scheduler tick decided, recorded on the tick span."""

    due: int = 0
    spawned: int = 0
    skipped: int = 0
    cancelled: int = 0
    disabled: int = 0


class Scheduler:
    def __init__(self, datasette):
        self.datasette = datasette
        self._handler_registry: dict[str, Callable[..., Any]] = {}
        self._internal_db: InternalDB | None = None
        self._wake_event = asyncio.Event()
        self._shutting_down = False
        # Per-task set of in-flight executions. A manual trigger force-runs
        # regardless of overlap_policy, so multiple runs of the same task can
        # coexist; tracking them all here lets shutdown cancel every one.
        self._running_tasks: dict[str, set[asyncio.Task]] = {}
        # In-memory state the observable gauge callbacks read from the
        # SDK's collection thread (see telemetry.py): when the loop last
        # completed a tick, and the task list _compute_sleep already
        # fetched.
        self._last_tick_finished: float | None = None
        self._task_snapshot: list[CronTask] = []

    @property
    def internal_db(self) -> InternalDB:
        if self._internal_db is None:
            self._internal_db = InternalDB(self.datasette.get_internal_database())
        return self._internal_db

    def register_handlers(
        self, plugin_name: str, handlers: dict[str, Callable[..., Any]]
    ) -> None:
        for name, fn in handlers.items():
            self._handler_registry[f"{plugin_name}:{name}"] = fn

    def get_handler(self, handler_ref: str) -> Callable[..., Any] | None:
        return self._handler_registry.get(handler_ref)

    def list_handlers(self) -> list[str]:
        """Return all registered handler refs (plugin:name), sorted."""
        return sorted(self._handler_registry.keys())

    async def shutdown(self) -> None:
        """Cancel in-flight executions and record final bookkeeping.

        Called from the plugin's `shutdown` hook, which core runs before it
        cancels the supervised `run()` loop task itself (registered via
        `datasette.add_background_task(scheduler.run, ...)` in `startup`) --
        so this no longer touches the loop task. It only needs to deal with
        work this scheduler manages on its own: per-execution child tasks
        spawned by `_spawn_execution`.
        """
        self._shutting_down = True
        self._wake_event.set()
        telemetry.unregister_scheduler(self)

        # Cancel every in-flight execution across all tasks.
        in_flight = [t for tasks in self._running_tasks.values() for t in tasks]
        for t in in_flight:
            t.cancel()
        for t in in_flight:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    def _wake(self) -> None:
        self._wake_event.set()

    def is_running(self, name: str) -> bool:
        """Return True if the task has any in-flight executions in this process."""
        return any(not t.done() for t in self._running_tasks.get(name, ()))

    def _spawn_execution(
        self,
        task: CronTask,
        handler_fn: Callable[..., Any],
        *,
        force: bool = False,
        scheduled_at: str | None = None,
        now: datetime | None = None,
    ) -> str:
        """Spawn _execute_task, respecting overlap_policy unless force=True.

        Returns the outcome: "started", "skipped" (blocked by overlap), or
        "cancelled" (started after cancelling in-flight runs). Manual
        triggers pass force=True so the user's "Run now" always fires.
        """
        name = task.name
        running = {t for t in self._running_tasks.get(name, ()) if not t.done()}

        cancelled_in_flight = False
        if running and not force:
            telemetry.overlaps.add(1, {TASK: name, OVERLAP_POLICY: task.overlap_policy})
            if task.overlap_policy == "skip":
                return "skipped"
            if task.overlap_policy == "cancel":
                for t in running:
                    t.cancel()
                cancelled_in_flight = True

        # Capture the run span's root-with-link kwargs here, not inside
        # _execute_task: this is the one place that is definitely still
        # inside the causing span (the tick span, or core's HTTP request
        # span for a manual trigger).
        run_span_kwargs = linked_root_span_kwargs()
        exec_task = asyncio.get_running_loop().create_task(
            self._execute_task(
                task,
                handler_fn,
                trigger="manual" if force else "scheduled",
                run_span_kwargs=run_span_kwargs,
                scheduled_at=scheduled_at,
                now=now,
            )
        )
        self._running_tasks.setdefault(name, set()).add(exec_task)
        return "cancelled" if cancelled_in_flight else "started"

    # ---- Public Task CRUD API ----

    async def add_task(
        self,
        *,
        name: str,
        handler: str,
        schedule,
        config: dict | None = None,
        timezone: str | None = None,
        overlap: str = "skip",
        retry: dict | None = None,
    ) -> None:
        """Upsert a task. Idempotent -- safe to call on every startup."""
        config = config or {}
        retry = retry or {}
        sched = parse_schedule(schedule, tz_str=timezone)
        now = _utcnow()
        next_run = add_jitter(sched.next_run(now), sched)

        await self.internal_db.upsert_task(
            name=name,
            handler=handler,
            config=config,
            schedule_type=sched.schedule_type,
            schedule_config=json.dumps(sched.to_dict()),
            timezone=timezone,
            overlap_policy=overlap,
            retry_max=retry.get("max_retries", 0),
            retry_backoff=retry.get("backoff", "exponential"),
            next_run_at=next_run.isoformat(),
        )
        self._wake()

    async def update_task(self, name: str, **kwargs) -> None:
        existing = await self.internal_db.get_task(name)
        if not existing:
            raise ValueError(f"Task not found: {name}")

        updates: dict = {}

        # Schedule or timezone changes require recomputing next_run_at: a
        # cron-with-tz task's "8am New York" UTC moment shifts when the tz
        # changes, so we re-derive both schedule_config (in case schedule
        # changed) and next_run_at together from the resolved (sched, tz) pair.
        schedule_changed = "schedule" in kwargs
        timezone_changed = "timezone" in kwargs
        if schedule_changed or timezone_changed:
            tz_str = kwargs["timezone"] if timezone_changed else existing.timezone
            if schedule_changed:
                sched = parse_schedule(kwargs["schedule"], tz_str=tz_str)
            else:
                sched = schedule_from_db(
                    existing.schedule_type, existing.schedule_config, tz_str
                )
            updates["schedule_type"] = sched.schedule_type
            updates["schedule_config"] = json.dumps(sched.to_dict())
            updates["next_run_at"] = add_jitter(
                sched.next_run(_utcnow()), sched
            ).isoformat()

        if "config" in kwargs:
            updates["config"] = kwargs["config"]
        if "timezone" in kwargs:
            updates["timezone"] = kwargs["timezone"]
        if "overlap" in kwargs:
            updates["overlap_policy"] = kwargs["overlap"]
        if "retry" in kwargs:
            retry = kwargs["retry"] or {}
            updates["retry_max"] = retry.get("max_retries", 0)
            updates["retry_backoff"] = retry.get("backoff", "exponential")
        if "enabled" in kwargs:
            updates["enabled"] = 1 if kwargs["enabled"] else 0

        if updates:
            await self.internal_db.update_task(name, **updates)
            self._wake()

    async def remove_task(self, name: str) -> None:
        await self.internal_db.delete_task(name)
        # Cancel any in-flight executions for this task.
        for t in list(self._running_tasks.get(name, ())):
            t.cancel()
        self._wake()

    async def trigger_task(self, name: str) -> None:
        """Run a task immediately, out of schedule.

        Manual triggers force-run regardless of overlap_policy — the user
        explicitly asked for this run, so we honor that even if a scheduled
        execution is in flight. The concurrent run is tracked and visible
        in the runs table with status='running'.
        """
        task = await self.internal_db.get_task(name)
        if not task:
            raise ValueError(f"Task not found: {name}")
        handler_fn = self.get_handler(task.handler)
        if not handler_fn:
            raise ValueError(f"Handler not found: {task.handler}")
        self._spawn_execution(task, handler_fn, force=True)

    async def set_enabled(self, name: str, enabled: bool) -> None:
        """Enable or disable a task. A no-op for unknown task names."""
        await self.internal_db.update_task(name, enabled=1 if enabled else 0)
        self._wake()

    # ---- Scheduler Loop ----

    async def run(self, datasette) -> None:
        """Entry point for `datasette.add_background_task(scheduler.run, ...)`.

        Thin adaptation of the scheduler's main loop to the supervised
        background-task signature core calls as `func(datasette)`.
        `datasette` here is always `self.datasette` -- the loop already
        closes over that -- so it's accepted for signature compatibility
        and otherwise unused.
        """
        logger.info(
            "Scheduler loop started, handlers: %s", list(self._handler_registry.keys())
        )
        while not self._shutting_down:
            # Clear wake event before tick so any wake() during tick is not
            # lost.
            self._wake_event.clear()

            # The wait on the wake event and the error sleep both stay
            # outside the span, so its duration means "work". The span is
            # emitted even for a no-op tick: it is the parent that stops the
            # loop's own queries being orphan roots, and one-span-a-minute
            # is the "is the loop alive?" signal.
            sleep_seconds: float | None = None
            with tracer.start_as_current_span(TICK) as tick_span:
                try:
                    stats = await self._tick()
                except asyncio.CancelledError:
                    break
                except Exception:
                    logger.exception("Error in scheduler tick")
                    # Caught here rather than escaping the block, so set the
                    # status by hand; the error sleep happens below, outside
                    # the span.
                    tick_span.set_status(Status(StatusCode.ERROR))
                else:
                    tick_span.set_attribute(DUE, stats.due)
                    tick_span.set_attribute(SPAWNED, stats.spawned)
                    tick_span.set_attribute(SKIPPED, stats.skipped)
                    tick_span.set_attribute(CANCELLED, stats.cancelled)
                    tick_span.set_attribute(DISABLED, stats.disabled)
                    # Sleep until next due task or max 60s
                    sleep_seconds = await self._compute_sleep()
                    tick_span.set_attribute(SLEEP, sleep_seconds)

            # An errored tick still counts as the loop being alive; the
            # tick.age gauge measures loop liveness, not tick success.
            self._last_tick_finished = time.monotonic()

            if sleep_seconds is None:
                await asyncio.sleep(5)
                continue
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=sleep_seconds)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
        logger.info("Scheduler loop stopped")

    async def _tick(self, now: datetime | None = None) -> TickStats:
        # `now` is injectable for tests; production always uses wall-clock.
        if now is None:
            now = _utcnow()
        due_tasks = await self.internal_db.get_due_tasks()
        stats = TickStats(due=len(due_tasks))

        for task in due_tasks:
            name = task.name
            handler_fn = self.get_handler(task.handler)

            # Lag is recorded for every due task, spawned or not, so an
            # overlap-starved task still shows its slot drifting.
            if task.next_run_at:
                telemetry.run_lag.record(
                    _lag_seconds(task.next_run_at, now), {TASK: name}
                )

            if not handler_fn:
                logger.error(
                    "Handler %r not found for task %r (available: %s), disabling",
                    task.handler,
                    name,
                    list(self._handler_registry.keys()),
                )
                await self.internal_db.update_task(name, enabled=0, last_status="error")
                stats.disabled += 1
                continue

            outcome = self._spawn_execution(
                task, handler_fn, scheduled_at=task.next_run_at, now=now
            )
            if outcome == "skipped":
                stats.skipped += 1
                logger.debug(
                    "Skipped %r: overlap_policy=%s and a run is in flight",
                    name,
                    task.overlap_policy,
                )
            else:
                stats.spawned += 1
                if outcome == "cancelled":
                    stats.cancelled += 1

            # Advance next_run_at regardless of whether we spawned — a skipped
            # run still consumes its scheduling slot.
            #
            # The new next_run is deliberately anchored to the tick's
            # wall-clock `now`, not the task's stored (scheduled) next_run_at:
            # - intervals mean "at least N seconds between scheduled starts",
            #   so their phase drifts by tick latency; this also means a
            #   scheduler that was down never tries to catch up on missed
            #   slots (no burst of back-to-back runs after downtime).
            # - cron/rrule next-runs are absolute wall-clock times, so a slot
            #   is only skipped when the tick itself is more than a full
            #   period late — acceptable for a best-effort scheduler.
            sched = schedule_from_db(
                task.schedule_type, task.schedule_config, task.timezone
            )
            next_run = add_jitter(sched.next_run(now), sched)
            await self.internal_db.update_next_run(name, next_run.isoformat())

        return stats

    async def _execute_task(
        self,
        task: CronTask,
        handler_fn: Callable[..., Any],
        *,
        trigger: str = "scheduled",
        run_span_kwargs: dict | None = None,
        scheduled_at: str | None = None,
        now: datetime | None = None,
    ) -> None:
        name = task.name
        config = (
            json.loads(task.config) if isinstance(task.config, str) else task.config
        )
        max_attempts = task.retry_max + 1
        backoff_strategy = task.retry_backoff
        plugin, _, _ = task.handler.partition(":")
        if run_span_kwargs is None:
            # create_task copied the context, so the causing span is still
            # current here too; the helper discards it and keeps the link.
            run_span_kwargs = linked_root_span_kwargs()

        def record_attempt_metrics(outcome: str, attempt: int, elapsed: float) -> None:
            # Called while the attempt span is current, so the SDK can
            # attach an exemplar linking the histogram bucket to the trace.
            telemetry.run_duration.record(
                elapsed,
                {TASK: name, HANDLER: task.handler, STATUS: outcome, TRIGGER: trigger},
            )
            telemetry.attempts.add(
                1,
                {
                    TASK: name,
                    HANDLER: task.handler,
                    STATUS: outcome,
                    RETRY: attempt > 1,
                },
            )

        try:
            with tracer.start_as_current_span(RUN, **run_span_kwargs) as run_span:
                run_span.set_attribute(TASK, name)
                run_span.set_attribute(HANDLER, task.handler)
                run_span.set_attribute(PLUGIN, plugin)
                run_span.set_attribute(
                    CODE_FUNCTION,
                    "{}.{}".format(
                        getattr(handler_fn, "__module__", "<unknown>"),
                        getattr(handler_fn, "__qualname__", repr(handler_fn)),
                    ),
                )
                run_span.set_attribute(TRIGGER, trigger)
                run_span.set_attribute(MAX_ATTEMPTS, max_attempts)
                if scheduled_at is not None:
                    run_span.set_attribute(SCHEDULED_AT, scheduled_at)
                    run_span.set_attribute(
                        LAG, _lag_seconds(scheduled_at, now or _utcnow())
                    )
                status = "error"
                error_type: str | None = None
                error_message: str | None = None
                attempts_made = 0
                try:
                    for attempt in range(1, max_attempts + 1):
                        attempts_made = attempt
                        with tracer.start_as_current_span(ATTEMPT_SPAN) as attempt_span:
                            attempt_span.set_attribute(ATTEMPT, attempt)
                            run_id = await self.internal_db.record_run_start(
                                name, attempt
                            )
                            attempt_span.set_attribute(RUN_ID, run_id)
                            start_time = time.monotonic()
                            try:
                                result = handler_fn(self.datasette, config)
                                is_async = asyncio.iscoroutine(result)
                                attempt_span.set_attribute(HANDLER_ASYNC, is_async)
                                if is_async:
                                    await result
                                elapsed = time.monotonic() - start_time
                                duration_ms = int(elapsed * 1000)
                                record_attempt_metrics("success", attempt, elapsed)
                                await self.internal_db.record_run_success(
                                    run_id, duration_ms
                                )
                                await self.internal_db.mark_last_run(name, "success")
                                status = "success"
                                return
                            except asyncio.CancelledError:
                                elapsed = time.monotonic() - start_time
                                duration_ms = int(elapsed * 1000)
                                record_attempt_metrics("cancelled", attempt, elapsed)
                                status = "cancelled"
                                error_type = "CancelledError"
                                error_message = "Cancelled"
                                attempt_span.set_attribute(ERROR_TYPE, error_type)
                                # use_span() only handles Exception, and
                                # CancelledError is a BaseException - the
                                # operator reading a trace still wants to see
                                # that the handler did not finish.
                                attempt_span.set_status(
                                    Status(StatusCode.ERROR, error_message)
                                )
                                await self.internal_db.record_run_error(
                                    run_id, "Cancelled", duration_ms
                                )
                                raise
                            except Exception as e:
                                elapsed = time.monotonic() - start_time
                                duration_ms = int(elapsed * 1000)
                                record_attempt_metrics("error", attempt, elapsed)
                                error_type = type(e).__name__
                                error_message = str(e)
                                attempt_span.set_attribute(ERROR_TYPE, error_type)
                                attempt_span.record_exception(e)
                                attempt_span.set_status(
                                    Status(StatusCode.ERROR, error_message)
                                )
                                await self.internal_db.record_run_error(
                                    run_id, str(e), duration_ms
                                )
                                logger.warning(
                                    "Task %r attempt %d/%d failed: %s",
                                    name,
                                    attempt,
                                    max_attempts,
                                    e,
                                )
                        # The retry sleep lives outside the attempt span so
                        # the backoff span is a sibling of the attempts, not
                        # nested inside a failed one.
                        if attempt < max_attempts:
                            delay = self._backoff_delay(backoff_strategy, attempt)
                            with tracer.start_as_current_span(BACKOFF) as backoff_span:
                                backoff_span.set_attribute(BACKOFF_DELAY, delay)
                                await asyncio.sleep(delay)
                        else:
                            logger.error(
                                "Task %r failed after %d attempts", name, max_attempts
                            )
                            await self.internal_db.mark_last_run(name, "error")
                finally:
                    run_span.set_attribute(ATTEMPTS, attempts_made)
                    run_span.set_attribute(STATUS, status)
                    # A failed attempt that was then retried successfully
                    # leaves the run span clean; the failure is on the
                    # attempt span.
                    if status != "success":
                        if error_type is not None:
                            run_span.set_attribute(ERROR_TYPE, error_type)
                        run_span.set_status(Status(StatusCode.ERROR, error_message))
        finally:
            # Remove ourselves from the in-flight set; clean up empty entries.
            current = asyncio.current_task()
            running = self._running_tasks.get(name)
            if running is not None and current is not None:
                running.discard(current)
                if not running:
                    self._running_tasks.pop(name, None)

    async def _compute_sleep(self) -> float:
        tasks = await self.internal_db.get_all_tasks()
        # Cache for the tasks gauge callback: it runs on the SDK's
        # collection thread and must never touch SQLite itself.
        self._task_snapshot = tasks
        now = _utcnow()
        min_wait = 60.0
        for task in tasks:
            if not task.enabled or not task.next_run_at:
                continue
            next_run = datetime.fromisoformat(task.next_run_at)
            wait = (next_run - now).total_seconds()
            if wait < min_wait:
                min_wait = max(wait, 0.1)
        return min_wait

    @staticmethod
    def _backoff_delay(strategy: str, attempt: int) -> float:
        if strategy == "exponential":
            base = min(2**attempt, 300)
        elif strategy == "linear":
            base = attempt * 30
        else:
            base = 30
        # Add jitter
        return base * random.uniform(0.8, 1.2)
