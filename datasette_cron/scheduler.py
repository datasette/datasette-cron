from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .internal_db import InternalDB
from .models import CronTask
from .schedules import add_jitter, parse_schedule, schedule_from_db

logger = logging.getLogger("datasette_cron")


def _utcnow() -> datetime:
    """Current UTC time as a naive datetime (matching SQLite's datetime('now'))."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Scheduler:
    def __init__(self, datasette):
        self.datasette = datasette
        self._handler_registry: dict[str, Callable[..., Any]] = {}
        self._internal_db: InternalDB | None = None
        self._loop_task: asyncio.Task | None = None
        self._wake_event = asyncio.Event()
        self._shutting_down = False
        self._running_tasks: dict[str, asyncio.Task] = {}

    @property
    def internal_db(self) -> InternalDB:
        if self._internal_db is None:
            self._internal_db = InternalDB(self.datasette.get_internal_database())
        return self._internal_db

    def register_handlers(
        self, plugin_name: str, handlers: dict[str, Callable[..., Any]]
    ) -> None:
        for name, fn in handlers.items():
            key = f"{plugin_name}:{name}"
            self._handler_registry[key] = fn
            # Also register without prefix for convenience
            if name not in self._handler_registry:
                self._handler_registry[name] = fn

    def get_handler(self, handler_ref: str) -> Callable[..., Any] | None:
        return self._handler_registry.get(handler_ref)

    def start(self) -> None:
        if self._loop_task is None or self._loop_task.done():
            loop = asyncio.get_running_loop()
            self._loop_task = loop.create_task(self._loop())

    async def shutdown(self) -> None:
        self._shutting_down = True
        self._wake_event.set()

        # Cancel all running task executions
        for name, task in list(self._running_tasks.items()):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        # Cancel the loop
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except (asyncio.CancelledError, Exception):
                pass

    def _wake(self) -> None:
        self._wake_event.set()

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
        updates = {}
        if "schedule" in kwargs:
            tz_str = kwargs.get("timezone")
            # If timezone not provided, look up existing
            if tz_str is None and "timezone" not in kwargs:
                existing = await self.internal_db.get_task(name)
                if existing:
                    tz_str = existing.timezone
            sched = parse_schedule(kwargs["schedule"], tz_str=tz_str)
            updates["schedule_type"] = sched.schedule_type
            updates["schedule_config"] = json.dumps(sched.to_dict())
            now = _utcnow()
            updates["next_run_at"] = add_jitter(sched.next_run(now), sched).isoformat()
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
        # Cancel if running
        if name in self._running_tasks:
            self._running_tasks[name].cancel()
        self._wake()

    async def trigger_task(self, name: str) -> None:
        """Run a task immediately, out of schedule."""
        task = await self.internal_db.get_task(name)
        if not task:
            raise ValueError(f"Task not found: {name}")
        handler_fn = self.get_handler(task.handler)
        if not handler_fn:
            raise ValueError(f"Handler not found: {task.handler}")
        asyncio.get_event_loop().create_task(self._execute_task(task, handler_fn))

    async def enable_task(self, name: str) -> None:
        await self.internal_db.update_task(name, enabled=1)
        self._wake()

    async def disable_task(self, name: str) -> None:
        await self.internal_db.update_task(name, enabled=0)

    # ---- Scheduler Loop ----

    async def _loop(self) -> None:
        logger.info(
            "Scheduler loop started, handlers: %s", list(self._handler_registry.keys())
        )
        while not self._shutting_down:
            # Clear wake event before tick so any wake() during tick is not lost
            self._wake_event.clear()

            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in scheduler tick")
                await asyncio.sleep(5)
                continue

            # Sleep until next due task or max 60s
            sleep_seconds = await self._compute_sleep()
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=sleep_seconds)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
        logger.info("Scheduler loop stopped")

    async def _tick(self) -> None:
        now = _utcnow()
        due_tasks = await self.internal_db.get_due_tasks()

        for task in due_tasks:
            name = task.name
            handler_fn = self.get_handler(task.handler)

            if not handler_fn:
                logger.error(
                    "Handler %r not found for task %r (available: %s), disabling",
                    task.handler,
                    name,
                    list(self._handler_registry.keys()),
                )
                await self.internal_db.update_task(name, enabled=0, last_status="error")
                continue

            # Check overlap policy
            if task.overlap_policy == "skip" and name in self._running_tasks:
                running = self._running_tasks[name]
                if not running.done():
                    # Skip this run, advance next_run_at
                    sched = schedule_from_db(
                        task.schedule_type, task.schedule_config, task.timezone
                    )
                    next_run = add_jitter(sched.next_run(now), sched)
                    await self.internal_db.update_next_run(name, next_run.isoformat())
                    continue

            # Schedule execution
            exec_task = asyncio.get_event_loop().create_task(
                self._execute_task(task, handler_fn)
            )
            self._running_tasks[name] = exec_task

            # Advance next_run_at
            sched = schedule_from_db(
                task.schedule_type, task.schedule_config, task.timezone
            )
            next_run = add_jitter(sched.next_run(now), sched)
            await self.internal_db.update_next_run(name, next_run.isoformat())

    async def _execute_task(
        self, task: CronTask, handler_fn: Callable[..., Any]
    ) -> None:
        name = task.name
        config = (
            json.loads(task.config) if isinstance(task.config, str) else task.config
        )
        max_attempts = task.retry_max + 1
        backoff_strategy = task.retry_backoff

        try:
            for attempt in range(1, max_attempts + 1):
                run_id = await self.internal_db.record_run_start(name, attempt)
                start_time = time.monotonic()
                try:
                    result = handler_fn(self.datasette, config)
                    if asyncio.iscoroutine(result):
                        await result
                    duration_ms = int((time.monotonic() - start_time) * 1000)
                    await self.internal_db.record_run_success(run_id, duration_ms)
                    await self.internal_db.update_next_run(
                        name,
                        (await self._get_next_run_at(task)).isoformat(),
                        last_status="success",
                    )
                    return
                except asyncio.CancelledError:
                    duration_ms = int((time.monotonic() - start_time) * 1000)
                    await self.internal_db.record_run_error(
                        run_id, "Cancelled", duration_ms
                    )
                    raise
                except Exception as e:
                    duration_ms = int((time.monotonic() - start_time) * 1000)
                    await self.internal_db.record_run_error(run_id, str(e), duration_ms)
                    logger.warning(
                        "Task %r attempt %d/%d failed: %s",
                        name,
                        attempt,
                        max_attempts,
                        e,
                    )
                    if attempt < max_attempts:
                        delay = self._backoff_delay(backoff_strategy, attempt)
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            "Task %r failed after %d attempts", name, max_attempts
                        )
                        await self.internal_db.update_next_run(
                            name,
                            (await self._get_next_run_at(task)).isoformat(),
                            last_status="error",
                        )
        finally:
            self._running_tasks.pop(name, None)

    async def _get_next_run_at(self, task: CronTask) -> datetime:
        now = _utcnow()
        sched = schedule_from_db(
            task.schedule_type, task.schedule_config, task.timezone
        )
        return add_jitter(sched.next_run(now), sched)

    async def _compute_sleep(self) -> float:
        tasks = await self.internal_db.get_all_tasks()
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
