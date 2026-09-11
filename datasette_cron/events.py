from dataclasses import dataclass

from datasette.events import Event


@dataclass
class RunFinishedEvent(Event):
    """Event name: ``cron-run-finished``. Emitted when a run ends in
    ``error`` or ``cancelled``, and once when a task returns to ``success``
    after a failure. Not emitted for routine successes -- the alerts drain
    itself runs as a cron task every few seconds, and an emit-side rule is
    the only thing that keeps that from generating an event storm.

    ``alert_view_action`` names an action (``cron-task-view``) that does not
    exist on this branch -- cron's own ACL is a later effort (see
    ``R03-cron-acl-compat.md`` "Recommended compatible cron-task resource
    sketch"). Until a plugin registers that action with a ``resource_class``,
    alerts' protocol says an action with no ``resource_class`` is checked
    globally with no resource, so subscribe-time and fire-time checks both
    fall back to this plugin's existing global access action,
    ``datasette-cron-access`` (``router.py``'s ``ACCESS_ACTION``). No cron
    change is needed for that fallback to work.

    Recursion note: alerts caps deliveries and applies a cooldown for events
    whose target parent is ``"alerts"`` (consume side, alerts ticket 07).
    cron does not special-case its own tasks here -- it emits for every
    task, including alerts' own drain, so that a broken drain can still be
    reported once instead of being silently swallowed.
    """

    name = "cron-run-finished"

    task_name: str
    handler: str  # 'plugin:name'
    status: str  # error | cancelled | success (transition only)
    attempt: int
    error_message: str | None
    run_id: int | None
    trace_id: str | None
    span_id: str | None

    # --- datasette-alerts protocol (duck-typed; no import of datasette_alerts) ---
    alert_view_action = "cron-task-view"
    alert_target_wildcard_child = True

    def alert_target(self):
        plugin = self.handler.partition(":")[0]
        return ("cron-task", plugin, self.task_name)

    @property
    def concerns(self):
        # No owner today; actor is set only for manually-triggered runs.
        return [self.actor["id"]] if self.actor else []

    @property
    def dedupe_key(self):
        return f"{self.task_name}:{self.status}"

    @property
    def url(self):
        # Matches the real detail route (`routes/pages.py`'s
        # `r"/-/cron/(?P<task_name>[^/]+)$"`), not the `/-/cron/tasks/...`
        # path in the original sketch -- there is no `/tasks/` segment.
        # Not base_url-aware: the Event has no datasette instance to call
        # `datasette.urls.path()` with.
        return f"/-/cron/{self.task_name}"
