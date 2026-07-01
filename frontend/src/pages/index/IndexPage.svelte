<script lang="ts">
  import type { IndexPageData } from "../../page_data/IndexPageData.types.ts";
  import { loadPageData } from "../../page_data/load.ts";
  import { appState } from "../../store.svelte.ts";
  import { client, api, PollBackoff } from "../../lib/api.ts";
  import { countdown, recentlyDue } from "../../lib/time.ts";
  import {
    isContinuous,
    taskToSummary,
    type TaskSummary,
  } from "../../lib/tasks.ts";
  import StatusDot from "../../lib/StatusDot.svelte";
  import "../../lib/shared.css";

  const pageData = loadPageData<IndexPageData>();

  let tasks = $state<TaskSummary[]>(pageData.tasks);
  let now = $state(Date.now());
  let errorMessage = $state<string | null>(null);
  // Per-task busy flags so action buttons are disabled while their POST is
  // in flight (a double-clicked "Run now" would force-run twice).
  let busyTrigger = $state<Record<string, boolean>>({});
  let busyToggle = $state<Record<string, boolean>>({});

  // Refresh bookkeeping. Deliberately non-reactive: the poller reads `tasks`
  // inside a setInterval callback (async, so untracked), which means
  // reassigning `tasks` can never re-trigger the poller — the old
  // $effect-based refresh re-ran itself on every response and produced an
  // unthrottled request loop while a task was due.
  const refreshing = new Set<string>();
  const nextPollAt = new Map<string, number>();
  // After "Run now": poll until last_run_at advances (run completed), capped.
  const pendingTriggers = new Map<
    string,
    { until: number; lastRunAt: string | null }
  >();
  // Bumped on every user action; refresh responses that started before the
  // latest mutation are discarded so a stale snapshot can't revert a toggle.
  let mutationCount = 0;
  const backoff = new PollBackoff();

  function pollingRecovered() {
    errorMessage = null;
    backoff.reset();
  }

  // Tick every 5 seconds (drives countdown rendering only)
  $effect(() => {
    const id = setInterval(() => {
      now = Date.now();
    }, 5000);
    return () => clearInterval(id);
  });

  // Poll once a second; each task refreshes at most once per 1-2s while it
  // is due, recently triggered, or running. Skip continuous tasks
  // (interval < 10s) to avoid excessive API hits.
  $effect(() => {
    const id = setInterval(pollTasks, 1000);
    return () => clearInterval(id);
  });

  function shouldPoll(task: (typeof tasks)[number], t: number): boolean {
    const pending = pendingTriggers.get(task.name);
    if (pending && t < pending.until) return true;
    if (task.last_status === "running") return true;
    if (!task.next_run_at || !task.enabled || isContinuous(task)) return false;
    return recentlyDue(task.next_run_at, t);
  }

  function pollTasks() {
    const t = Date.now();
    if (t < backoff.pausedUntil) return;
    for (const task of tasks) {
      if (!shouldPoll(task, t)) continue;
      if (refreshing.has(task.name)) continue;
      if (t < (nextPollAt.get(task.name) ?? 0)) continue;
      refreshTask(task.name);
    }
  }

  async function refreshTask(name: string) {
    if (refreshing.has(name)) return;
    refreshing.add(name);
    const startedMutation = mutationCount;
    try {
      const { data, errorMessage: err } = await api(
        client.GET("/-/api/cron/tasks/{task_name}", {
          params: { path: { task_name: name } },
        }),
      );
      if (err !== undefined) {
        errorMessage = `Failed to refresh "${name}": ${err}`;
        backoff.fail();
        return;
      }
      backoff.reset();
      // Discard responses that raced a user action
      if (!data || startedMutation !== mutationCount) return;
      const updated = taskToSummary(data);
      const prev = tasks.find((t) => t.name === name);
      // Backoff: if the server still reports the same state (e.g. due but
      // the scheduler hasn't advanced next_run_at yet), wait 2s before the
      // next attempt; otherwise 1s.
      const unchanged =
        prev !== undefined &&
        prev.next_run_at === updated.next_run_at &&
        prev.last_run_at === updated.last_run_at &&
        prev.last_status === updated.last_status;
      nextPollAt.set(name, Date.now() + (unchanged ? 2000 : 1000));
      tasks = tasks.map((t) => (t.name === name ? updated : t));
      const pending = pendingTriggers.get(name);
      if (pending && updated.last_run_at !== pending.lastRunAt) {
        pendingTriggers.delete(name);
      }
    } finally {
      refreshing.delete(name);
    }
  }

  async function triggerTask(name: string) {
    if (busyTrigger[name]) return;
    busyTrigger[name] = true;
    mutationCount++;
    const prev = tasks.find((t) => t.name === name);
    // Keep polling until the run completes (last_run_at advances), capped at
    // 2 minutes, so the UI converges even for long-running manual triggers.
    pendingTriggers.set(name, {
      until: Date.now() + 120_000,
      lastRunAt: prev?.last_run_at ?? null,
    });
    try {
      const { data, errorMessage: err } = await api(
        client.POST("/-/api/cron/tasks/{task_name}/trigger", {
          params: { path: { task_name: name } },
          // No body, but Datasette's CSRF protection only skips requests
          // that declare a JSON content type.
          headers: { "Content-Type": "application/json" },
        }),
      );
      if (err !== undefined || !data?.ok) {
        pendingTriggers.delete(name);
        errorMessage = `Failed to trigger "${name}": ${err ?? data?.message ?? "unknown error"}`;
        return;
      }
      pollingRecovered();
      nextPollAt.set(name, 0);
    } finally {
      busyTrigger[name] = false;
    }
  }

  async function toggleTask(name: string, currentEnabled: boolean) {
    if (busyToggle[name]) return;
    busyToggle[name] = true;
    mutationCount++; // invalidate in-flight refreshes
    // Optimistic update; the response (or a later refresh) settles it.
    tasks = tasks.map((t) =>
      t.name === name ? { ...t, enabled: !currentEnabled } : t,
    );
    try {
      const { data, errorMessage: err } = await api(
        client.POST("/-/api/cron/tasks/{task_name}/enable", {
          params: { path: { task_name: name } },
          body: { enabled: !currentEnabled },
        }),
      );
      if (err !== undefined || !data?.ok) {
        // Revert the optimistic update
        tasks = tasks.map((t) =>
          t.name === name ? { ...t, enabled: currentEnabled } : t,
        );
        errorMessage = `Failed to ${currentEnabled ? "disable" : "enable"} "${name}": ${err ?? "unknown error"}`;
        return;
      }
      tasks = tasks.map((t) =>
        t.name === name ? { ...t, enabled: data.enabled } : t,
      );
      pollingRecovered();
    } finally {
      busyToggle[name] = false;
    }
  }
</script>

<div class="cron-page">
  <div class="cron-header">
    <h1>Cron Tasks</h1>
    <p class="cron-subtitle">{tasks.length} registered task{tasks.length !== 1 ? "s" : ""}</p>
  </div>

  {#if errorMessage}
    <div class="error-banner" role="alert">
      <span>{errorMessage}</span>
      <button
        class="error-dismiss"
        onclick={() => (errorMessage = null)}
        aria-label="Dismiss error"
      >
        &times;
      </button>
    </div>
  {/if}

  {#if tasks.length === 0}
    <div class="cron-empty">
      <p>No scheduled tasks registered.</p>
      <p class="cron-empty-hint">Plugins can register handlers via <code>cron_register_handlers</code> and create tasks via <code>scheduler.add_task()</code>.</p>
    </div>
  {:else}
    <div class="cron-tasks">
      {#each tasks as task (task.name)}
        {@const continuous = isContinuous(task)}
        {@const next = continuous ? { text: "continuous", className: "time-continuous" } : countdown(task.next_run_at, now)}
        <div class="cron-task-card" class:disabled={!task.enabled}>
          <div class="task-main">
            <div class="task-name-row">
              <a href="{appState.basePath}/{task.name}" class="task-name">{task.name}</a>
              {#if !task.enabled}
                <span class="badge badge-disabled">disabled</span>
              {/if}
            </div>
            <div class="task-meta">
              <span class="task-handler"><code>{task.handler}</code></span>
              <span class="task-schedule">{task.schedule_description}</span>
            </div>
          </div>
          <div class="task-status">
            <div class="task-timing">
              {#if task.last_status}
                <StatusDot
                  status={task.last_status}
                  label="Last run status: {task.last_status}"
                />
              {/if}
              <span class="next-run {next.className}" title={task.next_run_at ?? ""}>{next.text}</span>
            </div>
            <div class="task-actions">
              <button
                class="btn btn-sm"
                disabled={!!busyTrigger[task.name]}
                onclick={() => triggerTask(task.name)}
              >
                Run now
              </button>
              <button
                class="btn btn-sm btn-toggle"
                class:btn-on={task.enabled}
                aria-pressed={task.enabled}
                disabled={!!busyToggle[task.name]}
                onclick={() => toggleTask(task.name, task.enabled)}
              >
                {task.enabled ? "Enabled" : "Disabled"}
              </button>
            </div>
          </div>
        </div>
      {/each}
    </div>
  {/if}

  <!-- Tasks referencing a handler not in this list never run (the scheduler
       skips unknown handlers), so surface what's actually registered. -->
  <div class="cron-handlers">
    <span class="cron-handlers-label">Registered handlers:</span>
    {#if pageData.handlers.length === 0}
      <span class="cron-handlers-empty">none</span>
    {:else}
      {#each pageData.handlers as handler (handler)}
        <code>{handler}</code>
      {/each}
    {/if}
  </div>
</div>

<style>
  .cron-header {
    margin-bottom: 1.5rem;
  }
  .cron-header h1 {
    margin: 0;
  }
  .cron-subtitle {
    margin: 0.25rem 0 0;
    color: #666;
    font-size: 0.9rem;
  }
  .cron-empty-hint {
    font-size: 0.85rem;
    margin-top: 0.5rem;
  }

  .cron-tasks {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .cron-task-card {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
    padding: 0.75rem 1rem;
    border: 1px solid #e0e0e0;
    border-radius: 6px;
    background: #fff;
    transition: border-color 0.15s;
  }
  .cron-task-card:hover {
    border-color: #aaa;
  }
  .cron-task-card.disabled {
    opacity: 0.55;
  }

  .task-main {
    min-width: 0;
    flex: 1;
  }
  .task-name-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .task-name {
    font-weight: 600;
    font-size: 1rem;
    text-decoration: none;
    color: #1a73e8;
  }
  .task-name:hover {
    text-decoration: underline;
  }
  .task-meta {
    display: flex;
    gap: 1rem;
    margin-top: 0.2rem;
    font-size: 0.82rem;
    color: #666;
  }
  .task-handler code {
    font-size: 0.8rem;
    background: #f0f0f0;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
  }

  .task-status {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 0.4rem;
    flex-shrink: 0;
  }
  .task-timing {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.85rem;
    white-space: nowrap;
  }
  .next-run {
    font-variant-numeric: tabular-nums;
    min-width: 5em;
    text-align: right;
  }

  .task-actions {
    display: flex;
    gap: 0.35rem;
  }

  .btn-toggle:not(.btn-on) {
    color: #888;
  }

  .cron-handlers {
    margin-top: 1.5rem;
    font-size: 0.8rem;
    color: #888;
    display: flex;
    align-items: baseline;
    flex-wrap: wrap;
    gap: 0.4rem;
  }
  .cron-handlers code {
    font-size: 0.75rem;
    background: #f0f0f0;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
  }
  .cron-handlers-empty {
    font-style: italic;
  }
</style>
