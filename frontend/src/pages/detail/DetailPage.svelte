<script lang="ts">
  import type { DetailPageData } from "../../page_data/DetailPageData.types.ts";
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

  const pageData = loadPageData<DetailPageData>();

  let task = $state<TaskSummary>(pageData.task);
  let runs = $state(pageData.runs);
  let now = $state(Date.now());
  let errorMessage = $state<string | null>(null);
  // Busy flags so action buttons are disabled while their POST is in flight
  // (a double-clicked "Run now" would force-run twice).
  let busyTrigger = $state(false);
  let busyToggle = $state(false);

  const continuous = $derived(isContinuous(task));

  // Refresh bookkeeping. Deliberately non-reactive: the poller reads state
  // inside a setInterval callback (async, so untracked), which means
  // reassigning `task`/`runs` can never re-trigger the poller — the old
  // $effect-based refresh re-ran itself on every response and produced an
  // unthrottled request loop (refetching the whole runs list each time)
  // while the task was due.
  let refreshing = false;
  let nextPollAt = 0;
  // After "Run now": poll until last_run_at advances (run completed), capped.
  let pendingTrigger: { until: number; lastRunAt: string | null } | null =
    null;
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
    const id = setInterval(() => { now = Date.now(); }, 5000);
    return () => clearInterval(id);
  });

  // Poll once a second; refresh at most once per 1-2s while the task is
  // due, recently triggered, or has a run in flight (non-continuous only).
  $effect(() => {
    const id = setInterval(poll, 1000);
    return () => clearInterval(id);
  });

  function shouldPoll(t: number): boolean {
    if (pendingTrigger && t < pendingTrigger.until) return true;
    if (runs.some((r) => r.status === "running")) return true;
    if (continuous || !task.next_run_at || !task.enabled) return false;
    return recentlyDue(task.next_run_at, t);
  }

  function poll() {
    const t = Date.now();
    if (refreshing || t < nextPollAt || t < backoff.pausedUntil) return;
    if (!shouldPoll(t)) return;
    refreshTask();
  }

  async function refreshTask() {
    if (refreshing) return;
    refreshing = true;
    const startedMutation = mutationCount;
    try {
      const { data, errorMessage: taskErr } = await api(
        client.GET("/-/api/cron/tasks/{task_name}", {
          params: { path: { task_name: task.name } },
        }),
      );
      const runsResp = await api(
        client.GET("/-/api/cron/tasks/{task_name}/runs", {
          params: { path: { task_name: task.name } },
        }),
      );
      const err = taskErr ?? runsResp.errorMessage;
      if (err !== undefined) {
        errorMessage = `Failed to refresh "${task.name}": ${err}`;
        backoff.fail();
        return;
      }
      backoff.reset();
      // Discard responses that raced a user action
      if (startedMutation !== mutationCount) return;
      if (data) {
        const updated = taskToSummary(data);
        // Backoff: if the server still reports the same state (e.g. due but
        // the scheduler hasn't advanced next_run_at yet), wait 2s before
        // the next attempt; otherwise 1s.
        const unchanged =
          task.next_run_at === updated.next_run_at &&
          task.last_run_at === updated.last_run_at &&
          task.last_status === updated.last_status;
        nextPollAt = Date.now() + (unchanged ? 2000 : 1000);
        if (pendingTrigger && updated.last_run_at !== pendingTrigger.lastRunAt) {
          pendingTrigger = null;
        }
        task = updated;
      }
      if (runsResp.data) {
        runs = runsResp.data.runs;
      }
    } finally {
      refreshing = false;
    }
  }

  async function triggerTask() {
    if (busyTrigger) return;
    busyTrigger = true;
    mutationCount++;
    // Keep polling until the run completes (last_run_at advances), capped at
    // 2 minutes, so the UI converges even for long-running manual triggers.
    pendingTrigger = {
      until: Date.now() + 120_000,
      lastRunAt: task.last_run_at,
    };
    try {
      const { data, errorMessage: err } = await api(
        client.POST("/-/api/cron/tasks/{task_name}/trigger", {
          params: { path: { task_name: task.name } },
          // No body, but Datasette's CSRF protection only skips requests
          // that declare a JSON content type.
          headers: { "Content-Type": "application/json" },
        }),
      );
      if (err !== undefined || !data?.ok) {
        pendingTrigger = null;
        errorMessage = `Failed to trigger "${task.name}": ${err ?? data?.message ?? "unknown error"}`;
        return;
      }
      pollingRecovered();
      nextPollAt = 0;
    } finally {
      busyTrigger = false;
    }
  }

  async function toggleTask() {
    if (busyToggle) return;
    busyToggle = true;
    mutationCount++; // invalidate in-flight refreshes
    const previous = task.enabled;
    const wanted = !previous;
    // Optimistic update; the response (or a later refresh) settles it.
    task = { ...task, enabled: wanted };
    try {
      const { data, errorMessage: err } = await api(
        client.POST("/-/api/cron/tasks/{task_name}/enable", {
          params: { path: { task_name: task.name } },
          body: { enabled: wanted },
        }),
      );
      if (err !== undefined || !data?.ok) {
        // Revert the optimistic update
        task = { ...task, enabled: previous };
        errorMessage = `Failed to ${wanted ? "enable" : "disable"} "${task.name}": ${err ?? "unknown error"}`;
        return;
      }
      task = { ...task, enabled: data.enabled };
      pollingRecovered();
    } finally {
      busyToggle = false;
    }
  }

  function formatDuration(ms: number | null): string {
    if (ms === null) return "—";
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
  }
</script>

<div class="cron-page">
  <a href={appState.basePath} class="back-link">&larr; All Tasks</a>

  <div class="detail-header">
    <div class="detail-title-row">
      <h1>{task.name}</h1>
      {#if !task.enabled}
        <span class="badge badge-disabled">disabled</span>
      {:else}
        <span class="badge badge-enabled">enabled</span>
      {/if}
    </div>
    <div class="detail-actions">
      <button class="btn" disabled={busyTrigger} onclick={triggerTask}>Run now</button>
      <button
        class="btn btn-toggle"
        class:btn-on={task.enabled}
        aria-pressed={task.enabled}
        disabled={busyToggle}
        onclick={toggleTask}
      >
        {task.enabled ? "Disable" : "Enable"}
      </button>
    </div>
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

  <div class="detail-grid">
    <div class="detail-card">
      <div class="card-label">Handler</div>
      <div class="card-value"><code>{task.handler}</code></div>
    </div>
    <div class="detail-card">
      <div class="card-label">Schedule</div>
      <div class="card-value">{task.schedule_description}</div>
    </div>
    <div class="detail-card">
      <div class="card-label">Next Run</div>
      {#if continuous}
        <div class="card-value time-continuous">continuous</div>
      {:else}
        {@const next = countdown(task.next_run_at, now)}
        <div class="card-value {next.className}" title={task.next_run_at ?? ""}>{next.text}</div>
      {/if}
    </div>
    <div class="detail-card">
      <div class="card-label">Last Run</div>
      <div class="card-value" title={task.last_run_at ?? ""}>
        {#if task.last_status}
          <StatusDot
            status={task.last_status}
            label="Last run status: {task.last_status}"
          />
        {/if}
        {task.last_run_at ? countdown(task.last_run_at, now).text : "never"}
      </div>
    </div>
    {#if task.timezone}
      <div class="detail-card">
        <div class="card-label">Timezone</div>
        <div class="card-value">{task.timezone}</div>
      </div>
    {/if}
  </div>

  <h2>Run History</h2>

  {#if runs.length === 0}
    <div class="cron-empty">No runs yet.</div>
  {:else}
    <table class="runs-table">
      <thead>
        <tr>
          <th>When</th>
          <th>Status</th>
          <th>Duration</th>
          <th>Attempt</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody>
        {#each runs as run (run.id)}
          {@const started = countdown(run.started_at, now)}
          <tr class="run-row run-{run.status}">
            <td title={run.started_at}>{started.text}</td>
            <td>
              <!-- decorative: the status text follows -->
              <StatusDot status={run.status} />
              {run.status}
            </td>
            <td class="mono">{formatDuration(run.duration_ms)}</td>
            <td>{run.attempt}</td>
            <td class="error-cell">{run.error_message ?? ""}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</div>

<style>
  .back-link {
    display: inline-block;
    margin-bottom: 0.75rem;
    font-size: 0.85rem;
    color: #666;
    text-decoration: none;
  }
  .back-link:hover { color: #1a73e8; }

  .detail-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 1.5rem;
  }
  .detail-title-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }
  .detail-title-row h1 { margin: 0; }
  .detail-actions {
    display: flex;
    gap: 0.5rem;
  }

  .detail-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    gap: 0.75rem;
    margin-bottom: 2rem;
  }
  .detail-card {
    padding: 0.75rem 1rem;
    border: 1px solid #e0e0e0;
    border-radius: 6px;
    background: #fafafa;
  }
  .card-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: #888;
    margin-bottom: 0.25rem;
  }
  .card-value {
    font-size: 0.95rem;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }
  .card-value code {
    font-size: 0.85rem;
    background: #eee;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
  }

  .runs-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88rem;
  }
  .runs-table th {
    text-align: left;
    padding: 0.5rem 0.75rem;
    border-bottom: 2px solid #e0e0e0;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: #666;
  }
  .runs-table td {
    padding: 0.4rem 0.75rem;
    border-bottom: 1px solid #f0f0f0;
  }
  .run-row:hover {
    background: #f8f8f8;
  }
  .run-error td { color: #c62828; }
  .mono { font-variant-numeric: tabular-nums; font-family: monospace; font-size: 0.82rem; }
  .error-cell {
    font-size: 0.82rem;
    color: #c62828;
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
