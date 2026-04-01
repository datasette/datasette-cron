<script lang="ts">
  import type { IndexPageData } from "../../page_data/IndexPageData.types.ts";
  import { loadPageData } from "../../page_data/load.ts";
  import { appState } from "../../store.svelte.ts";
  import createClient from "openapi-fetch";
  import type { paths } from "../../../api.d.ts";

  const pageData = loadPageData<IndexPageData>();
  const client = createClient<paths>({ baseUrl: "/" });

  let tasks = $state(pageData.tasks);
  let now = $state(Date.now());

  // Tick every 5 seconds
  $effect(() => {
    const id = setInterval(() => {
      now = Date.now();
    }, 5000);
    return () => clearInterval(id);
  });

  // Check for tasks that just became due and refresh them
  // Skip continuous tasks (interval < 10s) to avoid excessive API hits
  $effect(() => {
    const _t = now;
    for (const task of tasks) {
      if (!task.next_run_at || !task.enabled || isContinuous(task)) continue;
      const diff = new Date(task.next_run_at + "Z").getTime() - _t;
      if (diff < 0 && diff > -5500) {
        refreshTask(task.name);
      }
    }
  });

  async function refreshTask(name: string) {
    const { data } = await client.GET("/-/api/cron/tasks/{task_name}", {
      params: { path: { task_name: name } },
    });
    if (data) {
      tasks = tasks.map((t) => (t.name === name ? taskToSummary(data) : t));
    }
  }

  function taskToSummary(apiTask: Record<string, unknown>) {
    return {
      name: apiTask.name as string,
      handler: apiTask.handler as string,
      schedule_type: apiTask.schedule_type as string,
      schedule_description: apiTask.schedule_description as string,
      timezone: apiTask.timezone as string | null,
      enabled: apiTask.enabled as boolean,
      next_run_at: apiTask.next_run_at as string | null,
      last_run_at: apiTask.last_run_at as string | null,
      last_status: apiTask.last_status as string | null,
    };
  }

  async function triggerTask(name: string) {
    await client.POST("/-/api/cron/tasks/{task_name}/trigger", {
      params: { path: { task_name: name } },
      body: {},
    });
    // Wait a beat then refresh
    setTimeout(() => refreshTask(name), 500);
  }

  async function toggleTask(name: string, currentEnabled: boolean) {
    const { data } = await client.POST("/-/api/cron/tasks/{task_name}/enable", {
      params: { path: { task_name: name } },
      body: { enabled: !currentEnabled },
    });
    if (data?.ok) {
      tasks = tasks.map((t) =>
        t.name === name ? { ...t, enabled: data.enabled } : t,
      );
    }
  }

  function isContinuous(task: (typeof tasks)[number]): boolean {
    return task.schedule_type === "interval" && task.schedule_description.match(/^every [0-9]+s$/) !== null
      && parseInt(task.schedule_description.replace(/\D/g, "")) < 10;
  }

  function countdown(iso: string | null): { text: string; className: string } {
    if (!iso) return { text: "—", className: "" };
    const diff = (new Date(iso + "Z").getTime() - now) / 1000;
    const abs = Math.abs(diff);
    const past = diff < 0;
    let label: string;
    if (abs < 120) {
      // Under 2 minutes: show seconds
      const s = Math.round(abs / 5) * 5; // round to nearest 5
      label = `${s}s`;
    } else if (abs < 3600) {
      label = `${Math.round(abs / 60)}m`;
    } else if (abs < 86400) {
      const h = Math.floor(abs / 3600);
      const m = Math.round((abs % 3600) / 60);
      label = m > 0 ? `${h}h ${m}m` : `${h}h`;
    } else {
      const d = Math.floor(abs / 86400);
      const h = Math.round((abs % 86400) / 3600);
      label = h > 0 ? `${d}d ${h}h` : `${d}d`;
    }
    const text = past ? `${label} ago` : `in ${label}`;
    const className = past ? "time-past" : "time-future";
    return { text, className };
  }
</script>

<div class="cron-page">
  <div class="cron-header">
    <h1>Cron Tasks</h1>
    <p class="cron-subtitle">{tasks.length} registered task{tasks.length !== 1 ? "s" : ""}</p>
  </div>

  {#if tasks.length === 0}
    <div class="cron-empty">
      <p>No scheduled tasks registered.</p>
      <p class="cron-empty-hint">Plugins can register handlers via <code>cron_register_handlers</code> and create tasks via <code>scheduler.add_task()</code>.</p>
    </div>
  {:else}
    <div class="cron-tasks">
      {#each tasks as task (task.name)}
        {@const continuous = isContinuous(task)}
        {@const next = continuous ? { text: "continuous", className: "time-continuous" } : countdown(task.next_run_at)}
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
                <span class="status-dot status-{task.last_status}"></span>
              {/if}
              <span class="next-run {next.className}" title={task.next_run_at ?? ""}>{next.text}</span>
            </div>
            <div class="task-actions">
              <button class="btn btn-sm" onclick={() => triggerTask(task.name)}>Run now</button>
              <button
                class="btn btn-sm btn-toggle"
                class:btn-on={task.enabled}
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
</div>

<style>
  .cron-page {
    max-width: 900px;
  }
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
  .cron-empty {
    padding: 2rem;
    text-align: center;
    border: 1px dashed #ccc;
    border-radius: 8px;
    color: #666;
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
  .time-past { color: #888; }
  .time-future { color: #1a73e8; }
  .time-continuous { color: #34a853; font-style: italic; }

  .status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
  }
  .status-success { background: #34a853; }
  .status-error { background: #ea4335; }
  .status-running { background: #fbbc04; }

  .task-actions {
    display: flex;
    gap: 0.35rem;
  }

  .badge {
    font-size: 0.7rem;
    padding: 0.1rem 0.4rem;
    border-radius: 3px;
    text-transform: uppercase;
    font-weight: 600;
    letter-spacing: 0.03em;
  }
  .badge-disabled {
    background: #f0f0f0;
    color: #888;
  }

  .btn {
    cursor: pointer;
    border: 1px solid #ccc;
    background: #fff;
    border-radius: 4px;
    padding: 0.25rem 0.6rem;
    font-size: 0.8rem;
    transition: background 0.1s, border-color 0.1s;
  }
  .btn:hover {
    background: #f5f5f5;
    border-color: #aaa;
  }
  .btn-sm {
    padding: 0.2rem 0.5rem;
    font-size: 0.78rem;
  }
  .btn-toggle.btn-on {
    background: #e8f5e9;
    border-color: #a5d6a7;
    color: #2e7d32;
  }
  .btn-toggle:not(.btn-on) {
    color: #888;
  }
</style>
