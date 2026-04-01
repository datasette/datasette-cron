<script lang="ts">
  import type { DetailPageData } from "../../page_data/DetailPageData.types.ts";
  import { loadPageData } from "../../page_data/load.ts";
  import { appState } from "../../store.svelte.ts";
  import createClient from "openapi-fetch";
  import type { paths } from "../../../api.d.ts";

  const pageData = loadPageData<DetailPageData>();
  const client = createClient<paths>({ baseUrl: "/" });

  let task = $state(pageData.task);
  let runs = $state(pageData.runs);
  let now = $state(Date.now());

  const continuous = $derived(
    task.schedule_type === "interval"
      && task.schedule_description.match(/^every [0-9]+s$/) !== null
      && parseInt(task.schedule_description.replace(/\D/g, "")) < 10
  );

  // Tick every 5 seconds
  $effect(() => {
    const id = setInterval(() => { now = Date.now(); }, 5000);
    return () => clearInterval(id);
  });

  // Auto-refresh when countdown crosses zero (non-continuous only)
  $effect(() => {
    const _t = now;
    if (continuous || !task.next_run_at || !task.enabled) return;
    const diff = new Date(task.next_run_at + "Z").getTime() - _t;
    if (diff < 0 && diff > -5500) {
      refreshTask();
    }
  });

  async function refreshTask() {
    const { data } = await client.GET("/-/api/cron/tasks/{task_name}", {
      params: { path: { task_name: task.name } },
    });
    if (data) {
      task = {
        name: data.name as string,
        handler: data.handler as string,
        schedule_type: data.schedule_type as string,
        schedule_description: data.schedule_description as string,
        timezone: data.timezone as string | null,
        enabled: data.enabled as boolean,
        next_run_at: data.next_run_at as string | null,
        last_run_at: data.last_run_at as string | null,
        last_status: data.last_status as string | null,
      };
    }
    // Also refresh runs
    const runsResp = await client.GET("/-/api/cron/tasks/{task_name}/runs", {
      params: { path: { task_name: task.name } },
    });
    if (runsResp.data) {
      runs = runsResp.data.runs as typeof runs;
    }
  }

  async function triggerTask() {
    await client.POST("/-/api/cron/tasks/{task_name}/trigger", {
      params: { path: { task_name: task.name } },
      body: {},
    });
    setTimeout(() => refreshTask(), 500);
  }

  async function toggleTask() {
    const { data } = await client.POST("/-/api/cron/tasks/{task_name}/enable", {
      params: { path: { task_name: task.name } },
      body: { enabled: !task.enabled },
    });
    if (data?.ok) {
      task = { ...task, enabled: data.enabled };
    }
  }

  function countdown(iso: string | null): { text: string; className: string } {
    if (!iso) return { text: "—", className: "" };
    const diff = (new Date(iso + "Z").getTime() - now) / 1000;
    const abs = Math.abs(diff);
    const past = diff < 0;
    let label: string;
    if (abs < 120) {
      const s = Math.round(abs / 5) * 5;
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
      <button class="btn" onclick={triggerTask}>Run now</button>
      <button
        class="btn btn-toggle"
        class:btn-on={task.enabled}
        onclick={toggleTask}
      >
        {task.enabled ? "Disable" : "Enable"}
      </button>
    </div>
  </div>

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
        {@const next = countdown(task.next_run_at)}
        <div class="card-value {next.className}" title={task.next_run_at ?? ""}>{next.text}</div>
      {/if}
    </div>
    <div class="detail-card">
      <div class="card-label">Last Run</div>
      <div class="card-value" title={task.last_run_at ?? ""}>
        {#if task.last_status}
          <span class="status-dot status-{task.last_status}"></span>
        {/if}
        {task.last_run_at ? countdown(task.last_run_at).text : "never"}
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
        {#each runs as run}
          {@const started = countdown(run.started_at)}
          <tr class="run-row run-{run.status}">
            <td title={run.started_at}>{started.text}</td>
            <td>
              <span class="status-dot status-{run.status}"></span>
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
  .cron-page {
    max-width: 900px;
  }
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
  .time-past { color: #888; }
  .time-future { color: #1a73e8; }
  .time-continuous { color: #34a853; font-style: italic; }

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

  .status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
  }
  .status-success { background: #34a853; }
  .status-error { background: #ea4335; }
  .status-running { background: #fbbc04; }

  .badge {
    font-size: 0.7rem;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    text-transform: uppercase;
    font-weight: 600;
    letter-spacing: 0.03em;
  }
  .badge-disabled { background: #f0f0f0; color: #888; }
  .badge-enabled { background: #e8f5e9; color: #2e7d32; }

  .cron-empty {
    padding: 2rem;
    text-align: center;
    border: 1px dashed #ccc;
    border-radius: 8px;
    color: #666;
  }

  .btn {
    cursor: pointer;
    border: 1px solid #ccc;
    background: #fff;
    border-radius: 4px;
    padding: 0.35rem 0.75rem;
    font-size: 0.85rem;
    transition: background 0.1s, border-color 0.1s;
  }
  .btn:hover { background: #f5f5f5; border-color: #aaa; }
  .btn-toggle.btn-on {
    background: #e8f5e9;
    border-color: #a5d6a7;
    color: #2e7d32;
  }
</style>
