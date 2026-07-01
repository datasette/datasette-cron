import type { paths } from "../../api.d.ts";

/** The GET /-/api/cron/tasks/{task_name} response — the full task shape. */
export type TaskResponse =
  paths["/-/api/cron/tasks/{task_name}"]["get"]["responses"][200]["content"]["application/json"];

/** The task fields the pages keep in state (the page-data TaskSummary shape). */
export interface TaskSummary {
  name: string;
  handler: string;
  schedule_type: string;
  schedule_description: string;
  schedule_seconds: number | null;
  timezone: string | null;
  enabled: boolean;
  next_run_at: string | null;
  last_run_at: string | null;
  last_status: string | null;
}

/**
 * Continuous tasks (interval < 10s) render as "continuous" instead of a
 * countdown and are excluded from due-time polling.
 */
export function isContinuous(
  task: Pick<TaskSummary, "schedule_type" | "schedule_seconds">,
): boolean {
  return (
    task.schedule_type === "interval" &&
    typeof task.schedule_seconds === "number" &&
    task.schedule_seconds < 10
  );
}

/**
 * Narrow a full API task response to the summary fields the pages render.
 * Typed against the generated api.d.ts response shape — no casts.
 */
export function taskToSummary(task: TaskResponse): TaskSummary {
  return {
    name: task.name,
    handler: task.handler,
    schedule_type: task.schedule_type,
    schedule_description: task.schedule_description,
    schedule_seconds: task.schedule_seconds,
    timezone: task.timezone,
    enabled: task.enabled,
    next_run_at: task.next_run_at,
    last_run_at: task.last_run_at,
    last_status: task.last_status,
  };
}
