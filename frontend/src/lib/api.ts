import createClient from "openapi-fetch";
import type { paths } from "../../api.d.ts";

/** Shared typed client for the cron JSON API. */
export const client = createClient<paths>({ baseUrl: "/" });

/**
 * Normalize openapi-fetch results and thrown network errors into
 * { data } | { errorMessage } so callers can't silently ignore failures.
 */
export async function api<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<{ data?: T; errorMessage?: string }> {
  try {
    const { data, error, response } = await promise;
    if (error !== undefined || !response.ok) {
      const err = error as { message?: string; error?: string } | undefined;
      return {
        errorMessage: err?.message ?? err?.error ?? `HTTP ${response.status}`,
      };
    }
    return { data };
  } catch (e) {
    return { errorMessage: e instanceof Error ? e.message : String(e) };
  }
}

/**
 * Auto-refresh failure backoff: pause polling after a failure (don't hammer
 * a dead server), doubling the pause up to 60s; reset on any success or
 * successful user action.
 */
export class PollBackoff {
  private failures = 0;
  pausedUntil = 0;

  fail(): void {
    this.failures++;
    this.pausedUntil =
      Date.now() + Math.min(60_000, 5000 * 2 ** (this.failures - 1));
  }

  reset(): void {
    this.failures = 0;
    this.pausedUntil = 0;
  }
}
