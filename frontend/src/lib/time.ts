/**
 * Parse a UTC timestamp string from the backend into a Date, or null if it
 * cannot be parsed.
 *
 * Backend timestamps are naive UTC in two flavors: SQLite's
 * strftime('%Y-%m-%dT%H:%M:%f') writes 3 fractional digits, while the
 * scheduler writes next_run_at via Python datetime.isoformat() with up to 6
 * (e.g. "2026-06-19T08:00:00.123456"). The ECMAScript date-time string
 * format only specifies millisecond precision and space separators are not
 * valid at all — stricter engines return Invalid Date, and 6-digit
 * fractions are implementation-defined. Normalize to a strict
 * "YYYY-MM-DDTHH:MM:SS[.SSS]Z" form before parsing and never throw.
 */
export function parseUtc(ts: string): Date | null {
  const normalized =
    ts.replace(" ", "T").replace(/(\.\d{3})\d+/, "$1") +
    (ts.endsWith("Z") ? "" : "Z");
  const d = new Date(normalized);
  return isNaN(d.getTime()) ? null : d;
}

/**
 * True when `iso` is just past due (less than 5.5s before `now`) — the
 * window where the scheduler should be firing the task and the pages poll
 * for fresh state.
 */
export function recentlyDue(iso: string, now: number): boolean {
  const parsed = parseUtc(iso);
  if (!parsed) return false;
  const diff = parsed.getTime() - now;
  return diff < 0 && diff > -5500;
}

/**
 * Render a timestamp relative to `now` (epoch millis) as a short countdown
 * ("in 45s", "3h 10m ago") plus the time-* class for coloring it.
 */
export function countdown(
  iso: string | null,
  now: number,
): { text: string; className: string } {
  const parsed = iso ? parseUtc(iso) : null;
  if (!parsed) return { text: "—", className: "" };
  const diff = (parsed.getTime() - now) / 1000;
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
