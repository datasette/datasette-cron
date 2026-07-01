// The task detail page for the flaky hourly import: schedule/next-run/
// last-run cards and a run history with error rows (messages shown), retry
// attempts 1-3, durations, a recovered retry and an abandoned run.

import { defineShot } from "../defineShot.mjs";
import { CRON, TASKS } from "../config.mjs";

export default defineShot({
  name: "detail",
  // Task names contain ":" (the plugin-handler prefix). ":" is legal raw in
  // a path segment and MUST stay raw: the route regexes match the encoded
  // path without unquoting url_vars, so percent-encoding it 404s (verified
  // empirically — this matches the index page's own task links).
  url: `${CRON}/${TASKS.flakyImport}`,
  async prepare(page) {
    // All seven seeded runs for this task rendered…
    await page
      .locator(".runs-table tbody tr")
      .nth(6)
      .waitFor({ timeout: 15000 });
    // …including an error row with its message and the abandoned row.
    await page
      .locator(".error-cell", { hasText: "ConnectionError" })
      .first()
      .waitFor({ timeout: 15000 });
    await page
      .locator(".run-abandoned", { hasText: "abandoned" })
      .waitFor({ timeout: 15000 });
  },
});
