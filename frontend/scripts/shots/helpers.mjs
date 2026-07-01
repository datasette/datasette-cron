// Page-stabilization + capture helpers shared by the shot definitions.

import { VIEWPORT, DEVICE_SCALE_FACTOR, TASKS } from "./config.mjs";

// Injected on every navigation: kill the caret, transitions and animations,
// so re-runs produce no binary diff.
export const STABILITY_CSS = `
  *, *::before, *::after {
    caret-color: transparent !important;
    transition: none !important;
    animation: none !important;
  }
`;

// Rewrite volatile on-screen text to fixed strings so the pixels don't change
// between runs. Runs just before each capture.
//
// Volatile UI in datasette-cron (all from countdown() in lib/time.ts):
//   * Future countdowns ("in 4m", "in 59m") render next_run_at, which is
//     computed at boot from wall-clock + random jitter (add_jitter in
//     schedules.py) — genuinely different every run, and the cron-schedule
//     tasks ("0 8 * * *") depend on the time of day. Pinned per task name.
//   * Past relative times ("35m ago", "2h 1m ago") render the seeded
//     last_run_at / started_at values. The seed uses coarse fixed offsets
//     (-35m / -2h / -1d …) so these are stable in practice; as a boundary
//     guard we recompute them from the element's raw-timestamp title
//     attribute with floor()-based single-unit buckets ("2h ago", never
//     "2h 1m ago"), which is immune to the few seconds between server boot
//     and capture.
//   * Duration cells ("184ms") are seeded constants — nothing to freeze.
export async function freezeVolatile(page) {
  await page.evaluate((TASKS) => {
    const FUTURE_PIN = {
      [TASKS.refreshFeeds]: "in 5m",
      [TASKS.flakyImport]: "in 1h",
      [TASKS.nightlyReport]: "in 9h",
      [TASKS.weeklyDigest]: "in 2d",
    };

    // Index page: each task card's next-run countdown, pinned by task name.
    document.querySelectorAll(".cron-task-card").forEach((card) => {
      const name = card.querySelector(".task-name")?.textContent?.trim();
      const el = card.querySelector(".next-run");
      if (el && name && FUTURE_PIN[name]) {
        el.textContent = FUTURE_PIN[name];
        el.setAttribute("title", "pinned for screenshot");
      }
    });

    // Detail page: the "Next Run" card, pinned by the h1 task name.
    const h1 = document.querySelector("h1")?.textContent?.trim();
    document.querySelectorAll(".detail-card").forEach((card) => {
      const label = card.querySelector(".card-label")?.textContent?.trim();
      const value = card.querySelector(".card-value");
      if (label === "Next Run" && value && h1 && FUTURE_PIN[h1]) {
        value.textContent = FUTURE_PIN[h1];
        value.setAttribute("title", "pinned for screenshot");
      }
    });

    // Past relative times: recompute from the raw timestamp in the title
    // attribute (fixed by the seed) using floor()-based coarse buckets.
    document.querySelectorAll("[title]").forEach((el) => {
      const iso = el.getAttribute("title") ?? "";
      if (!/^\d{4}-\d{2}-\d{2}T/.test(iso)) return;
      const t = Date.parse(iso.replace(/(\.\d{3})\d*$/, "$1") + "Z");
      const diffS = (Date.now() - t) / 1000;
      if (!Number.isFinite(diffS) || diffS <= 0) return; // future → pinned above
      const text =
        diffS < 3600
          ? `${Math.floor(diffS / 60)}m ago`
          : diffS < 86400
            ? `${Math.floor(diffS / 3600)}h ago`
            : `${Math.floor(diffS / 86400)}d ago`;
      for (const node of el.childNodes) {
        if (
          node.nodeType === Node.TEXT_NODE &&
          /^\d+[smhd](\s\d+[smh])?\sago$/.test(node.textContent?.trim() ?? "")
        ) {
          node.textContent = text;
        }
      }
      el.setAttribute("title", "pinned for screenshot");
    });
  }, TASKS);
}

// New browser context: viewport + retina, stability stylesheet injected on
// initial doc + every navigation. All shots run as the anonymous actor — the
// cron UI has no per-actor chrome, so no ds_actor cookie is needed.
export async function makeContext(browser, { viewport = VIEWPORT } = {}) {
  const ctx = await browser.newContext({
    viewport,
    deviceScaleFactor: DEVICE_SCALE_FACTOR,
  });
  await ctx.addInitScript((css) => {
    const inject = () => {
      const style = document.createElement("style");
      style.textContent = css;
      document.head?.appendChild(style);
    };
    if (document.head) inject();
    document.addEventListener("DOMContentLoaded", inject);
  }, STABILITY_CSS);
  return ctx;
}
