// Shared constants for the documentation-screenshot harness.
//
// One throwaway `uv run datasette` server, booted on a fixed port with a fresh
// internal DB and a fresh (empty) data DB. See server.mjs.

import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));

// Port is fixed but overridable; 8492 is free in the datasette-* family
// (paper=8486, sheets=8487, town=8489, skill-default=8490, places=8491,
// alerts=8493).
export const PORT = Number(process.env.SHOTS_PORT) || 8492;
export const BASE = `http://localhost:${PORT}`;
export const CRON = `${BASE}/-/cron`;

// Throwaway databases. The internal DB holds datasette_cron_tasks/_runs;
// data.db is an empty user DB (datasette wants at least one mutable DB
// attached). Both are recreated from scratch on every run.
export const INTERNAL_DB = "/tmp/datasette-cron-shots-internal.db";
export const DATA_DIR = "/tmp/datasette-cron-shots-data";
export const DATA_DB = `${DATA_DIR}/data.db`;

// Dev-only seed plugin (handlers + demo tasks + run history), loaded via
// --plugins-dir. The module filename (datasette_demo.py) determines the
// handler prefix: "datasette_demo" -> "demo:".
export const PLUGINS_DIR = resolve(HERE, "../shot-plugins");

// Task names seeded by shot-plugins/datasette_demo.py.
export const TASKS = {
  refreshFeeds: "demo:refresh-feeds",
  nightlyReport: "demo:nightly-report",
  flakyImport: "demo:flaky-import",
  weeklyDigest: "demo:weekly-digest",
};

// Output: PNGs committed under docs/screenshots, named after the shot.
export const OUT = resolve(HERE, "../../../docs/screenshots");
export const out = (name) => resolve(OUT, `${name}.png`);

// Default capture geometry. deviceScaleFactor:2 → crisp retina PNGs.
export const VIEWPORT = { width: 1100, height: 820 };
export const DEVICE_SCALE_FACTOR = 2;

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
