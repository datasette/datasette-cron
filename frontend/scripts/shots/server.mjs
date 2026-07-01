// Boot / tear down the throwaway datasette the screenshots run against.
//
// datasette is launched as a grandchild of `uv run`, so teardown kills the
// whole process group (a plain child.kill would orphan datasette holding the
// port). Refuses to start over an already-listening server rather than produce
// garbage shots against whatever is there.
//
// Seeding: there is no HTTP task-create API — tasks are registered by plugins
// via scheduler.add_task(). The dev-only plugin in ../shot-plugins/ (loaded
// with --plugins-dir) registers demo handlers, creates four demo tasks and
// inserts a fixed run history, all in its startup hook. No cookie machinery:
// the permission gate is opened for the anonymous actor with a -s setting.
//
// NOTE: the cron scheduler loop starts on the FIRST request — which is this
// file's readiness poll — so tasks genuinely tick while shots run. The seeded
// schedules all have their next fire >= 5 minutes out, so nothing executes
// mid-shoot and the seeded run history stays exactly as written.

import { spawn, execFileSync } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";

import {
  PORT,
  CRON,
  INTERNAL_DB,
  DATA_DIR,
  DATA_DB,
  PLUGINS_DIR,
  sleep,
} from "./config.mjs";

// Is something already answering on our port? (status < 500 = "alive").
async function reachable() {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 500);
  try {
    const resp = await fetch(CRON, { signal: ctrl.signal });
    return resp.status < 500;
  } catch {
    return false;
  } finally {
    clearTimeout(t);
  }
}

// Fresh empty data.db — datasette needs one attached mutable DB; the demo
// tasks themselves live in the internal DB.
function setupDataDb() {
  rmSync(DATA_DIR, { recursive: true, force: true });
  mkdirSync(DATA_DIR, { recursive: true });
  execFileSync("uv", [
    "run",
    "python",
    "-c",
    "import sqlite3, sys; sqlite3.connect(sys.argv[1]).execute('vacuum')",
    DATA_DB,
  ]);
}

export async function startServer() {
  if (await reachable()) {
    throw new Error(
      `Something is already serving on ${PORT}. Stop it first (the harness ` +
        `won't screenshot an unknown server).`,
    );
  }

  rmSync(INTERNAL_DB, { force: true });
  setupDataDb();

  const child = spawn(
    "uv",
    [
      "run",
      "datasette",
      "--internal",
      INTERNAL_DB,
      DATA_DB,
      "--plugins-dir",
      PLUGINS_DIR,
      // Let the anonymous actor reach the cron pages + API.
      "-s",
      "permissions.datasette-cron-access",
      "true",
      "-p",
      String(PORT),
    ],
    {
      stdio: ["ignore", "pipe", "pipe"],
      detached: true, // own process group, so we can kill the whole tree
      // PYTHONHASHSEED=0 → any hash-derived ordering stays stable.
      env: { ...process.env, PYTHONHASHSEED: "0" },
    },
  );

  let logs = "";
  child.stdout.on("data", (d) => (logs += d));
  child.stderr.on("data", (d) => (logs += d));

  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(
        `datasette exited early (code ${child.exitCode}):\n${logs}`,
      );
    }
    if (await reachable()) return child;
    await sleep(250);
  }
  throw new Error(`datasette did not become ready in 30s:\n${logs}`);
}

export function stopServer(child) {
  if (!child) return;
  try {
    process.kill(-child.pid, "SIGKILL"); // negative pid = process group
  } catch {
    try {
      child.kill("SIGKILL");
    } catch {
      /* already gone */
    }
  }
}
