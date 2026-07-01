#!/usr/bin/env node
// Documentation-screenshot harness for datasette-cron.
//
//   node frontend/scripts/screenshots.mjs               # all shots
//   node frontend/scripts/screenshots.mjs index detail  # a subset
//
// SELF-CONTAINED: boots a throwaway `uv run datasette` on a fixed port with a
// fresh internal DB, seeds deterministic demo tasks + run history via the
// dev-only plugin in shot-plugins/ (there is no HTTP task-create API), drives
// headless Chromium with a stability stylesheet, and writes deterministic
// PNGs to docs/screenshots/. Run via `just shots` (which builds the frontend
// first so shots reflect current code).
//
// The harness lives in shots/ (+ the seed plugin in shot-plugins/):
//   * shots/config.mjs        — port/DB/output constants + out(name)
//   * shots/server.mjs        — boot/teardown of the throwaway datasette
//   * shots/helpers.mjs       — STABILITY_CSS / freezeVolatile
//   * shots/defineShot.mjs    — per-shot new-page → freeze → capture → close
//   * shots/defs/<name>.mjs   — ONE FILE PER SHOT, auto-discovered below.
//                               Adding a screenshot is a single new file here.
//   * shot-plugins/datasette_demo.py — demo handlers + tasks + run history

import { chromium } from "@playwright/test";
import { readdirSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { startServer, stopServer } from "./shots/server.mjs";
import { OUT } from "./shots/config.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const DEFS_DIR = resolve(HERE, "shots/defs");

// Auto-discover shots: every shots/defs/<name>.mjs default-exports a descriptor
// whose `name` must equal its filename.
async function loadShots() {
  const shots = new Map();
  for (const file of readdirSync(DEFS_DIR).sort()) {
    if (!file.endsWith(".mjs")) continue;
    const expected = file.slice(0, -4);
    const mod = await import(pathToFileURL(resolve(DEFS_DIR, file)).href);
    const shot = mod.default;
    if (!shot?.name) throw new Error(`${file}: missing default-exported shot`);
    if (shot.name !== expected) {
      throw new Error(`${file}: shot name "${shot.name}" must match filename`);
    }
    shots.set(shot.name, shot);
  }
  return shots;
}

async function main() {
  const requested = process.argv.slice(2);
  const shots = await loadShots();

  const unknown = requested.filter((n) => !shots.has(n));
  if (unknown.length) {
    console.error(`Unknown shot(s): ${unknown.join(", ")}`);
    console.error(`Available: ${[...shots.keys()].join(", ")}`);
    process.exit(1);
  }
  const toRun = requested.length ? requested : [...shots.keys()];

  await mkdir(OUT, { recursive: true });

  let server;
  let browser;
  const cleanup = () => {
    stopServer(server);
  };
  process.on("SIGINT", () => (cleanup(), process.exit(130)));
  process.on("SIGTERM", () => (cleanup(), process.exit(143)));

  try {
    server = await startServer();
    browser = await chromium.launch();

    for (const name of toRun) {
      const path = await shots.get(name).run(browser);
      console.log(`✓ ${name} → ${path}`);
    }
  } finally {
    if (browser) await browser.close();
    cleanup();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
