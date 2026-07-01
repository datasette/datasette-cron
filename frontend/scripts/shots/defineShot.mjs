// Turn a declarative shot descriptor into a runner function.
//
// Each defs/<name>.mjs default-exports defineShot({...}); the filename must
// equal `name` (asserted by screenshots.mjs — no central registry).
//
// Descriptor fields:
//   name      required, must match the filename
//   viewport  context viewport (default VIEWPORT)
//   url       string — where to navigate
//   prepare   async (page, { ctx }) => void — wait for readiness, interact
//   capture   async (page, path) => void — defaults to a full-page screenshot
//             cropped at datasette's footer (short pages otherwise pad the
//             PNG with viewport-height whitespace)

import { makeContext, freezeVolatile } from "./helpers.mjs";
import { out } from "./config.mjs";

async function captureToFooter(page, path) {
  const height = await page.evaluate(() => {
    const footer = document.querySelector("footer.ft");
    if (!footer) return document.documentElement.scrollHeight;
    return Math.ceil(footer.getBoundingClientRect().bottom + window.scrollY);
  });
  const { width } = page.viewportSize();
  await page.screenshot({ path, clip: { x: 0, y: 0, width, height } });
}

export function defineShot(descriptor) {
  const { name } = descriptor;
  if (!name) throw new Error("defineShot: `name` is required");
  return {
    name,
    async run(browser) {
      const ctx = await makeContext(browser, {
        viewport: descriptor.viewport,
      });
      try {
        const page = await ctx.newPage();
        // domcontentloaded (not networkidle) — readiness is asserted
        // explicitly in each shot's prepare() with settled-state waits.
        await page.goto(descriptor.url, { waitUntil: "domcontentloaded" });
        if (descriptor.prepare) await descriptor.prepare(page, { ctx });
        // Freeze immediately before capture: the scheduler is live during
        // the run, so volatile text must be pinned as late as possible.
        await freezeVolatile(page);
        const path = out(name);
        if (descriptor.capture) {
          await descriptor.capture(page, path);
        } else {
          await captureToFooter(page, path);
        }
        return path;
      } finally {
        await ctx.close();
      }
    },
  };
}
