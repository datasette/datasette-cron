import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

const DEV_PORT = 5180;

export default defineConfig({
  server: {
    port: DEV_PORT,
    strictPort: true,
    cors: true,
    hmr: { host: "localhost", port: DEV_PORT, protocol: "ws" },
  },
  plugins: [svelte()],
  build: {
    manifest: "manifest.json",
    outDir: "../datasette_cron",
    assetsDir: "static/gen",
    rollupOptions: {
      input: {
        index: "src/pages/index/index.ts",
        detail: "src/pages/detail/index.ts",
      },
    },
  },
});
