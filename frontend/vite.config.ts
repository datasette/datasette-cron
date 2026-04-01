import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { readFileSync, writeFileSync, globSync } from "fs";
import { compile } from "json-schema-to-typescript";

const DEV_PORT = 5180;

export default defineConfig({
  server: {
    port: DEV_PORT,
    strictPort: true,
    cors: true,
    hmr: { host: "localhost", port: DEV_PORT, protocol: "ws" },
  },
  plugins: [
    svelte(),
    {
      name: "page-data-types",
      async buildStart() {
        const files = globSync("src/page_data/*_schema.json");
        for (const file of files) {
          const schema = JSON.parse(readFileSync(file, "utf-8"));
          const ts = await compile(schema, "");
          const outFile = file.replace("_schema.json", ".types.ts");
          writeFileSync(outFile, ts);
        }
      },
      async handleHotUpdate({ file, server }) {
        if (!file.endsWith("_schema.json")) return;
        const schema = JSON.parse(readFileSync(file, "utf-8"));
        const ts = await compile(schema, "");
        const outFile = file.replace("_schema.json", ".types.ts");
        writeFileSync(outFile, ts);
        const mod = server.moduleGraph.getModuleById(outFile);
        if (mod) {
          server.moduleGraph.invalidateModule(mod);
          return [mod];
        }
      },
    },
  ],
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
