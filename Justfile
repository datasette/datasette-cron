# === Type Generation ===

# Every recipe here ends by running prettier over what it just wrote.
# The generators emit their own house style (openapi-typescript indents with
# four spaces, json.dumps wraps arrays and omits the trailing newline) while
# `just format` runs prettier over the whole frontend. Without this pass the
# two overwrite each other and `types-check-fresh` and `format-check` can
# never both be green.
#
# The pass runs from inside frontend/, the same cwd as the format/format:check
# npm scripts. Prettier resolves config from each file's directory upward, but
# resolves plugins and ignore files relative to cwd -- from the repo root it
# cannot find anything in frontend/node_modules.

types-routes:
  uv run python -c 'from datasette_cron.router import router; import json; print(json.dumps(router.openapi_document_json()))' \
    | npx --prefix frontend openapi-typescript > frontend/api.d.ts
  cd frontend && npx prettier --write --log-level warn api.d.ts

types-pagedata:
  uv run scripts/typegen-pagedata.py
  for f in frontend/src/page_data/*_schema.json; do npx --prefix frontend json2ts "$f" > "${f%_schema.json}.types.ts"; done
  cd frontend && npx prettier --write --log-level warn src/page_data/

types:
  just types-routes
  just types-pagedata

types-watch:
  watchexec -e py --clear -- just types

# Regenerate generated types and fail if the working tree diverges from
# what's committed. Used by CI to catch "I forgot to run `just types`."
types-check-fresh:
  just types
  git diff --exit-code -- frontend/api.d.ts frontend/src/page_data/

# === Frontend ===

frontend *flags:
  npm run build --prefix frontend {{flags}}

frontend-dev *flags:
  npm run dev --prefix frontend -- --port 5180 {{flags}}

# Regenerate committed doc screenshots in docs/screenshots/. Self-contained:
# boots a throwaway datasette on :8492, seeds demo tasks + run history via a
# dev-only plugin, shoots, tears down. Builds the frontend first so shots
# reflect current code. Pass shot names for a subset, e.g. `just shots index`.
shots *names:
  just frontend
  node frontend/scripts/screenshots.mjs {{names}}

# === Formatting ===

format-backend *flags:
  uv run ruff check --fix --quiet
  uv run ruff format {{flags}}

format-backend-check *flags:
  uv run ruff format --check {{flags}}

format-frontend *flags:
  npm run format --prefix frontend {{flags}}

format-frontend-check *flags:
  npm run format:check --prefix frontend {{flags}}

format:
  just format-backend
  just format-frontend

format-check:
  just format-backend-check
  just format-frontend-check

# === Type Checking ===

check-backend:
  uvx ty check
  uv run ruff check

check-frontend:
  npm run check --prefix frontend

check:
  just check-backend
  just check-frontend

# === Testing ===

test *flags:
  uv run pytest {{flags}}

# === Build ===

clean:
  rm -rf build/ dist/ *.egg-info datasette_cron/__pycache__ tests/__pycache__

build: clean
  uv build

# === Development ===

dev *flags:
  mkdir -p .tmp
  DATASETTE_SECRET=abc123 uv run datasette \
    -s permissions.datasette-cron-access true \
    -s permissions.permissions-debug true \
    --internal .tmp/internal.db \
    -p 8010 \
    .tmp/tmp.db \
    --plugins-dir samples \
    {{flags}}

# Like `just dev`, with the sibling datasette-otel-viewer plugin loaded so
# this plugin's spans and metrics can be browsed in-instance at /-/otel
# (traces + metrics). Rows land in .tmp/otel.db.
#
# The viewer self-records: it installs its own TracerProvider/MeterProvider
# at import time, so no `opentelemetry-instrument` and no OTEL_* env vars
# here — a provider installed before the plugin imports wins the race and
# disables the viewer's self-recording. It stays on a `--with ../` sibling
# path because it is not on PyPI yet (same reasoning as datasette-paper's
# Justfile); once published, move it to the `dev` group and drop the flag.
#
# `--no-sources` + the second `--with` pin datasette to the same rev this
# branch's [tool.uv.sources] points at (whose add_background_task the
# scheduler needs, and whose telemetry kit this branch is built on).
# Without them the viewer's own [tool.uv.sources] datasette pin (an older
# GitHub rev) conflicts with ours and resolution fails. Keep this rev in
# sync with the one in pyproject.toml.
dev-otel *flags:
  mkdir -p .tmp
  DATASETTE_SECRET=abc123 uv run \
    --no-sources \
    --no-cache \
    --with ../datasette-otel-viewer \
    --with 'datasette @ git+https://github.com/simonw/datasette@4579a19520b502e90662a79ebd84555a87542340' \
    datasette \
    -s permissions.datasette-cron-access true \
    -s permissions.permissions-debug true \
    -s permissions.datasette-otel-viewer true \
    -s plugins.datasette-otel-viewer.db_path .tmp/otel.db \
    -s plugins.datasette-otel-viewer.service_name datasette-cron \
    --internal .tmp/internal.db \
    -p 8010 \
    .tmp/tmp.db \
    --plugins-dir samples \
    {{flags}}

# Regenerate the OpenTelemetry reference in README.md from the registry.
telemetry-doc:
  uv run scripts/telemetry-doc.py

# CI: fail if README's telemetry reference is stale.
telemetry-doc-check:
  uv run scripts/telemetry-doc.py --check

clean-dev:
  rm -rf .tmp/

dev-with-hmr *flags:
  watchexec --stop-signal SIGKILL -e py,html --ignore '*.db' --restart --clear -- \
    just dev -s plugins.datasette-vite.dev_ports.datasette_cron 5180 {{flags}}
