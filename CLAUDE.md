# datasette-cron

Scheduled tasks and background jobs for Datasette. Provides a central scheduler where plugins register handler functions via a hook, and tasks (DB rows) reference those handlers with config + schedule.

## Architecture

- **Backend:** Python, Datasette >=1.0a23, datasette-plugin-router, Pydantic
- **Frontend:** Svelte 5 (runes), TypeScript, Vite, openapi-fetch
- **Database:** sqlite-migrate for internal.db schema management
- **Build:** Just (Justfile), uv (Python), npm (frontend)

## Commands

| Command | What it does |
|---------|-------------|
| `just dev` | Run Datasette dev server on port 8010 |
| `just dev-with-hmr` | Datasette + Vite HMR (restarts on .py/.html changes) |
| `just frontend-dev` | Start Vite dev server on port 5180 |
| `just frontend` | Build frontend for production |
| `just types` | Regenerate all TypeScript types from Python |
| `just types-watch` | Watch .py files, auto-regenerate types |
| `just format` | Format backend (ruff) + frontend (prettier) |
| `just check` | Type-check backend (ty) + frontend (svelte-check) |
| `just test` | Run Python tests |
| `just shots [names...]` | Regenerate committed doc screenshots in `docs/screenshots/` (throwaway datasette on :8492; one-time setup `npx playwright install chromium` in `frontend/`) |

## Project Structure

```
datasette_cron/
├── __init__.py              # Plugin hooks (startup, routes, asgi_wrapper, menu_links)
├── hookspecs.py             # cron_register_handlers hookspec
├── router.py                # Shared Router + permission decorator
├── page_data.py             # Pydantic models (page data + API contracts)
├── internal_db.py           # Database operations wrapper
├── internal_migrations.py   # sqlite-migrate schema
├── schedules.py             # CronSchedule, IntervalSchedule, RRuleSchedule
├── scheduler.py             # Scheduler class: loop, execution, retry, CRUD API
├── routes/
│   ├── pages.py             # Page routes (render HTML)
│   └── api.py               # API routes (return JSON)
└── templates/
    └── cron_base.html       # Single base template

frontend/src/
├── pages/index/             # Task list page
├── pages/detail/            # Task detail + run history page
├── page_data/load.ts        # loadPageData<T>() helper
├── page_data/*.types.ts     # Generated TypeScript types
├── components/              # Shared components
└── store.svelte.ts          # Global state
```

## Core Concepts

**Handlers:** Python functions registered via `cron_register_handlers(datasette)` hook. Return `{"name": callable}`. Signature: `async fn(datasette, config: dict)`.

**Tasks:** DB rows in `datasette_cron_tasks`. Reference a handler by name + JSON config + schedule. Created via `datasette._cron_scheduler.add_task(...)`.

**Schedules:** Three types — cron string (`"0 8 * * *"`), interval (`{"interval": 60}`), rrule (`{"rrule": "FREQ=WEEKLY;BYDAY=MO"}`). All support timezone.

## Routes

**Pages:**
- `GET /-/cron` → Task list
- `GET /-/cron/<name>` → Task detail + run history

**API:**
- `GET /-/api/cron/tasks` → JSON task list
- `GET /-/api/cron/tasks/<name>/runs` → Run history
- `POST /-/api/cron/tasks/<name>/trigger` → Manual trigger
- `POST /-/api/cron/tasks/<name>/enable` → Toggle enable/disable

## Database

**Tables (in internal.db):**
- `datasette_cron_tasks` — Task definitions (handler, config, schedule, status)
- `datasette_cron_runs` — Execution history (status, duration, errors)

**Migrations:** `internal_migrations.py` — applied on startup.

## Hooks Used

- `register_routes()` — registers all routes from shared router
- `extra_template_vars()` — provides `datasette_cron_vite_entry()` for templates
- `register_actions()` — defines `datasette-cron-access` permission
- `menu_links()` — adds "Cron Tasks" to nav menu
- `startup()` — applies migrations, builds handler registry, starts scheduler
- `asgi_wrapper()` — intercepts lifespan shutdown for graceful cleanup

## Hooks Defined

- `cron_register_handlers(datasette)` — plugins implement this to register handler functions

## Key Conventions

- **Svelte 5 runes**: Use `$state()`, `$derived()`, `$effect()`, `$props()`
- **Permissions**: All routes use `@check_permission()` decorator
- **Template**: One template for all pages — different `entrypoint` and `page_data` per route
- **Internal DB reads**: Use `execute_write_fn()` even for reads
- **add_task is idempotent**: Safe to call on every startup (upsert)
