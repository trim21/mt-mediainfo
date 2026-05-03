---
name: server-dashboard
description: "FastAPI dashboard routes, daily_stats cache and backfill, thread pages, node pages, and admin APIs. Use when working on app/server.py or dashboard templates."
user-invocable: false
---

# Server Dashboard

The web server in `app/server.py` is a FastAPI factory that serves Jinja templates plus a small JSON API surface. It is read-heavy, uses asyncpg, and derives most page state directly from SQL queries.

## Architecture

- `create_app()` loads config, creates an asyncpg pool, and installs a JSONB codec
- HTML rendering goes through the `Render` dependency and `templates.TemplateResponse`
- `ORJSONResponse` is the JSON response wrapper used by the API endpoints
- Migrations run synchronously via `Database.run_migrations()` at startup (in a thread)

## Data Access Rules

- Server code uses asyncpg only
- Thread and job status predicates should stay aligned with `thread-lifecycle`
- RPC request validation and queue semantics belong to `rpc-system`
- Time-series reporting is normalized to `Asia/Shanghai`

## Daily Stats Cache

- Historical chart data is stored in the `daily_stats` table
- `_backfill_daily_stats(since)` fills missing days from `since` through yesterday under a single asyncio lock
- `_compute_today_stats()` computes today live so the app does not cache partial-day data
- `/api/daily-stats/clear` invalidates the cache, and `/admin` exposes that control

## Route Groups

### Overview and charts

- `/` renders the top-level dashboard with aggregate counts, sizes, and scrape status
- `/api/weekly-charts` returns cached weekly history plus live today stats
- `/detail` renders daily charts from an arbitrary start date (with `?start=YYYY-MM-DD`)

### Thread lists

- `/threads/pending-mediainfo`
- `/threads/pending-torrent`
- `/threads/pending-download` (supports `?strategy=tid|seeders` for sort order)
- `/threads/downloading`
- `/threads/done`
- `/threads/failed`
- `/threads/removed`
- `/threads/errors` (scrape error log from `scrape_error` table)

Each list page maps directly to a lifecycle predicate. If you change these filters, update `thread-lifecycle` too.

### Node and RPC views

- `/nodes` summarizes job counts per node with download speed
- `/nodes/{node_id}` shows jobs with speed and ETA formatting, supports `?status=downloading|done`
- `/rpc` shows the most recent `node_command` rows with derived status
- `POST /api/node/{node_id}/rpc` validates the node and method, then enqueues a command

### Mutations

- `POST /api/thread/{tid}/reset` deletes failed or removed jobs for a thread
- `POST /api/threads/removed/reset-all` deletes all removed-by-client jobs
- `POST /api/node/{node_id}/reset-jobs` deletes downloading jobs for a node
- `POST /api/node/{node_id}/alias` sets a node alias
- `POST /api/daily-stats/clear` clears cached history
- `GET /api/config` lists all config entries
- `POST /api/config` upserts a config entry (blocks `schema_version`)
- `DELETE /api/config/{key}` deletes a config entry (blocks `schema_version`)

## Change Guidance

- Update SQL queries and template expectations together
- Historical chart backfill stops at yesterday; keep today live-only
- Reuse existing size and byte-rate formatters from `app/utils.py`
- If a new page or API changes lifecycle semantics, update the corresponding skill as part of the same change

## Related

- `app/server.py` - FastAPI app factory and all route handlers
- `app/templates/` - Jinja templates for dashboard, threads, nodes, RPC, errors, detail, and admin pages
- `app/rpc.py` - RPC enqueue validation and method whitelist
- `thread-lifecycle` skill - Lifecycle predicates used by page filters
- `rpc-system` skill - RPC queue behavior and method registration
