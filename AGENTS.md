# AGENTS.md - AI Agent Guide

## Project Overview

This project downloads torrents from M-Team, processes local media files to extract metadata, detects hardcoded subtitles, and prepares reposting data. The codebase has three long-running modes: a downloader node, a background scraper, and a FastAPI web server.

## Code Map

- `app/downloader.py` - Downloader loop, qBittorrent integration, local mediainfo extraction, hardcoded-subtitle detection, RPC polling
- `app/scrape.py` - Thread discovery, API mediainfo fetch, torrent download, S3 backup, and `selected_size` backfill
- `app/server.py` - FastAPI dashboard, JSON endpoints, daily stats cache, node and RPC views
- `app/rpc.py` - RPC method definitions, payload validation, queue polling, and enqueue helpers
- `app/config.py` - Pydantic-based config from environment variables for downloader, scraper, and server
- `app/const.py` - Status, tag, lock, category, and pick-strategy constants
- `app/mt.py` - M-Team API client and exceptions
- `app/torrent.py` - Torrent parsing and largest-video-file selection
- `app/torrent_store.py` - S3-backed torrent content storage
- `app/mediainfo.py` - Mediainfo extraction from local files
- `app/hardcode_subtitle.py` - Hardcoded Chinese subtitle detection via ffprobe/ffmpeg
- `app/kv.py` - KV config store with TTL support (backed by `config` table)
- `app/db/__init__.py` - `Database` class with psycopg connection pool, advisory locks, and migration runner
- `app/sql/migrations/` - Numbered SQL migrations executed once on server startup; version tracked in the `config` table (`schema_version` key). To add a new migration, create a file named `NNN_description.sql` (e.g. `009_add_column.sql`) where `NNN` is the next integer in sequence. The runner in `app/db/__init__.py` (`Database.run_migrations`) sorts files by name, parses the numeric prefix, and applies any migration whose version exceeds the stored `schema_version`.
- `taskfile.yaml` - Standard local commands
- `pyproject.toml` - Dependency and tooling configuration

## Runtime Invariants

- Python 3.12 project with environment-driven config in `app/config.py`
- `app/downloader.py` uses `qbittorrentapi` directly to interact with qBittorrent
- Downloader loop order matters: heartbeat -> wait for PG notify or timeout -> process RPC commands -> process qBittorrent torrents -> pick new jobs
- `app/downloader.py` and `app/scrape.py` use psycopg-based sync DB access; `app/server.py` uses asyncpg
- `app/sql/migrations/` contains all SQL migrations; `001_initial_schema.sql` creates the initial tables; subsequent migrations add columns and indexes; `schema_version` in the `config` table tracks which migrations have run (absent = 0)
- The service is designed to run continuously; preserve retry, cooldown, and background-loop behavior when refactoring

## Key Tables

### `thread`

Tracks every M-Team torrent thread (discovered via search or gap-fill) along with its metadata, mediainfo, torrent info, and processing state.

- **Inserted**: Upserted during `scrape_search()` (search results), `scrape_detail()` (detail fetch), and `scrape_mediainfo()` (mediainfo fetch) in `app/scrape.py`. Uses `ON CONFLICT (tid) DO UPDATE`.
- **Updated**: `info_hash`, `selected_size`, `selected_files`, `torrent_fetched_at` set when a .torrent is downloaded and parsed; `mediainfo`, `hard_coded_subtitle` set by `app/downloader.py` after local extraction; `deleted=true` when M-Team reports torrent not found; `torrent_invalid` set on parse errors.
- **Deleted**: Never physically deleted — uses soft-delete via `deleted = true`.
- **Read**: Queried by downloader to pick jobs, by scraper to find pending work, and by server for dashboard/filter pages.

### `job`

Represents a download job assigned to a specific downloader node. Tracks the lifecycle from `downloading` through terminal states (`done`, `failed`, `removed_from_download_client`, `skipped`).

- **Inserted**: In `app/downloader.py` `__pick_and_add_jobs()` when a thread is selected for download.
- **Updated**: `progress`, `dlspeed`, `eta` updated each qB poll iteration; `status` transitions: `downloading` → `done` / `failed` / `removed_from_download_client` / `skipped`; `completed_at` set when status becomes `done`.
- **Deleted**: By admin reset APIs in `app/server.py` — per-thread reset, all-removed reset, or per-node reset.
- **Read**: Joined with `thread` in pick query; aggregated by status/node for dashboard pages.

### `node`

Registers downloader nodes with their identity, last heartbeat, alias, and version.

- **Inserted/Updated**: Upserted every main-loop iteration in `app/downloader.py` `__heart_beat()`.
- **Updated**: `alias` set by admin via `POST /api/node/{node_id}/alias` in `app/server.py`.
- **Deleted**: Never.
- **Read**: For node list pages, alias resolution, and RPC target validation in `app/server.py`.

### `config`

Dual-purpose table: (1) schema migration tracking via `schema_version` key; (2) general-purpose key-value store with optional TTL (`expires_at` column).

- **Inserted/Updated**: Upserted by `KVConfig.set()` in `app/kv.py`, by migration runner in `app/db/__init__.py`, and by admin config API in `app/server.py`.
- **Deleted**: By `KVConfig.delete()`, `KVConfig.cleanup()` (expired rows), and admin APIs (single key or prefix group).
- **Read**: By `KVConfig.get()` (filters expired), migration runner, and server dashboard.
- **Notable keys**: `schema_version`, `search_cursor:normal`, `search_cursor:adult`, `quota_exhausted.*`, `torrent_dl:{today}:{tid}`, `daily_torrent_dl:{today}`, `last_backup_date`.

### `daily_stats`

Pre-computed daily aggregate statistics for chart endpoints. Columns include `downloaded_bytes`, `downloaded_count`, `fetched_bytes`, `fetched_count`, `thread_count`, `torrent_count`, `mediainfo_count`, and per-node breakdown (`node_downloaded` JSONB).

- **Inserted/Updated**: Upserted by `_backfill_daily_stats()` in `app/server.py`, triggered on chart page loads.
- **Deleted**: By admin `POST /api/daily-stats/clear` to force full re-backfill.
- **Read**: By chart pages in `app/server.py` for weekly/daily statistics.

### `node_command`

RPC command queue. The server enqueues commands; downloader nodes poll and execute them.

- **Inserted**: By `enqueue_command()` in `app/rpc.py`, called from `POST /api/node/{node_id}/rpc` in `app/server.py`. A `pg_notify` wakes the target node.
- **Updated**: `executed_at`, `result`, `error` set after command execution in `app/rpc.py` `process_commands()`.
- **Deleted**: Never — retained as history.
- **Read**: Polled by `process_commands()` in `app/downloader.py` for pending commands; displayed on RPC history page in `app/server.py`.

### `scrape_status`

Tracks the last run time, result, and next-allowed time for each named scrape operation (e.g., `0-search`, `1-mediainfo`, `2-fetch-detail`).

- **Inserted/Updated**: Upserted by `__update_status()` in `app/scrape.py` before and after each operation.
- **Deleted**: Stale entries (operations that no longer exist) removed at startup in `app/scrape.py`.
- **Read**: Displayed on the dashboard in `app/server.py`.

### `scrape_error`

Append-only log of M-Team API errors encountered during scraping.

- **Inserted**: By `_log_scrape_error()` in `app/scrape.py` when `fetch_torrent()` encounters an API error.
- **Updated**: Never.
- **Deleted**: Never.
- **Read**: Paginated listing on `/threads/errors` in `app/server.py`.

### `job_download_size`

Tracks per-torrent download progress over time.

- **Inserted**: During the qBittorrent poll loop in `app/downloader.py`, a new row is written only when `t.completed` (bytes downloaded) differs from the most recent recorded value for that `(info_hash, node_id)` pair.
- **Deleted**: Rows are removed when the associated job is resolved — either completed (mediainfo extraction done) or failed (including stalled cleanup).
- **Read**: Used to detect stalled downloads — torrents whose latest `recorded_at` is older than 3 days are marked failed and removed from qBittorrent.

## Skills

- `qb-torrent-lifecycle` - qBittorrent state machine, tags, file selection, and cleanup rules for `app/downloader.py`
- `thread-lifecycle` - Canonical thread-table stage predicates used by scrape, downloader, and server queries
- `rpc-system` - `node_command` queue, payload types, handler registration, and RPC history behavior
- `scrape-mteam` - Search cursor, scrape scheduling, rate limits, detail and mediainfo fetch, torrent download flow
- `server-dashboard` - Dashboard queries, `daily_stats` caching and backfill, thread pages, node pages, and admin APIs

## Development Workflow

- Install or update dependencies: `uv sync`
- Run checks: `task default`
- Run downloader locally: `task dev` or `task downloader`
- Run scraper: `task scrape`
- Run web server: `task dev:server`

## Change Guidelines

- Always use a dataclass to receive JSON request bodies in FastAPI endpoints; never call `request.json()` and manually extract fields
- Keep statuses, tags, and selected-category definitions in `app/const.py` as the source of truth
- Treat this project as an application rather than a reusable library: preserve data compatibility during refactors, but backward compatibility of internal code interfaces is not required
- When changing thread or job state queries, keep `app/downloader.py`, `app/scrape.py`, and `app/server.py` aligned with `thread-lifecycle`
- When changing qBittorrent processing, keep `app/downloader.py` aligned with `qb-torrent-lifecycle`
- When adding server-to-downloader actions, update `app/rpc.py`, `app/downloader.py`, and `app/server.py` together
- Keep subsystem-specific procedures in skills rather than expanding `AGENTS.md`
- when adding query argument to fastapi handler, prefer to use `Annotated[T, Query()]`.
