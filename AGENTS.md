# AGENTS.md - AI Agent Guide

## Project Overview

This project downloads torrents from M-Team, processes local media files to extract metadata, detects hardcoded subtitles, and prepares reposting data. The codebase has three long-running modes: a downloader node, a background scraper, and a FastAPI web server.

## Code Map

- `app/downloader.py` - Downloader loop, BT client integration, local mediainfo extraction, hardcoded-subtitle detection, RPC polling
- `app/bt_client.py` - Abstract `BTClient` base class defining the torrent client interface (add, delete, list, resume, pause, get files, set file priority)
- `app/qb_client.py` - `QBittorrentClient` implementation of `BTClient` using `qbittorrentapi`
- `app/rt_client.py` - `RTorrentClient` implementation of `BTClient` using XML-RPC (`rtorrent-rpc`)
- `app/scrape.py` - Thread discovery, API mediainfo fetch, torrent download, S3 backup, and `selected_size` backfill
- `app/server.py` - FastAPI dashboard, JSON endpoints, daily stats cache, node and RPC views
- `app/rpc.py` - RPC method definitions, payload validation, queue polling, and enqueue helpers
- `app/config.py` - Pydantic-based config from environment variables for downloader, scraper, and server
- `app/const.py` - Status, tag, lock, category, and pick-strategy constants
- `app/_zstd.py` - Thin zstd streaming wrapper around `zstandard`; use `zstd_writer(dst)` / `zstd_reader(src)` instead of raw zstandard API
- `app/mt.py` - M-Team API client and exceptions
- `app/torrent.py` - Torrent parsing and largest-video-file selection
- `app/torrent_store.py` - S3-backed torrent content storage
- `app/mediainfo.py` - Mediainfo extraction from local files
- `app/hardcode_subtitle.py` - Hardcoded Chinese subtitle detection via ffprobe/ffmpeg
- `app/kv.py` - KV config store with TTL support (backed by `config` table)
- `app/db/__init__.py` - `Database` class with psycopg connection pool, advisory locks, and migration runner
- `app/sql/migrations/` - Numbered SQL migrations executed once on server startup; version tracked in the `schema_version` table. To add a new migration, create a file named `NNN_description.sql` (e.g. `009_add_column.sql`) where `NNN` is the next integer in sequence. The runner in `app/db/__init__.py` (`Database.run_migrations`) sorts files by name, parses the numeric prefix, and applies any migration whose version exceeds the stored `schema_version`.
- `taskfile.yaml` - Standard local commands
- `pyproject.toml` - Dependency and tooling configuration

## Runtime Invariants

- Python 3.12 project with environment-driven config in `app/config.py`
- `app/downloader.py` uses `BTClient` abstraction; concrete clients are `QBittorrentClient` (`qbittorrentapi`) and `RTorrentClient` (`rtorrent-rpc`)
- Downloader loop order matters: heartbeat -> wait for PG notify or timeout -> process RPC commands -> process qBittorrent torrents -> pick new jobs
- `app/downloader.py` and `app/scrape.py` use psycopg-based sync DB access; `app/server.py` uses asyncpg
- `app/sql/migrations/` contains all SQL migrations; `001_initial_schema.sql` creates the initial tables; subsequent migrations add columns and indexes; `schema_version` table tracks which migrations have run (absent = 0)
- The service is designed to run continuously; preserve retry, cooldown, and background-loop behavior when refactoring

## Key Tables

### `thread`

Tracks every M-Team torrent thread (discovered via search or gap-fill) along with its metadata, mediainfo, torrent info, and processing state.

- **Inserted**: Upserted during `scrape_search()` (search results), `scrape_detail()` (detail fetch), and `scrape_mediainfo()` (mediainfo fetch) in `app/scrape.py`. Uses `ON CONFLICT (tid) DO UPDATE`.
- **Updated**: `info_hash`, `selected_size`, `selected_index`, `torrent_fetched_at` set when a .torrent is downloaded and parsed; `mediainfo`, `hard_coded_subtitle`, `generated_mediainfo_at` set by `app/downloader.py` after local extraction; `api_mediainfo`, `api_mediainfo_at` set by scraper when fetching M-Team API mediainfo; `deleted=true` when M-Team reports torrent not found; `torrent_invalid` set on parse errors.
- **Deleted**: Never physically deleted — uses soft-delete via `deleted = true`.
- **Read**: Queried by downloader to pick jobs, by scraper to find pending work, and by server for dashboard/filter pages.
- **Key columns**: `api_mediainfo` (M-Team API data), `api_mediainfo_at` (when fetched), `mediainfo` (locally extracted), `generated_mediainfo_at` (when local extraction occurred). See `thread-lifecycle` skill for pipeline stage definitions via views (`pending_mediainfo_threads`, `pending_torrent_threads`, `pending_download_threads`, `completed_threads`, `skipped_threads`, `dormant_threads`).

### `job`

Represents a download job assigned to a specific downloader node. Tracks the lifecycle from `downloading` through terminal states (`done`, `failed`, `removed_from_download_client`, `skipped`).

- **Inserted**: In `app/downloader.py` `__pick_and_add_jobs()` when a thread is selected for download.
- **Updated**: `progress`, `dlspeed`, `eta` updated each qB poll iteration; `status` transitions: `downloading` → `done` / `failed` / `removed_from_download_client` / `skipped`; `completed_at` set when status becomes `done`; `removed_reason` set when status becomes `removed_from_download_client`.
- **Deleted**: By admin reset APIs in `app/server.py` — per-thread reset, all-removed reset, or per-node reset.
- **Read**: Joined with `thread` in pick query; aggregated by status/node for dashboard pages.

### `node`

Registers downloader nodes with their identity, last heartbeat, alias, and version.

- **Inserted/Updated**: Upserted every main-loop iteration in `app/downloader.py` `__heart_beat()`.
- **Updated**: `alias` set by admin via `POST /api/node/{node_id}/alias` in `app/server.py`.
- **Deleted**: Never.
- **Read**: For node list pages, alias resolution, and RPC target validation in `app/server.py`.

### `config`

General-purpose key-value store with optional TTL (`expires_at` column).

- **Inserted/Updated**: Upserted by `KVConfig.set()` in `app/kv.py` and by admin config API in `app/server.py`.
- **Deleted**: By `KVConfig.delete()`, `KVConfig.cleanup()` (expired rows), and admin APIs (single key or prefix group).
- **Read**: By `KVConfig.get()` (filters expired) and server dashboard.
- **Notable keys**: `search_cursor:normal`, `search_cursor:adult`, `quota_exhausted.*`, `torrent_dl:{today}:{tid}`, `daily_torrent_dl:{today}`, `last_backup_date`.

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
- **Read**: Used to detect stalled downloads — torrents whose latest `recorded_at` is older than 2 days are marked failed and removed from qBittorrent.

### `export_record`

Tracks mediainfo export jobs to S3.

- **Inserted**: By `_export_mediainfo_to_s3()` in `app/scrape.py` when a monthly export runs.
- **Updated**: `status` transitions: `running` → `success` / `failed`; `error` set on failure; `exported_count` set on insert.
- **Deleted**: By `POST /api/export-records/{export_date}/reset` in `app/server.py`.
- **Read**: Listed on `/exports` page; failed exports surfaced on dashboard index.

Export workflow:

1. Runs on the 1st of each month (`__run_export_mediainfo` checks `today.day == 1` and `last_export_mediainfo_date` KV)
2. Selects threads where `api_mediainfo != ''`, `mediainfo != ''`, `mediainfo != api_mediainfo` (locally generated mediainfo that differs from M-Team API), `exported_at = 0`, `seeders != 0`, `deleted = false`
3. Writes zstd-compressed JSON Lines to S3 under `exports/{date}/mediainfo_export.jsonl.zst`
4. Marks `thread.exported_at` with the export date int so those rows are excluded from future exports
5. On exception: marks `export_record` as `failed` with error message; does NOT update `last_export_mediainfo_date`, so the next interval retries
6. Manual reset (`POST /api/export-records/{export_date}/reset`): deletes the S3 object, resets `thread.exported_at = 0` for affected threads, deletes the `export_record` row — enabling a fresh re-export

### `schema_version`

Tracks which SQL migrations have been applied. Single-row table.

- **Inserted**: By `Database.run_migrations()` in `app/db/__init__.py` on first migration.
- **Updated**: By `Database.run_migrations()` after each migration is applied.
- **Read**: By `Database.wait_db_migration()` to block until all migrations are applied.

### `backfill_task`

Tracks per-thread work items for oneshot backfill jobs. Each backfill name gets its own set of tids populated from a `WHERE` clause on `thread`.

- **Inserted**: By `run_backfill()` in `app/scrape.py` on first run — `INSERT INTO backfill_task (name, tid) SELECT ... FROM thread WHERE ...`.
- **Updated**: `status` transitions: `pending` → `done` / `error`; `error` message set on failure.
- **Deleted**: Never — retained as history.
- **Read**: By `run_backfill()` to pick pending tasks, by `__run_backfills()` to check completion and display progress (`done/total`).

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
