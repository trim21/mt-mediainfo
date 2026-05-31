---
name: scrape-mteam
description: "M-Team scraping workflow, search cursor, rate limiting, and torrent or mediainfo fetch behavior. Use when working on app/scrape.py, app/mt.py, or thread discovery and fetch flow."
user-invocable: false
---

# M-Team Scrape Workflow

The scraper in `app/scrape.py` is a long-running scheduler that moves threads from discovery toward either API mediainfo or local-download readiness. Use `thread-lifecycle` for canonical database stage definitions; this skill explains how the scraper advances those stages.

## Loop Scheduling

- `Scrape.start()` runs `__run()` forever
- Each interval runs independent operations, each with its own cooldown window:

| Operation                | Runner key        | Description                             |
| ------------------------ | ----------------- | --------------------------------------- |
| `scrape_search()`        | `0-search`        | Discover new threads via search API     |
| `scrape_mediainfo`       | `1-mediainfo`     | Fetch mediainfo via mediaInfo API       |
| `scrape_detail`          | `2-fetch-detail`  | Fetch full detail (including mediainfo) |
| `fetch_torrent`          | `3-fetch-torrent` | Download .torrent files                 |
| `backup_to_s3`           | `4-backup`        | Daily DB backup to S3                   |
| `backfill_selected_size` | `5-backfill`      | Recompute selected_size for legacy rows |
| `_pg_dump_to_s3`         | `6-pg-dump`       | Daily raw pg_dump backup to S3          |

- A rate-limited operation gets a 5-minute cooldown; only that operation pauses, others continue
- Status per operation is tracked in the `scrape_status` table

## Operations

### `scrape_search()`

- Calls the M-Team search API sorted by `CREATED_DATE ASC`
- Runs twice per interval: once for `mode="normal"`, once for `mode="adult"`, each with its own cursor (`search_cursor.normal` / `search_cursor.adult`)
- Cursors stored via `KVConfig` (backed by the `config` table)
- Inserts or updates `thread` rows with `tid`, `size`, `category`, `seeders`, `upload_at`, and `deleted`
- Advances the cursor using the last non-topped result; topped items are excluded so the cursor does not stall

### `scrape_mediainfo(limit)`

- Selects from `pending_mediainfo_threads` view, filtered by `category = any(SELECTED_CATEGORY)`
- Calls `torrent_mediainfo(tid)` and writes `api_mediainfo` plus `api_mediainfo_at`
- If M-Team returns `種子未找到`, the thread is marked `deleted = true`

### `scrape_detail(limit)`

- Selects from `pending_mediainfo_threads` view
- Primary path: fetch detail for pending threads
- Fallback path: if nothing is pending, fill gaps in the `tid` sequence with `generate_series`
- Writes `size`, `api_mediainfo`, `category`, `seeders`, `deleted`, and `api_mediainfo_at`
- If M-Team returns `種子未找到`, the row is upserted as deleted instead of being retried forever

### `fetch_torrent()`

- Selects from `pending_torrent_threads` view, filtered by `category = any(SELECTED_CATEGORY)`
- Has a per-thread daily download limit to avoid hitting M-Team's "相同種子當天最多下載" error
- Downloads the `.torrent`, computes `info_hash`, stores content via `TorrentStore` (S3), and updates `torrent_fetched_at`
- Parses files and sets `selected_size` to the largest video file size, or `-1` when no video file exists
- Torrent download failures set `torrent_invalid = 'file error'`; parse failures set `torrent_invalid = 'parse error'`
- Errors are logged to the `scrape_error` table

### `backfill_selected_size()`

- Recomputes `selected_size` for legacy rows where `selected_size = 0` and `info_hash != ''`
- Uses stored torrent content from `TorrentStore` instead of calling the M-Team API again

### `backup_to_s3()`

- Dumps `thread`, `job`, and `node` tables as zstd-compressed JSON Lines to S3
- Runs once per day (tracked via `last_backup_date` in KV config)
- Retention: last 7 days + every 1st-of-month + specific date 2026-05-30

### `_pg_dump_to_s3()`

- Runs `pg_dump --no-owner --no-acl --no-comments` on the database
- Compresses output with zstd and uploads to S3 under `pg_dumps/{date}/dump.sql.zst`
- Same retention policy as `backup_to_s3`
- SSL key reuses the temp file already created by `pg_dsn()` (`/tmp/pg-client.key`)

## Views

All scraper queries use pipeline views instead of raw WHERE conditions:

- `pending_mediainfo_threads` — used by `scrape_detail` and `scrape_mediainfo`
- `pending_torrent_threads` — used by `fetch_torrent`

Views are defined in `app/sql/migrations/013_thread_pipeline_view.sql`.

## Ordering and Prioritization

- All selected work is filtered through `SELECTED_CATEGORY`
- Priority ordering uses `(category = any(PRIORITY_CATEGORY)) desc, tid asc`
- The scraper only prepares rows for download; downloader-side job picking and qBittorrent lifecycle are owned by `app/downloader.py`

## Error Handling

- Network failures return `RunResult.error` and do not alter cooldown timers
- `MTeamRequestError` messages `請求過於頻繁` triggers a cooldown for that operation only
- `今日下載配額用盡` is tracked via KV with a TTL so fetch_torrent skips for the rest of the day
- Missing torrents (`種子未找到`) mark the thread deleted
- Invalid torrent content is recorded via `torrent_invalid` column rather than retried indefinitely

## Related

- `app/scrape.py` - Scheduler and stage advancement
- `app/mt.py` - M-Team API surface and exceptions
- `app/torrent.py` - Torrent parsing and largest-video-file selection
- `app/torrent_store.py` - S3-backed torrent content storage
- `app/kv.py` - KV config store (cursors, TTL-based flags)
- `thread-lifecycle` skill - Canonical thread-table stage definitions
