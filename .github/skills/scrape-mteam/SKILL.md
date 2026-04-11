---
name: scrape-mteam
description: 'M-Team scraping workflow, search cursor, rate limiting, and torrent or mediainfo fetch behavior. Use when working on app/scrape.py, app/mt.py, or thread discovery and fetch flow.'
user-invocable: false
---

# M-Team Scrape Workflow

The scraper in `app/scrape.py` is a long-running scheduler that moves threads from discovery toward either API mediainfo or local-download readiness. Use `thread-lifecycle` for the canonical database stage definitions; this skill explains how the scraper advances those stages.

## Loop Scheduling

- `Scrape.start()` runs `__run()` forever with a 10-minute interval
- `SCRAPE_LIMIT` controls the `scrape_detail()` and `scrape_mediainfo()` batch size; default `100`
- Each interval runs four independent operations: `fetch`, `search`, `mediainfo`, and `scrape`
- Each operation has its own cooldown window, so an M-Team rate limit only pauses the affected operation for 60 minutes

## Operations

### `scrape_search()`

- Calls the M-Team search API sorted by `CREATED_DATE ASC`
- Stores progress in the `config` table key `search_cursor`
- Inserts or updates `thread` rows with `tid`, `size`, `category`, `seeders`, `upload_at`, and `deleted`
- Advances the cursor using the last non-topped result; topped items are excluded so the cursor does not stall

### `scrape_mediainfo(limit)`

- Targets selected-category threads where `deleted = false` and `mediainfo_at is null`
- Calls `torrent_mediainfo(tid)` and writes `mediainfo` plus `mediainfo_at`
- If M-Team returns `種子未找到`, the thread is marked `deleted = true`

### `scrape_detail(limit)`

- Primary path: fetch detail for selected-category threads missing `mediainfo_at`
- Fallback path: if nothing is pending, fill gaps in the `tid` sequence with `generate_series`
- Writes `size`, `mediainfo`, `category`, `seeders`, `deleted`, and `mediainfo_at`
- If M-Team returns `種子未找到`, the row is upserted as deleted instead of being retried forever

### `fetch_torrent()`

- Targets threads where `mediainfo_at is not null`, `mediainfo = ''`, `info_hash = ''`, `seeders != 0`, and category is selected
- Downloads the `.torrent`, computes `info_hash`, stores raw content in `torrent`, and updates `torrent_fetched_at`
- Parses files and sets `selected_size` to the largest video file size, or `-1` when no video file exists
- Torrent parse failures and invalid torrent files are terminalized as `mediainfo = 'invalid torrent'`

### `backfill_selected_size()`

- Recomputes `selected_size` for legacy rows where `selected_size = 0` and `info_hash != ''`
- Uses stored torrent content instead of calling the M-Team API again

## Ordering and Prioritization

- All selected work is filtered through `SELECTED_CATEGORY`
- Priority ordering uses `(category = any(PRIORITY_CATEGORY)) desc, tid asc`
- The scraper only prepares rows for download; node-side job picking and qBittorrent lifecycle are owned by `app/node.py`

## Error Handling

- Network failures return `RunResult.error` and do not alter cooldown timers
- `MTeamRequestError` messages `請求過於頻繁` and `今日下載配額用盡` trigger a 60-minute cooldown for that operation only
- Missing torrents (`種子未找到`) mark the thread deleted
- Invalid torrent content is recorded in the database rather than retried indefinitely

## Related

- `app/scrape.py` - Scheduler and stage advancement
- `app/mt.py` - M-Team API surface and exceptions
- `app/torrent.py` - Torrent parsing and largest-video-file selection
- `thread-lifecycle` skill - Canonical thread-table stage definitions
