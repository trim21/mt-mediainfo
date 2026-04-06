---
name: thread-lifecycle
description: 'Database thread lifecycle stages and transitions. Use when working on thread state queries, scraping logic, or status filtering in app/scrape.py, app/server.py, app/application.py.'
user-invocable: false
---

# Thread Lifecycle (Database)

A thread represents a torrent page on M-Team. Threads are stored in the `thread` table and progress through several stages based on column values. There is no explicit `status` column — the stage is determined by a combination of `mediainfo_at`, `mediainfo`, `info_hash`, and `selected_size`.

## Key Columns

| Column | Type | Purpose |
|---|---|---|
| `mediainfo_at` | `timestamptz NULL` | When mediainfo was fetched (NULL = not yet attempted) |
| `mediainfo` | `text` | Mediainfo text (`''` = not yet obtained, `'invalid torrent'` = parse error) |
| `info_hash` | `text` | Torrent info hash (`''` = torrent file not yet downloaded) |
| `selected_size` | `int8` | Size of largest video file (`0` = not computed, `-1` = no video file found) |
| `deleted` | `bool` | Marked as deleted on M-Team |
| `seeders` | `int8` | Number of seeders |

## Lifecycle Stages

```
scrape_search() discovers thread
  mediainfo_at=NULL, mediainfo='', info_hash=''
       │
       ▼
  Stage 1: Pending Fetch Mediainfo
       │
       ├── scrape_mediainfo() or scrape_detail()
       │   sets mediainfo_at, possibly mediainfo
       │
       ▼
  ┌─ mediainfo != '' ──► Stage 5: Done (API mediainfo)
  │
  └─ mediainfo == '' ──► Stage 2: Pending Fetch Torrent
       │
       ├── fetch_torrent()
       │   downloads .torrent, parses info_hash, computes selected_size
       │
       ▼
  ┌─ selected_size == -1 ──► Terminal: No video file (skipped)
  │
  └─ selected_size > 0 ──► Stage 3: Pending Download
       │
       ├── pick_job() in application.py
       │   creates job, adds to qBittorrent
       │
       ▼
  Stage 4: Downloading (tracked by job table)
       │
       ├── __process_local_torrent()
       │   extracts mediainfo from downloaded file
       │
       ▼
  Stage 5: Done (local mediainfo)
```

## Stage Details

### Stage 1: Pending Fetch Mediainfo

- **Condition**: `mediainfo_at IS NULL AND deleted = false`
- **Action**: `scrape_mediainfo()` calls M-Team `/api/torrent/mediaInfo` API
- **Transition**: Sets `mediainfo_at = current_timestamp`. If API returns mediainfo text, sets `mediainfo` → Done. If empty, → Stage 2.
- **Alternative**: `scrape_detail()` also sets `mediainfo_at` and `mediainfo` via `/api/torrent/detail`

### Stage 2: Pending Fetch Torrent

- **Condition**: `mediainfo_at IS NOT NULL AND mediainfo = '' AND info_hash = ''`
- **Action**: `fetch_torrent()` downloads the `.torrent` file via M-Team API, parses it to extract `info_hash`, computes `selected_size`
- **Transition**: Sets `info_hash`, stores torrent content in `torrent` table, sets `selected_size`
- **Edge cases**:
  - Invalid torrent file → sets `mediainfo = 'invalid torrent'` (terminal)
  - No video file in torrent → sets `selected_size = -1` (skipped by download)

### Stage 3: Pending Download

- **Condition**: `mediainfo = '' AND info_hash != '' AND selected_size > 0`
- **Action**: `__pick_and_add_jobs()` in `application.py` selects threads ordered by `selected_size DESC`, creates a job, adds torrent to qBittorrent
- **Filters**: Must have `seeders != 0`, `category` in `SELECTED_CATEGORY`, `selected_size < single_torrent_size_limit`, no existing job

### Stage 4: Downloading

- **Tracked by**: `job` table with `status = 'downloading'`
- **Action**: qBittorrent downloads the torrent, node monitors progress
- **See**: `qb-torrent-lifecycle` skill for qBittorrent-side details

### Stage 5: Done

- **Condition**: `mediainfo_at IS NOT NULL AND mediainfo != '' AND mediainfo != 'invalid torrent'`
- **Two paths**:
  - **API mediainfo**: Obtained directly from M-Team API in Stage 1 (no download needed, `info_hash` may be `''`)
  - **Local mediainfo**: Extracted from downloaded file by `__process_local_torrent()` (has `info_hash`)
- **Web UI**: Done page filters by `info_hash != ''` to show only locally processed threads

## Terminal States

| State | Condition | Cause |
|---|---|---|
| Deleted | `deleted = true` | Thread removed from M-Team (`種子未找到`) |
| Invalid torrent | `mediainfo = 'invalid torrent'` | Torrent file could not be parsed |
| No video file | `selected_size = -1` | No video file found in torrent |

## Backfill

`backfill_selected_size()` recomputes `selected_size` for threads where `selected_size = 0 AND info_hash != ''`. This handles threads that had their torrent downloaded before `selected_size` was introduced. Sets `-1` for no video file.

## Related

- `app/scrape.py` — All scraping and fetching logic
- `app/application.py` — Download and processing logic (Stages 3-5)
- `app/server.py` — Web UI pages for each stage (`/threads/pending-mediainfo`, `/threads/pending-torrent`, `/threads/pending-download`, `/threads/done`, etc.)
- `app/sql/schema.sql` — Table definitions
