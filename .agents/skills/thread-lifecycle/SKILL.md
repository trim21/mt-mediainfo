---
name: thread-lifecycle
description: "Database thread lifecycle stages and transitions. Use when working on thread state queries, scraping logic, or status filtering in app/scrape.py, app/server.py, app/downloader.py."
user-invocable: false
---

# Thread Lifecycle (Database)

A thread represents a torrent page on M-Team. Threads are stored in the `thread` table and progress through several pipeline stages. Stage membership is defined by **views** (`013_thread_pipeline_view.sql`), not by a status column. Scraper and downloader queries use these views to avoid duplicating WHERE conditions.

## Key Columns

| Column                   | Type               | Purpose                                                                                                 |
| ------------------------ | ------------------ | ------------------------------------------------------------------------------------------------------- |
| `api_mediainfo`          | `text`             | M-Team API mediainfo (written by scraper; `''` = not fetched or empty)                                  |
| `api_mediainfo_at`       | `timestamptz NULL` | When M-Team API mediainfo was fetched (NULL = not yet attempted)                                        |
| `mediainfo`              | `text`             | Locally extracted mediainfo (written by downloader; `''` = not yet extracted)                           |
| `generated_mediainfo_at` | `timestamptz NULL` | When local mediainfo was extracted                                                                      |
| `torrent_invalid`        | `text`             | Torrent error reason (`''` = valid, `'file error'` = download failure, `'parse error'` = parse failure) |
| `info_hash`              | `text`             | Torrent info hash (`''` = torrent file not yet downloaded)                                              |
| `selected_size`          | `int8`             | Size of largest video file (`0` = not computed, `-1` = no video file found, `-2` = BDMV)                |
| `deleted`                | `bool`             | Marked as deleted on M-Team                                                                             |
| `seeders`                | `int8`             | Number of seeders                                                                                       |
| `hard_coded_subtitle`    | `bool`             | Whether hardcoded Chinese subtitles were detected                                                       |
| `torrent_fetched_at`     | `timestamptz NULL` | When the .torrent file was downloaded                                                                   |
| `created_at`             | `timestamptz`      | When the thread row was first created                                                                   |
| `upload_at`              | `timestamptz`      | When the torrent was uploaded to M-Team                                                                 |

## Pipeline Views

Six views define the pipeline stages. All code selects from these views instead of writing raw WHERE conditions.

| View                        | Purpose                                    | Used by                                                |
| --------------------------- | ------------------------------------------ | ------------------------------------------------------ |
| `pending_mediainfo_threads` | Threads needing M-Team API mediainfo fetch | scraper `scrape_detail`, `scrape_mediainfo`            |
| `pending_torrent_threads`   | Threads needing .torrent file download     | scraper `fetch_torrent`                                |
| `pending_download_threads`  | Threads ready for downloader               | downloader `_pick_query`, server pending-download page |
| `completed_threads`         | Threads with mediainfo (API or local)      | server done page                                       |
| `skipped_threads`           | Threads that can't produce mediainfo       | reference only                                         |
| `dormant_threads`           | Threads with no seeders or deleted         | server skipped count                                   |

See `app/sql/migrations/013_thread_pipeline_view.sql` for the full definition.

## Lifecycle Stages

```
scrape_search() discovers thread
  api_mediainfo_at=NULL, api_mediainfo='', mediainfo='', info_hash=''
       │
       ▼
  pending_mediainfo_threads (Stage 1)
       │
       ├── scrape_mediainfo() or scrape_detail()
       │   sets api_mediainfo, api_mediainfo_at
       │
       ▼
  ┌─ api_mediainfo != '' ──► completed_threads (M-Team has it, no download needed)
  │
  └─ api_mediainfo == '' ──► pending_torrent_threads (Stage 2)
       │
       ├── fetch_torrent()
       │   downloads .torrent, parses info_hash, computes selected_size
       │
       ▼
  ┌─ selected_size == -1 ──► skipped_threads (no video file)
  │
  ├─ selected_size == -2 ──► skipped_threads (BDMV)
  │
  └─ selected_size > 0 ──► pending_download_threads (Stage 3)
       │
       ├── downloader picks → creates job, adds to qBittorrent
       │
       ▼
  Stage 4: Downloading (tracked by job table)
       │
       ├── __process_local_torrent()
       │   extracts mediainfo, sets generated_mediainfo_at
       │
       ▼
  completed_threads (Stage 5: local mediainfo)
```

## Stage Details

### pending_mediainfo_threads

- **Definition**: `deleted=false AND seeders!=0 AND api_mediainfo_at IS NULL AND api_mediainfo=''`
- **Action**: scraper calls M-Team API to fetch mediainfo
- **Transition**: Sets `api_mediainfo` and `api_mediainfo_at`:
  - `api_mediainfo != ''` → `completed_threads` (M-Team has mediainfo, no local extraction needed)
  - `api_mediainfo == ''` → `pending_torrent_threads` (need .torrent for local extraction)

### pending_torrent_threads

- **Definition**: `deleted=false AND seeders!=0 AND api_mediainfo_at IS NOT NULL AND mediainfo='' AND api_mediainfo='' AND info_hash='' AND torrent_invalid=''`
- **Action**: scraper downloads `.torrent` file, parses it, stores content via `TorrentStore`
- **Transition**: Sets `info_hash`, `selected_size`, `torrent_fetched_at`:
  - `selected_size > 0` → `pending_download_threads`
  - `selected_size <= 0` → `skipped_threads`

### pending_download_threads

- **Definition**: `deleted=false AND seeders!=0 AND mediainfo='' AND api_mediainfo='' AND info_hash!='' AND selected_size>0`
- **Action**: downloader picks threads, creates a job, adds torrent to qBittorrent
- **Additional downloader filters**: `selected_size < single_torrent_size_limit`, `category = any(SELECTED_CATEGORY)`, `seeder_condition`, no existing job

### completed_threads

- **Definition**: `deleted=false AND seeders!=0 AND ((mediainfo!='' AND info_hash!='') OR api_mediainfo!='')`
- **Two paths**:
  - M-Team API mediainfo: `api_mediainfo != ''`
  - Local extraction: `mediainfo != '' AND info_hash != ''` (also has `generated_mediainfo_at`)

### skipped_threads

- **Definition**: `deleted=false AND seeders!=0 AND (torrent_invalid!='' OR (selected_size<=0 AND info_hash!=''))`
- Terminal: can't produce mediainfo (invalid torrent or no video file)

### dormant_threads

- **Definition**: `deleted OR seeders=0`
- Not actively processed. If seeders return (>0) and not deleted, `scrape_search` upserts `seeders` and `scrape_detail` or `scrape_mediainfo` will pick them up.

## Mediainfo Sources

The project distinguishes two sources of mediainfo:

| Source           | Column          | Timestamp                | Writer     |
| ---------------- | --------------- | ------------------------ | ---------- |
| M-Team API       | `api_mediainfo` | `api_mediainfo_at`       | scraper    |
| Local extraction | `mediainfo`     | `generated_mediainfo_at` | downloader |

Export scripts compare `mediainfo` vs `api_mediainfo`: if they match, the data came from M-Team; if they differ, it's locally generated.

## Related

- `app/sql/migrations/013_thread_pipeline_view.sql` — View definitions
- `app/sql/migrations/012_api_mediainfo.sql` — `api_mediainfo` column and `api_mediainfo_at` rename
- `app/scrape.py` — All scraping and fetching logic
- `app/downloader.py` — Download and processing logic
- `app/server.py` — Dashboard queries using views
