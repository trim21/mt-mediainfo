---
name: qb-torrent-lifecycle
description: "qBittorrent torrent lifecycle stages and tag transitions. Use when working on torrent download, processing, or tag logic in app/downloader.py."
user-invocable: false
---

# qBittorrent Torrent Lifecycle

Torrents managed by the application go through several stages in qBittorrent. The lifecycle is determined by **torrent state** (`state.is_paused`, `state.is_uploading`, etc.) and the `process-error` tag — NOT by other tags. Tags are purely informational labels to help users see what stage a torrent is in via the qBittorrent UI.

## Tags (informational only)

Tags are defined in `app/const.py`. They do NOT drive lifecycle logic — they are set/removed alongside state transitions to provide visibility.

| Constant                 | Value             | Description                                                                            |
| ------------------------ | ----------------- | -------------------------------------------------------------------------------------- |
| `QB_TAG_NEED_SELECT`     | `need-select`     | Newly added torrent, needs file selection                                              |
| `QB_TAG_SELECTING_FILES` | `selecting-files` | Torrent is in stopped state, waiting for file selection                                |
| `QB_TAG_DOWNLOADING`     | `downloading`     | Torrent has been resumed and is actively downloading                                   |
| `QB_TAG_PROCESSING`      | `processing`      | Download complete, extracting mediainfo                                                |
| `QB_TAG_PROCESS_ERROR`   | `process-error`   | Mediainfo extraction failed (**exception**: this tag IS used in logic to skip retries) |

## Lifecycle Stages

```
torrents_add (with tags=[downloading, need-select], download_limit=1, sequential)
       │
       ▼
  Select largest video file (set other files priority=0)
  Clear download limit (set to 0), remove need-select tag
       │
       ▼
  Downloading... (state.is_paused=false, state.is_uploading=false)
  Progress updated in DB each interval
       │
       ▼
  Download complete (state.is_uploading=true)
  tag: -downloading +processing
       │
       ├── Success: extract mediainfo + check hardcoded subtitles → update DB → torrents_delete
       │
       └── Failure: tag: +process-error, job marked failed in DB
```

## Stage Determination Logic

The code in `__process_qb_torrents()` determines the stage using torrent state, NOT tags:

1. **Not in managed jobs**: Handled by `__handle_unmanaged_torrent()` — tries to reclaim if `removed-by-client`, otherwise deletes
2. **Old torrents** (`seen_complete` older than 10 days): Deleted, job marked failed with "no seeders"
3. **Error state** (`state.is_errored`): Deleted, job marked failed with "torrent error"
4. **Has `process-error` tag**: Skipped entirely (the one exception where a tag affects logic)
5. **Unselected category**: Deleted, job marked skipped
6. **Uploading/seeding** (`state.is_uploading`): Swaps tag to `processing`, runs mediainfo extraction
7. **Has `need-select` tag**: Selects largest video file, clears download limit, removes tag
8. **Stopped/paused** (`state.is_paused`): Swaps tag to `downloading`, resumes the torrent
9. **Downloading** (default): Updates progress/dlspeed/eta in DB

## Stage Details

### 1. File Selection (stopped)

- **Detected by**: `QB_TAG_NEED_SELECT in t.tags` (torrent added with `tags=[QB_TAG_DOWNLOADING, QB_TAG_NEED_SELECT]`)
- **Entry**: `__add_to_qb()` adds torrent with download limit of 1 byte/s and sequential download
- **Action**: `__fix_file_selection()` finds largest video file, sets all other files to priority 0, clears download limit
- **Exit**: Removes `need-select` tag, torrent continues downloading
- **Note**: No try/except in `__fix_file_selection()` — exceptions bubble up

### 2. Downloading (active)

- **Detected by**: `not state.is_paused and not state.is_uploading`
- **Entry**: After file selection in `__fix_file_selection()`
- **Action**: `__process_qb_torrents()` updates job progress/dlspeed/eta in DB each interval
- **Exit**: When `state.is_uploading` becomes true (download complete)

### 3. Processing (upload/seed state)

- **Detected by**: `state.is_uploading` and no `process-error` tag
- **Entry**: `__process_qb_torrents()` detects upload state
- **Action**: `__process_local_torrent()` extracts mediainfo, checks hardcoded subtitles
- **Tags set**: Remove `downloading`, add `processing`
- **Success exit**: Updates thread with mediainfo and `hard_coded_subtitle`, marks job done, deletes torrent from qb
- **Failure exit**: Adds `process-error` tag, marks job failed in DB

### 4. Process Error (terminal)

- **Detected by**: `QB_TAG_PROCESS_ERROR in t.tags`
- **Behavior**: Torrent remains in qb, skipped by all processing loops
- **Recovery**: Manual intervention required (or reset via web UI which deletes the job)

## Recovery: Stopped Torrents

In `__process_qb_torrents()`, paused torrents are detected and resumed with tag swap (`-selecting-files +downloading`).

## Cleanup

- **Old torrents**: Torrents where `seen_complete` is older than 10 days are deleted, job marked failed with "no seeders"
- **Torrent error state**: Torrents in `state.is_errored` are deleted, job marked failed with "torrent error"
- **Unselected category**: Torrents whose thread category is no longer in `SELECTED_CATEGORY` are deleted, job marked skipped
- **Removed from client**: If a torrent disappears from qb (user deleted), job is marked `removed-by-client`
- **Unmanaged torrents**: Torrents not in any downloading job are paused (or reclaimed if previously marked `removed-by-client`)

## Related

- `app/downloader.py` — All qBittorrent processing logic (`Downloader.__process_qb_torrents()`)
- `app/const.py` — Tag and status constants
- `thread-lifecycle` skill — Database-side thread state machine
