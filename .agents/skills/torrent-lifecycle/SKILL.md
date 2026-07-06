---
name: torrent-lifecycle
description: "Download client torrent lifecycle stages and tag transitions. Use when working on torrent download, processing, or tag logic in app/bin/downloader.py."
user-invocable: false
---

# Torrent Lifecycle

Torrents managed by the application go through several stages in the download client (qBittorrent, rTorrent, or Neptune). The lifecycle is determined by **torrent state** and the `process-error` tag — NOT by other tags. Tags are purely informational labels to help users see what stage a torrent is in via the client UI.

## Tags (informational only)

Tags are defined in `app/const.py`. They do NOT drive lifecycle logic — they are set/removed alongside state transitions to provide visibility.

| Constant                 | Value             | Description                                                                            |
| ------------------------ | ----------------- | -------------------------------------------------------------------------------------- |
| `BT_TAG_DOWNLOADING`     | `downloading`     | Torrent is actively downloading (set at add time and after resume)                     |
| `BT_TAG_FILE_SELECTED`   | `file-selected-4` | File selection has been applied, download limit cleared                                |
| `BT_TAG_SELECTING_FILES` | `selecting-files` | Torrent is in paused state waiting for file selection (only on rtorrent)               |
| `BT_TAG_PROCESSING`      | `processing`      | Download complete, extracting mediainfo                                                |
| `BT_TAG_PROCESS_ERROR`   | `process-error`   | Mediainfo extraction failed (**exception**: this tag IS used in logic to skip retries) |

## Lifecycle Stages

```
torrents_add (with tags=[downloading], download_limit=1, sequential)
       │  _progress_record(hash, 0) for initial stalled-detection sample
       │
       ▼
  file-selected-4 not in tags → select largest video file (set others priority=0)
  Add file-selected-4 tag, clear download limit, resume if paused
       │
       ▼
  Downloading... (state is downloading)
  Each loop: _progress_record() → _progress_avg_speed() → batch update job in PG
       │
       ▼
  Download complete (state.is_uploading)
  tag: -downloading +processing
       │
       ├── Success: extract mediainfo + check hardcoded subtitles → update thread → torrents_delete
       │
       └── Failure: tag: +process-error, job marked failed in DB
```

## Stage Determination Logic

The code in `__process_torrents()` determines the stage using torrent state, NOT tags:

1. **Not in managed jobs**: Handled by `__handle_unmanaged_torrent()` — tries to reclaim if `removed-by-client`, otherwise deletes
2. **Stalled** (no progress for 2 days via local `progress.db`): Removed from client, thread marked `torrent_invalid = 'stalled'`, job marked `removed_from_download_client` (reason: `stalled`)
3. **Error state** (`state.is_errored`): Deleted, job marked failed with "torrent error"
4. **Has `process-error` tag**: Skipped entirely (the one exception where a tag affects logic)
5. **Unselected category**: Deleted, job marked skipped
6. **Uploading/seeding** (`state.is_uploading`): Swaps tag to `processing`, runs mediainfo extraction
7. **File not yet selected** (`BT_TAG_FILE_SELECTED` not in tags): Selects largest video file, clears download limit, adds `BT_TAG_FILE_SELECTED` tag, resumes if paused
8. **Stopped/paused** (`state.is_paused`): Swaps tag to `downloading`, resumes the torrent
9. **Downloading** (default): Records progress sample to local `progress.db` via `_progress_record()`, computes avg speed via `_progress_avg_speed()`, batch-updates job progress/dlspeed/eta in PostgreSQL

## Stage Details

### 1. File Selection (just added)

- **Detected by**: `BT_TAG_FILE_SELECTED not in t.tags`
- **Entry**: `__add_torrent()` adds torrent with `download_limit=1`, `is_sequential_download=True`, tags=`[BT_TAG_DOWNLOADING]`
- **Action**: `__fix_file_selection()` finds largest video file, sets all other files to priority 0, clears download limit, adds `BT_TAG_FILE_SELECTED` tag
- **Exit**: Tag `BT_TAG_FILE_SELECTED` added, download proceeds at full speed
- **Note**: No try/except in `__fix_file_selection()` — exceptions bubble up

### 2. Downloading (active)

- **Detected by**: Torrent state is downloading (not paused, not uploading, not errored)
- **Entry**: After file selection or after resume from paused state
- **Action**: `__batch_update_downloading()` records progress to local `progress.db` via `_progress_record()`, computes avg speed via `_progress_avg_speed()`, batch-updates job `progress`/`dlspeed`/`eta` in PostgreSQL via pipeline mode. Speed uses `(MAX(size)-MIN(size)) / (now - MIN(recorded_at))` so idle periods naturally decay the average.
- **Stalled detection**: `_progress_stalled()` checks local `progress.db` — torrents with no sample for 2+ days are evicted.
- **Slow eviction**: `_progress_slowest()` finds the slowest torrent; if total speed < `min_download_speed`, the slowest is evicted.
- **Exit**: When torrent state becomes uploading

### 3. Processing (uploading state)

- **Detected by**: Torrent state is uploading and no `BT_TAG_PROCESS_ERROR` tag
- **Entry**: `__process_torrents()` phase 3 detects uploading torrents
- **Action**: `__process_completed_torrent()` extracts mediainfo (BDMV or regular), checks hardcoded subtitles
- **Tags set**: Remove `BT_TAG_DOWNLOADING`, add `BT_TAG_PROCESSING`
- **Success exit**: Updates thread with mediainfo and `hard_coded_subtitle`, marks job done, deletes torrent + files, calls `_progress_forget()`
- **Failure exit**: Adds `BT_TAG_PROCESS_ERROR` tag, marks job failed in DB, calls `_progress_forget()`

### 4. Process Error (terminal)

- **Detected by**: `BT_TAG_PROCESS_ERROR in t.tags`
- **Behavior**: Torrent remains in client, skipped by all processing loops
- **Recovery**: Manual intervention required (or reset via web UI which deletes the job)

## Recovery: Stopped Torrents

In `__process_torrents()`, paused torrents are detected and resumed with tag swap (`-BT_TAG_SELECTING_FILES +BT_TAG_DOWNLOADING`).

## Cleanup

- **Stalled torrents**: Torrents with no progress for 2 days (detected via local `progress.db` in `_progress_stalled()`) are removed from client, thread marked `torrent_invalid = 'stalled'`, job marked `removed_from_download_client` (reason: `stalled`)
- **Torrent error state**: Torrents in `state.is_errored` are deleted, job marked failed with "torrent error"
- **Unselected category**: Torrents whose thread category is no longer in `SELECTED_CATEGORY` are deleted, job marked skipped
- **Removed from client**: If a torrent disappears from qb (user deleted), job is marked `removed-by-client` (reason: `"manual"`)
- **Unmanaged torrents**: Torrents not in any downloading job are deleted (with files); reclaimed if previously marked `removed-by-client`

## Progress Tracking (local SQLite)

Instead of writing to the remote PostgreSQL `job_download_size` table on every poll loop, each downloader node maintains a local SQLite database at `{data_dir}/progress.db`:

- **`_progress_record(hash, size)`**: Inserts sample only when `size` differs from last recorded value. Returns `True` if a new sample was added.
- **`_progress_avg_speed(hash, window=1800)`**: Average bytes/s using `(MAX(size)-MIN(size)) / (now - MIN(recorded_at))`. Uses current time as interval end so idle periods are reflected as decaying speed. Falls back 1800s → 600s → 0.
- **`_progress_stalled(hashes, cutoff)`**: Returns hashes with no sample after `cutoff` (default 2 days).
- **`_progress_slowest(cutoff)`**: Returns the slowest torrent for eviction decisions.
- **`_progress_forget(hash)`**: Drops all samples when a job terminates.
- **`_progress_cleanup(hashes)`**: Prunes samples for hashes no longer actively downloading.

## Related

- `app/bin/downloader.py` — All torrent processing logic (`Downloader.__process_torrents()`)
- `app/const.py` — Tag and status constants
- `thread-lifecycle` skill — Database-side thread state machine
