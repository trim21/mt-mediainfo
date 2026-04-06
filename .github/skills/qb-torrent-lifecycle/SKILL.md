---
name: qb-torrent-lifecycle
description: 'qBittorrent torrent lifecycle stages and tag transitions. Use when working on torrent download, processing, or tag logic in app/application.py.'
user-invocable: false
---

# qBittorrent Torrent Lifecycle

Torrents managed by the application in qBittorrent go through a series of tagged stages. Tags are defined in `app/const.py` and managed in `app/application.py`.

## Tags

| Constant | Value | Description |
|---|---|---|
| `QB_TAG_SELECTING_FILES` | `selecting-files` | Torrent added in stopped state, waiting for file selection |
| `QB_TAG_DOWNLOADING` | `downloading` | File selection done, torrent resumed and actively downloading |
| `QB_TAG_PROCESSING` | `processing` | Download complete (uploading state), extracting mediainfo |
| `QB_TAG_PROCESS_ERROR` | `process-error` | Mediainfo extraction failed, torrent will not be retried automatically |

## Lifecycle Stages

```
torrents_add (stopped)
  tag: selecting-files
       │
       ▼
  Select largest video file (set other files priority=0)
       │
       ▼
  torrents_resume
  tag: -selecting-files +downloading
       │
       ▼
  Downloading... (state.is_downloading)
  Progress updated in DB each interval
       │
       ▼
  Download complete (state.is_uploading)
  tag: -downloading +processing
       │
       ├── Success: extract mediainfo → update DB → torrents_delete
       │
       └── Failure: tag: +process-error, job marked failed in DB
```

## Stage Details

### 1. selecting-files (stopped)

- **Entry**: `__add_to_qb()` adds torrent with `is_stopped=True, tags=QB_TAG_SELECTING_FILES`
- **Action**: Parse torrent files, find largest video file, set all other files to priority 0
- **Exit**: Remove `selecting-files` tag, add `downloading` tag, call `torrents_resume()`
- **Error handling**: If any exception occurs, the torrent is deleted from qb and job is marked failed

### 2. downloading (active)

- **Entry**: After file selection completes in `__add_to_qb()`
- **Action**: `__process_local_torrents()` updates job progress in DB each interval
- **Also**: `__fix_file_selection()` corrects legacy torrents that are downloading all files (`total_size == size`)
- **Exit**: When `state.is_uploading` (download complete), transition to processing

### 3. processing (uploading)

- **Entry**: `__process_local_torrents()` detects `state.is_uploading`, swaps tag from `downloading` to `processing`
- **Action**: `__process_local_torrent()` extracts mediainfo, checks hardcoded subtitles
- **Success exit**: Updates thread with mediainfo, marks job done, deletes torrent from qb
- **Failure exit**: Adds `process-error` tag, marks job failed in DB

### 4. process-error (terminal)

- **Entry**: When `__process_local_torrent()` throws an exception
- **Behavior**: Torrent remains in qb, skipped by all processing loops
- **Recovery**: Manual intervention required (or reset via web UI which deletes the job)

## Recovery: Stopped Torrents

`__resume_stopped_torrents()` runs each interval and handles:

- **Legacy torrents** (no tags): Adds `downloading` tag and resumes
- **Stuck selecting-files**: Removes `selecting-files`, adds `downloading`, resumes
- **process-error**: Skipped (not auto-resumed)

## Cleanup

- **Old torrents**: `__cleanup_old_torrents()` deletes torrents where `seen_complete` is older than 10 days
- **Unselected category**: `__cleanup_unselected_category()` deletes torrents whose thread category is no longer in `SELECTED_CATEGORY`
- **Removed from client**: If a torrent disappears from qb (user deleted), job is marked `removed-by-client`
