---
name: qb-torrent-lifecycle
description: 'qBittorrent torrent lifecycle stages and tag transitions. Use when working on torrent download, processing, or tag logic in app/application.py.'
user-invocable: false
---

# qBittorrent Torrent Lifecycle

Torrents managed by the application go through several stages in qBittorrent. The lifecycle is determined by **torrent state** (`state.is_paused`, `state.is_uploading`, etc.) and the `process-error` tag — NOT by other tags. Tags are purely informational labels to help users see what stage a torrent is in via the qBittorrent UI.

## Tags (informational only)

Tags are defined in `app/const.py`. They do NOT drive lifecycle logic — they are set/removed alongside state transitions to provide visibility.

| Constant | Value | Description |
|---|---|---|
| `QB_TAG_SELECTING_FILES` | `selecting-files` | Torrent is in stopped state, waiting for file selection |
| `QB_TAG_DOWNLOADING` | `downloading` | Torrent has been resumed and is actively downloading |
| `QB_TAG_PROCESSING` | `processing` | Download complete, extracting mediainfo |
| `QB_TAG_PROCESS_ERROR` | `process-error` | Mediainfo extraction failed (**exception**: this tag IS used in logic to skip retries) |

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
  Downloading... (state.is_paused=false, state.is_uploading=false)
  Progress updated in DB each interval
       │
       ▼
  Download complete (state.is_uploading=true)
  tag: -downloading +processing
       │
       ├── Success: extract mediainfo → update DB → torrents_delete
       │
       └── Failure: tag: +process-error, job marked failed in DB
```

## Stage Determination Logic

The code in `__process_local_torrents()` determines the stage using torrent state, NOT tags:

1. **Stopped/paused** (`state.is_paused`): Handled by `__resume_stopped_torrents()` — resumes the torrent
2. **Downloading** (`not state.is_uploading`): Updates progress in DB, fixes file selection if needed
3. **Uploading/seeding** (`state.is_uploading`): Processes mediainfo extraction
4. **Has `process-error` tag**: Skipped entirely (the one exception where a tag affects logic)

## Stage Details

### 1. File Selection (stopped)

- **Detected by**: `state.is_paused` (torrent added with `is_stopped=True`)
- **Entry**: `__add_to_qb()` adds torrent in stopped state
- **Action**: Parse torrent files, find largest video file, set all other files to priority 0
- **Exit**: Call `torrents_resume()` to start downloading
- **Tags set**: Remove `selecting-files`, add `downloading`
- **Error handling**: If any exception occurs, the torrent is deleted from qb and job is marked failed

### 2. Downloading (active)

- **Detected by**: `not state.is_paused and not state.is_uploading`
- **Entry**: After `torrents_resume()` in `__add_to_qb()`
- **Action**: `__process_local_torrents()` updates job progress in DB each interval
- **Also**: `__fix_file_selection()` corrects legacy torrents that are downloading all files (`total_size == size`)
- **Exit**: When `state.is_uploading` becomes true (download complete)

### 3. Processing (upload/seed state)

- **Detected by**: `state.is_uploading` and no `process-error` tag
- **Entry**: `__process_local_torrents()` detects upload state
- **Action**: `__process_local_torrent()` extracts mediainfo, checks hardcoded subtitles
- **Tags set**: Remove `downloading`, add `processing`
- **Success exit**: Updates thread with mediainfo, marks job done, deletes torrent from qb
- **Failure exit**: Adds `process-error` tag, marks job failed in DB

### 4. Process Error (terminal)

- **Detected by**: `QB_TAG_PROCESS_ERROR in t.tags`
- **Behavior**: Torrent remains in qb, skipped by all processing loops
- **Recovery**: Manual intervention required (or reset via web UI which deletes the job)

## Recovery: Stopped Torrents

`__resume_stopped_torrents()` runs each interval. It finds torrents where `state.is_paused` and resumes them:

- **Legacy torrents** (no tags): Resumes, adds `downloading` tag
- **Stuck in selecting-files**: Resumes, swaps tag to `downloading`
- **Has `process-error` tag**: Skipped (not auto-resumed)

## Cleanup

- **Old torrents**: `__cleanup_old_torrents()` deletes torrents where `seen_complete` is older than 10 days
- **Unselected category**: `__cleanup_unselected_category()` deletes torrents whose thread category is no longer in `SELECTED_CATEGORY`
- **Removed from client**: If a torrent disappears from qb (user deleted), job is marked `removed-by-client`
