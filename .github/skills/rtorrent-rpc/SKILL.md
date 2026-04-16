---
name: rtorrent-rpc
description: 'rtorrent-rpc library usage patterns and rTorrent RPC API. Use when working on rTorrent integration in app/rt.py or adding new rTorrent operations.'
user-invocable: false
---

# rTorrent RPC via rtorrent-rpc

This project uses the [`rtorrent-rpc`](https://github.com/trim21/rtorrent-rpc) Python library (>=0.9.4) to communicate with rTorrent over SCGI (TCP or Unix socket) or HTTP (nginx proxy). The wrapper lives in `app/rt.py` as `RTorrentClient`, which implements the `DownloadClient` protocol defined in `app/download_client.py`.

## Connection & Transport

```python
from rtorrent_rpc import RTorrent

# SCGI over TCP
rt = RTorrent("scgi://127.0.0.1:5000")
# SCGI over Unix socket
rt = RTorrent("scgi:///path/to/rtorrent.sock")
# HTTP (nginx scgi proxy) — /RPC2 path is appended automatically
rt = RTorrent("http://127.0.0.1:8080")
```

Constructor parameters:

| Parameter | Default | Description |
|---|---|---|
| `address` | — | `scgi://`, `http://`, or `https://` URL |
| `rutorrent_compatibility` | `True` | Store add-time, tags in `d.custom1`, comment in `d.custom2` (compatible with ruTorrent/Flood) |
| `timeout` | `5.0` | Socket timeout in seconds. In this project we use `30` |

The library exposes three call interfaces:
- **`rt.rpc.*`** — `xmlrpc.client.ServerProxy` for XML-RPC calls (always available)
- **`rt.jsonrpc.call(method, params)`** — JSON-RPC (only on jesec/rtorrent forks with JSON-RPC support)
- **`rt._transport.request(data, content_type)`** — Raw SCGI transport

In this project, `_RTorrent` subclass adds a `call()` method that auto-detects JSON-RPC support and falls back to XML-RPC.

## RPC Method Naming Convention

rTorrent uses a hierarchical dot-separated naming scheme:

| Prefix | Scope | Example |
|---|---|---|
| `d.*` | Download (torrent) | `d.name=`, `d.hash=`, `d.start`, `d.stop` |
| `f.*` | File within a download | `f.path=`, `f.size_bytes=`, `f.priority.set` |
| `t.*` | Tracker within a download | `t.url=`, `t.is_enabled.set` |
| `p.*` | Peer within a download | `p.address`, `p.down_rate` |
| `system.*` | System-level | `system.multicall`, `system.listMethods` |
| `load.*` | Load torrents | `load.raw_start_verbose` |
| `throttle.*` | Speed limits | `throttle.up` |
| `choke_group.*` | Choke group management | `choke_group.list` |

Getter methods end with `=` in multicall context (e.g., `"d.name="`). Setter methods use `.set` suffix (e.g., `"d.directory_base.set"`).

## Multicall Patterns

### d.multicall2 — Batch-query all torrents

```python
raw = rt.call("d.multicall2", [
    "",           # first arg is always empty string
    "default",    # view name ("default" = all, "main", "started", "stopped", etc.)
    "d.name=",
    "d.hash=",
    "d.directory_base=",
    "d.custom1=",       # tags (ruTorrent compat)
    "d.size_bytes=",
    "d.completed_bytes=",
    "d.up.total=",
    "d.is_open=",
    "d.state=",
    "d.complete=",
    "d.down.rate=",
])
# Returns: list[list[Any]] — each inner list has values in the order of the commands
for row in raw:
    name, hash_, directory, tags_raw, size, completed, uploaded, is_open, state, complete, dlrate = (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]
    )
```

### f.multicall — List files of a torrent

```python
raw = rt.call("f.multicall", [
    info_hash,
    "",           # second arg is always empty string
    "f.path=",
    "f.size_bytes=",
    "f.priority=",
    "f.completed_chunks=",
    "f.size_chunks=",
])
# Returns: list[list[Any]] — one entry per file
```

### system.multicall — Batch multiple independent calls

```python
from rtorrent_rpc import MultiCall

rt.system.multicall([
    MultiCall(methodName="d.stop", params=[info_hash]),
    MultiCall(methodName="d.close", params=[info_hash]),
])
```

Used for atomic multi-step operations (e.g., stop+close, open+start).

## Torrent State Fields

Three fields determine torrent state:

| Field | Values | Meaning |
|---|---|---|
| `d.complete` | 0 / 1 | All selected data downloaded |
| `d.is_open` | 0 / 1 | Torrent handle is open (active) |
| `d.state` | 0 / 1 | 0 = stopped, 1 = started (leeching or seeding) |

State mapping in `app/rt.py`:

```
complete=1 AND state=1  →  TorrentState.seeding
is_open=0 OR state=0    →  TorrentState.paused
otherwise               →  TorrentState.downloading
```

## Common Operations

### Add torrent

```python
rt.add_torrent_by_file(
    content=torrent_bytes,
    directory_base="/data/downloads",
    tags=["tag1", "tag2"],              # stored in d.custom1 (ruTorrent format)
    extras=["d.throttle.max=1024"],     # extra commands run on add
    custom={"mykey": "myvalue"},        # stored via d.custom.set
)
```

The method calls `load.raw_start_verbose` internally. The torrent starts immediately.

**Important**: rTorrent needs time to process the torrent after add. The info_hash may not be queryable immediately.

### Stop / Start torrent

```python
# Stop = d.stop + d.close
rt.stop_torrent(info_hash)

# Start = d.open + d.start
rt.start_torrent(info_hash)
```

### Delete torrent

```python
# Stop first, then erase
rt.call("d.stop", [info_hash])
rt.call("d.erase", [info_hash])
```

Note: `d.erase` only removes the torrent from rTorrent. It does NOT delete files on disk. File deletion must be handled separately.

### Tags (ruTorrent compatible)

Tags are stored in `d.custom1` as comma-separated URL-encoded strings.

```python
from rtorrent_rpc.helper import parse_tags

# Read tags
raw_tags: str = rt.d_get_custom(info_hash, "1")
tags: set[str] = parse_tags(raw_tags)

# Write tags
rt.d_set_tags(info_hash, {"tag1", "tag2", "tag3"})
```

### File priority

File IDs use the format `{info_hash}:f{index}` (0-based):

```python
# Set file priority: 0=skip, 1=normal, 2=high
rt.call("f.priority.set", [f"{info_hash}:f0", 1])

# Must call update_priorities to apply
rt.call("d.update_priorities", [info_hash])
```

### Download speed limit

```python
# Per-torrent limit (bytes/s), 0 = unlimited
rt.call("d.throttle.max.set", [info_hash, limit])
```

### Tracker operations

```python
# Add tracker
rt.d_add_tracker(info_hash, "https://tracker.example.com/announce", group=0)

# Enable/disable tracker by index
rt.t_enable_tracker(info_hash, tracker_index=0)
rt.t_disable_tracker(info_hash, tracker_index=0)
```

## Helper Utilities

`rtorrent_rpc.helper` provides:

| Function | Description |
|---|---|
| `parse_tags(s)` | Parse `d.custom1` string → `set[str]` |
| `parse_comment(s)` | Parse `d.custom2` string → comment string |
| `get_torrent_info_hash(content)` | SHA1 info_hash from `.torrent` bytes |
| `add_fast_resume_file(path, content)` | Inject resume data (skip hash check) — **use with caution** |
| `add_completed_resume_file(path, content)` | Inject completed resume data — **use with caution** |

## Project-Specific Wrapper: RTorrentClient

`app/rt.py` wraps `rtorrent_rpc.RTorrent` into `RTorrentClient` implementing the `DownloadClient` protocol:

| Protocol Method | rTorrent Implementation |
|---|---|
| `connect()` | `system.listMethods()` — verifies connection |
| `list_torrents()` | `d.multicall2` with 14 fields |
| `list_files(hash)` | `f.multicall` with 5 fields |
| `add_torrent(data, ...)` | `add_torrent_by_file()` with extras for throttle |
| `delete_torrent(hash)` | `d.stop` + `d.erase` (suppress errors) |
| `pause_torrent(hash)` | `stop_torrent()` (d.stop + d.close) |
| `resume_torrent(hash)` | `start_torrent()` (d.open + d.start) |
| `set_download_limit(hash, limit)` | `d.throttle.max.set` |
| `add_tags(hash, tags)` | Read current → merge → `d_set_tags()` |
| `remove_tags(hash, tags)` | Read current → subtract → `d_set_tags()` |
| `set_file_priority(hash, ids, pri)` | Loop `f.priority.set` per file ID |

## JSON-RPC vs XML-RPC

The `_RTorrent` subclass in `app/rt.py` adds dual-protocol support:

1. First call attempts JSON-RPC
2. If `json.JSONDecodeError` → falls back to XML-RPC for all subsequent calls
3. JSON-RPC is only available on jesec/rtorrent forks

**Known limitation**: Standard rTorrent's xmlrpc-c does not support all UTF-8 characters (e.g., emoji). Use jesec/rtorrent with JSON-RPC for full Unicode support.

## Related Files

- `app/rt.py` — `RTorrentClient` and `_RTorrent` wrapper classes
- `app/download_client.py` — `DownloadClient` protocol, `TorrentState`, `ClientTorrent`, `ClientFile`
- `app/node.py` — Node loop that uses `DownloadClient` (selects RTorrent or qBittorrent via config)
- `app/config.py` — `rt_url` config field for rTorrent address
