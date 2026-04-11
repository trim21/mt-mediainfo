# AGENTS.md - AI Agent Guide for pt-repost

## Project Overview

**pt-repost** is a Python application for automatically downloading torrents from M-Team (a private tracker), processing them (extracting mediainfo, checking for hardcoded subtitles), and reposting them. The system uses PostgreSQL for data persistence and provides both a CLI interface and a FastAPI web server. It supports multi-node distributed processing coordinated via PostgreSQL advisory locks.

## Architecture

### Core Components

- **[`app/application.py`](app/application.py)** - Main node-mode application logic: picks pending jobs, adds torrents to qBittorrent, monitors downloads, extracts mediainfo, detects hardcoded subtitles, updates job lifecycle
- **[`app/scrape.py`](app/scrape.py)** - Background scraper: discovers threads via M-Team search API, fetches mediainfo from API, downloads `.torrent` files, backfills selected sizes
- **[`app/mt.py`](app/mt.py)** - M-Team API client: download torrents, fetch details/mediainfo, search with pagination and date ranges
- **[`app/db/__init__.py`](app/db/__init__.py)** - PostgreSQL database layer using psycopg with connection pooling (min=1, max=3), provides `Connection` wrapper with `fetch_val`, `fetch_one`, `fetch_all` helpers
- **[`app/db/dlock/__init__.py`](app/db/dlock/__init__.py)** - Distributed locking via PostgreSQL advisory locks with xxhash key conversion, thread-safe (OS lock + PG lock), supports shared/exclusive modes
- **[`app/server.py`](app/server.py)** - FastAPI web server with Jinja2 templates, asyncpg connection pool, Polars-based analytics
- **[`app/torrent.py`](app/torrent.py)** - Torrent file parsing using bencode2, file selection logic
- **[`app/mediainfo.py`](app/mediainfo.py)** - MediaInfo extraction via external `mediainfo` binary
- **[`app/hardcode_subtitle.py`](app/hardcode_subtitle.py)** - Hardcoded Chinese subtitle detection via FFmpeg screenshots + RapidOCR
- **[`app/config.py`](app/config.py)** - Pydantic dataclass-based configuration from environment variables
- **[`app/const.py`](app/const.py)** - Constants for statuses, lock keys, categories, qBittorrent tags, file extensions
- **[`app/kv.py`](app/kv.py)** - Simple key-value store backed by the `config` database table
- **[`app/utils.py`](app/utils.py)** - Utility functions: subprocess wrappers, size formatting, info_hash extraction, torrent comment injection
- **[`app/patterns.py`](app/patterns.py)** - Regex patterns for season suffix, web-dl, Dolby Vision, 2160p detection

### Database Schema

The application uses PostgreSQL with the following tables (see [`app/sql/schema.sql`](app/sql/schema.sql)):

- **thread** - Stores torrent thread information (tid, size, selected_size, mediainfo, info_hash, category, seeders, hard_coded_subtitle, deleted, timestamps)
- **job** - Tracks download jobs per node (tid, node_id, info_hash, progress, status, failed_reason, timestamps)
- **node** - Manages distributed nodes (id, last_seen)
- **torrent** - Stores raw `.torrent` file content and info hashes
- **config** - Key-value store for runtime configuration (search cursors, etc.)

Please notice that the `schema.sql` is used as init.sql and migration,
so if you want to update database schema, use SQL like `alter ... if not exists ...`

## Tech Stack

- **Language**: Python 3.12.12
- **Web Framework**: FastAPI 0.135.3
- **Database**: PostgreSQL (psycopg 3.3.3 for sync, asyncpg 0.31.0 for async in server)
- **Torrent Client**: qBittorrent (via qbittorrent-api 2025.11.1)
- **HTTP Client**: httpx 0.28.1
- **Validation**: Pydantic 2.12.5
- **CLI**: Click 8.3.2
- **Data Analysis**: Polars 1.39.3 (for weekly/daily chart aggregations)
- **OCR**: rapidocr-onnxruntime 1.4.4
- **Logging**: sslog 0.0.0a52
- **External Tools**: MediaInfo, FFmpeg

## Thread Lifecycle States

A thread goes through the following states after being discovered by `scrape_search()`:

1. **Pending Fetch Mediainfo** — Thread discovered but mediainfo not yet fetched from the API.
   - Condition: `mediainfo_at IS NULL AND deleted = false`
   - Action: `scrape_mediainfo()` or `scrape_detail()` calls M-Team API to fetch mediainfo text.

2. **Pending Fetch Torrent (info_hash)** — Mediainfo fetched via API but returned empty; torrent file not yet downloaded to obtain `info_hash`.
   - Condition: `mediainfo_at IS NOT NULL AND mediainfo = '' AND info_hash = ''`
   - Action: `fetch_torrent()` downloads the `.torrent` file, parses it to extract `info_hash` and `selected_size`.

3. **Pending to Download** — Mediainfo empty after API check, torrent file already fetched (has `info_hash`), ready for a node to download the actual content and extract mediainfo locally.
   - Condition: `mediainfo = '' AND info_hash != '' AND selected_size > 0`
   - Action: `__pick_and_add_jobs()` in `application.py` assigns it to a node (with advisory lock); the node downloads via qBittorrent, runs MediaInfo on the files, and updates the thread.

4. **Done** — Mediainfo successfully obtained (either from the API or from local extraction).
   - Condition: `mediainfo_at IS NOT NULL AND mediainfo != ''`

## qBittorrent Torrent Lifecycle

Torrents in qBittorrent go through these tag-based stages:

1. **Added (stopped)** — Tag: `need-select`. Action: select largest video file, set other files to priority 0.
2. **Resuming** — Tag changes: `-need-select +downloading`. Active download begins.
3. **Uploading/Seeding** — Tag changes: `-downloading +processing`. Action: extract mediainfo from downloaded file.
4. **Success** — Torrent deleted from qBittorrent, job updated to `done` in database.
5. **Error** — Tag: `+process-error`. Requires manual intervention or reset via web UI.

## Key Workflows

### 1. Node Mode (`python main.py node`)

The main application mode that:
1. Runs main loop every 60 seconds (first iteration after 1 second):
   - Heartbeats to database (updates `node.last_seen`)
2. Each iteration:
   - Processes existing qBittorrent torrents (progress, file selection, mediainfo extraction)
   - Picks new pending threads and adds them to qBittorrent (with distributed advisory lock)
3. Monitors download progress and handles state transitions
4. Extracts mediainfo from completed downloads
5. Checks for hardcoded Chinese subtitles via OCR
6. Respects size limits: `total_process_size` (100GiB) and `single_torrent_size_limit` (10GiB)

### 2. Scrape Mode (`python main.py scrape`)

Background scraper that runs continuously with rate limiting:
1. `scrape_search()` — Paginates M-Team search API by creation date, maintains cursor in config table
2. `scrape_detail()` — Fetches full torrent details, fills tid gaps
3. `scrape_mediainfo()` — Calls `/api/torrent/mediaInfo` for threads pending mediainfo
4. `fetch_torrent()` — Downloads `.torrent` files, extracts `info_hash` and `selected_size`
5. `backfill_selected_size()` — Backfills `selected_size` for existing torrents

Each operation has independent rate-limit tracking with 60-minute cooldown on M-Team rate-limit.

### 3. Web Server (`uvicorn app.server:create_app --factory`)

FastAPI server providing dashboard and API:
- Progress dashboard with stats and weekly charts (Chart.js)
- Thread listings by status (pending, downloading, done, failed, removed)
- Node overview with job counts and ETA calculations
- Detail page with daily/weekly analytics (Polars aggregations)
- JSON API for programmatic access

## API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/` | Main progress dashboard |
| `GET` | `/detail` | Weekly/daily detail charts (with optional `?start=YYYY-MM-DD`) |
| `GET` | `/threads/pending-mediainfo` | HTML: waiting for API mediainfo |
| `GET` | `/threads/pending-torrent` | HTML: waiting for torrent download |
| `GET` | `/threads/pending-download` | HTML: ready to download |
| `GET` | `/threads/downloading` | HTML: currently downloading |
| `GET` | `/threads/done` | HTML: completed with mediainfo |
| `GET` | `/threads/failed` | HTML: failed with error reasons |
| `GET` | `/threads/removed` | HTML: removed from qBittorrent |
| `POST` | `/api/thread/{tid}/reset` | JSON: delete failed/removed jobs for a thread |
| `GET` | `/nodes` | HTML: node overview with job counts |
| `GET` | `/nodes/{node_id}` | HTML: node-specific jobs with ETA |

## Configuration

Configuration is managed via environment variables (see [`app/config.py`](app/config.py)):

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `MT_API_TOKEN` | str | **Required** | M-Team API token |
| `NODE_ID` | uuid | MAC address | Unique node identifier |
| `DEBUG` | bool | `False` | Debug mode |
| `PG_HOST` | str | `127.0.0.1` | PostgreSQL host |
| `PG_PORT` | int | `5432` | PostgreSQL port |
| `PG_DB` | str | `postgres` | PostgreSQL database name |
| `PG_USER` | str | `postgres` | PostgreSQL user |
| `PG_PASSWORD` | str | `postgres` | PostgreSQL password |
| `PG_SSLMODE` | str | None | SSL mode (disable, allow, prefer, require) |
| `PG_SSL_ROOTCERT` | str | None | SSL root certificate path |
| `PG_SSL_CERT` | str | None | SSL certificate path |
| `PG_SSL_KEY` | str | None | SSL key path |
| `QB_URL` | HttpUrl | `http://127.0.0.1:8084` | qBittorrent Web API URL |
| `DOWNLOAD_PATH` | str | `~/downloads` | Download directory |
| `TOTAL_SIZE` | ByteSize | `100GiB` | Total processing size limit |
| `SINGLE_TORRENT_SIZE_LIMIT` | ByteSize | `10GiB` | Single torrent size limit |
| `HTTP_PROXY` | str | None | HTTP proxy URL |
| `SCRAPE_LIMIT` | int | `100` | Scrape batch limit |

## Status Constants

Defined in [`app/const.py`](app/const.py):

**Job/Item Statuses:**
- `pending` — Initial state
- `skipped` — Skipped (e.g., size limit exceeded)
- `downloading` — Currently downloading
- `uploading` — Downloaded, being processed
- `done` — Completed successfully
- `failed` — Failed with error
- `removed-by-site` — Removed from M-Team
- `removed-by-client` — Removed from qBittorrent

**qBittorrent Tags:**
- `selecting-files` — File selection in progress
- `downloading` — Active download
- `processing` — Mediainfo extraction in progress
- `process-error` — Processing failed, needs manual intervention

**Lock Keys:**
- `LOCK_KEY_SCHEDULE_RSS` — Scrape scheduling lock
- `LOCK_KEY_PICK_RSS_JOB` — Job picking lock (ensures single-node picks at a time)

**Categories:**
- `MOVIE_CATEGORY` = {401, 419, 420, 421, 439}
- `SELECTED_CATEGORY` = [401, 419, 420, 421, 439, 403, 402, 438, 435, 404, 406, 405, 407]

## Development

### Prerequisites

- Python 3.12.12
- PostgreSQL
- qBittorrent
- MediaInfo
- FFmpeg

### Setup

```bash
# Install dependencies
uv sync

# Create .env file with required variables
cp .env.example .env
# Edit .env with your configuration

# Run database migrations
# Execute app/sql/schema.sql in your PostgreSQL database
```

### Running Locally

```bash
# Run the node (with auto-reload)
task dev

# Run the node (production)
task node

# Run the scraper
task scrape

# Run the web server (with auto-reload on .py, .sql, .j2 changes)
task dev:server

# Run all checks (ruff check, ruff format, pyright, mypy, ty)
task default
```

### Docker

Multi-stage Dockerfile using `ghcr.io/astral-sh/uv:python3.12-bookworm-slim` for dependency export and `python:3.12-slim` for runtime. Installs `mediainfo` and `ffmpeg` via apt.

```bash
# Build the image
docker build -t pt-repost .

# Run the container
docker run -d \
  --env-file .env \
  -v /path/to/downloads:/downloads \
  pt-repost node
```

### CI/CD

- **build.yaml** — Docker build + push to `ghcr.io` on master branch, auto-cleans old package versions
- **lint.yaml** — MyPy (strict), pre-commit hooks, Pyright type checking
- **Mergify** — Auto-merge configuration
- **Renovate** — Automated dependency updates

## Code Style & Conventions

- **Formatting**: Ruff format (line-length 100)
- **Linting**: Ruff with preview mode enabled
- **Type Checking**: MyPy (strict mode) + Pyright
- **Imports**: Sorted with Ruff (I rule)
- **Logging**: Using `sslog` library
- **Pre-commit**: Hooks configured via `.pre-commit-config.yaml`

### Key Patterns

1. **Dataclasses**: Used extensively for data models (e.g., `Torrent`, `Config`, `QbTorrent`, `QbFile`)
2. **Pydantic**: For API response validation and type coercion (e.g., `TorrentDetail`, `SearchResult`)
3. **Context Managers**: For database connections, transactions, and distributed locks
4. **Distributed Locking**: PostgreSQL advisory locks via xxhash for multi-node coordination
5. **Rate Limiting**: Per-operation independent cooldown timers (60-minute cooldown on M-Team rate-limit)
6. **Parallel Async Queries**: `asyncio.gather()` in server for concurrent database queries

## Error Handling

- **M-Team API Errors**: Custom `MTeamRequestError` with code/message. Rate-limiting detected by Chinese messages ("請求過於頻繁", "今日下載配額用盡") triggers 60-minute cooldown per operation.
- **Network Errors**: Caught as `httpx.NetworkError`, `httpx.TransportError`, `httpx.TimeoutException`, `ConnectionError`, `TimeoutError`, `httpcore.TimeoutException`
- **Invalid Torrents**: Marked as `invalid torrent` in database mediainfo field
- **Download Failures**: Job marked with `failed_reason`, torrent tagged `process-error` in qBittorrent
- **Manual Recovery**: Failed/removed jobs can be reset via `POST /api/thread/{tid}/reset`

## Common Tasks

### Adding a New Status

1. Add constant to [`app/const.py`](app/const.py)
2. Update status handling in [`app/application.py`](app/application.py)
3. Update web interface templates if needed

### Modifying Torrent Processing

1. Edit [`app/application.py`](app/application.py) for download/processing logic
2. Edit [`app/mediainfo.py`](app/mediainfo.py) for mediainfo extraction
3. Edit [`app/hardcode_subtitle.py`](app/hardcode_subtitle.py) for subtitle detection

### Adding API Endpoints

1. Add routes to [`app/server.py`](app/server.py)
2. Create templates in [`app/templates/`](app/templates/) if needed
3. Add database queries using asyncpg in the route handler

### Modifying Database Schema

1. Edit [`app/sql/schema.sql`](app/sql/schema.sql) using `ALTER ... IF NOT EXISTS` patterns
2. The schema file serves as both init SQL and migration

## Dependencies

Key dependencies (see [`pyproject.toml`](pyproject.toml)):

| Package | Version | Purpose |
|---------|---------|---------|
| `qbittorrent-api` | 2025.11.1 | qBittorrent Web API client |
| `httpx` | 0.28.1 | HTTP client for M-Team API |
| `psycopg[binary,pool]` | 3.3.3 | Sync PostgreSQL adapter with connection pooling |
| `asyncpg` | 0.31.0 | Async PostgreSQL driver (server) |
| `pydantic` | 2.12.5 | Data validation and settings |
| `fastapi` | 0.135.3 | Web framework |
| `uvicorn` | 0.43.0 | ASGI server |
| `bencode2` | 0.3.29 | Torrent file parsing |
| `polars` | 1.39.3 | Data analysis for chart aggregations |
| `rapidocr-onnxruntime` | 1.4.4 | OCR for hardcoded subtitle detection |
| `click` | 8.3.2 | CLI framework |
| `jinja2` | 3.1.6 | Template engine |
| `xxhash` | 3.6.0 | Fast hashing for advisory lock keys |
| `orjson` | 3.11.8 | Fast JSON serialization |
| `tenacity` | 9.1.4 | Retry logic |
| `sslog` | 0.0.0a52 | Structured logging |

## Project Structure

```
pt-repost/
├── main.py                      # Entry point (calls app.main:cli)
├── pyproject.toml               # Package config, dependencies
├── taskfile.yaml                # Task runner commands
├── Dockerfile                   # Multi-stage Docker build
├── uv.lock                      # Dependency lock file
├── .pre-commit-config.yaml      # Pre-commit hooks
├── app/
│   ├── __init__.py
│   ├── main.py                  # Click CLI (node, scrape commands)
│   ├── application.py           # Node-mode application logic
│   ├── scrape.py                # M-Team scraper
│   ├── mt.py                    # M-Team API client
│   ├── config.py                # Environment-based configuration
│   ├── const.py                 # Status constants, categories, tags
│   ├── torrent.py               # Torrent parsing (bencode)
│   ├── mediainfo.py             # MediaInfo extraction
│   ├── hardcode_subtitle.py     # OCR subtitle detection
│   ├── kv.py                    # DB-backed key-value store
│   ├── utils.py                 # Utility functions
│   ├── patterns.py              # Regex patterns
│   ├── server.py                # FastAPI web server
│   ├── db/
│   │   ├── __init__.py          # Database layer (psycopg pooling)
│   │   └── dlock/
│   │       └── __init__.py      # Distributed advisory locks
│   ├── sql/
│   │   └── schema.sql           # PostgreSQL schema (init + migration)
│   └── templates/
│       ├── index.html.j2        # Main progress dashboard
│       ├── detail.html.j2       # Weekly/daily analytics charts
│       ├── threads.html.j2      # Generic thread listing
│       ├── nodes.html.j2        # Node overview
│       └── node_jobs.html.j2    # Node-specific jobs with ETA
└── .github/
    ├── workflows/
    │   ├── build.yaml           # Docker build + push to ghcr.io
    │   └── lint.yaml            # MyPy, pre-commit, Pyright
    ├── skills/                  # AI agent skill definitions
    ├── mergify.yml              # Auto-merge config
    └── renovate.json            # Dependency update automation
```

## Notes for AI Agents

- The project uses Python 3.12.12 specifically
- Configuration is primarily via environment variables
- The application is designed to run as a long-lived service
- Multi-node support via PostgreSQL advisory locks
- Rate limiting is handled automatically for M-Team API with per-operation cooldowns
- Video files are processed for mediainfo and hardcoded subtitle detection
- The `schema.sql` serves as both initial schema and migration — use `ALTER ... IF NOT EXISTS` for schema changes
- The server uses asyncpg (async) while the node/scraper use psycopg (sync) for database access
- Polars is used for data aggregation in the web server's chart endpoints
- Web UI uses Bootstrap 5.3.3 and Chart.js for visualization
