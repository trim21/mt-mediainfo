# AGENTS.md - AI Agent Guide for pt-repost

## Project Overview

**pt-repost** is a Python application for automatically downloading torrents from M-Team (a private tracker), processing them (extracting mediainfo, checking for hardcoded subtitles), and reposting them. The system uses PostgreSQL for data persistence and provides both a CLI interface and a FastAPI web server.

## Architecture

### Core Components

- **[`app/application.py`](app/application.py)** - Main application logic for processing torrents, downloading from qBittorrent, extracting mediainfo, and managing job lifecycle
- **[`app/scrape.py`](app/scrape.py)** - Scraping M-Team for torrent details and downloading torrent files
- **[`app/mt.py`](app/mt.py)** - M-Team API client for torrent operations (download, detail retrieval)
- **[`app/db.py`](app/db.py)** - PostgreSQL database operations using psycopg with connection pooling
- **[`app/server.py`](app/server.py)** - FastAPI web server for RSS feeds and web interface
- **[`app/torrent.py`](app/torrent.py)** - Torrent file parsing using bencode2
- **[`app/mediainfo.py`](app/mediainfo.py)** - MediaInfo extraction from video files
- **[`app/dlock/__init__.py`](app/db/dlock/__init__.py)** - Distributed locking using PostgreSQL advisory locks
- **[`app/config.py`](app/config.py)** - Configuration management using Pydantic with environment variables
- **[`app/const.py`](app/const.py)** - Constants for status codes, categories, and file extensions

### Database Schema

The application uses PostgreSQL with the following tables (see [`app/sql/schema.sql`](app/sql/schema.sql)):

- **thread** - Stores torrent thread information (tid, size, mediainfo, category, seeders)
- **job** - Tracks download jobs with progress and status
- **node** - Manages distributed nodes
- **torrent** - Stores raw torrent content and info hashes

## Tech Stack

- **Language**: Python 3.12.12
- **Web Framework**: FastAPI 0.135.2
- **Database**: PostgreSQL (via psycopg 3.3.3 and asyncpg 0.31.0)
- **Torrent Client**: qBittorrent (via qbittorrent-api 2025.11.1)
- **HTTP Client**: httpx 0.28.1
- **Validation**: Pydantic 2.12.5
- **CLI**: Click 8.3.1
- **External Tools**: MediaInfo, FFmpeg

## Thread Lifecycle States

A thread goes through the following states after being discovered by `scrape_search()`:

1. **Pending Fetch Mediainfo** — Thread discovered but mediainfo not yet fetched from the API.
   - Condition: `mediainfo_at IS NULL`
   - Action: `scrape_mediainfo()` calls `/api/torrent/mediaInfo` to fetch mediainfo text.

2. **Pending Fetch Torrent (info_hash)** — Mediainfo fetched via API but returned empty; torrent file not yet downloaded to obtain `info_hash`.
   - Condition: `mediainfo_at IS NOT NULL AND mediainfo = '' AND info_hash = ''`
   - Action: `fetch_torrent()` downloads the `.torrent` file, parses it to extract `info_hash`.

3. **Pending to Download** — Mediainfo empty after API check, torrent file already fetched (has `info_hash`), ready for a node to download the actual content and extract mediainfo locally.
   - Condition: `mediainfo = '' AND info_hash != ''` (`mediainfo_at IS NOT NULL` is implied because `fetch_torrent()` requires it)
   - Action: `pick_job()` in `application.py` assigns it to a node; the node downloads via qBittorrent, runs MediaInfo on the files, and updates the thread.

4. **Done** — Mediainfo successfully obtained (either from the API or from local extraction).
   - Condition: `mediainfo_at IS NOT NULL AND mediainfo != ''`

## Key Workflows

### 1. Node Mode (`python main.py node`)

The main application mode that:
1. Polls the database for pending jobs
2. Downloads torrents from M-Team via API
3. Adds torrents to qBittorrent for downloading
4. Monitors download progress
5. Extracts mediainfo from downloaded video files
6. Checks for hardcoded Chinese subtitles
7. Updates job status in the database

### 2. Scrape Mode (`python main.py scrape`)

Background scraper that:
1. Fetches torrent details from M-Team API
2. Stores thread information in the database
3. Downloads torrent files for threads without info hashes
4. Runs continuously with rate limiting

### 3. Web Server (`uvicorn app.server:create_app`)

FastAPI server providing:
- RSS feed generation
- Web interface for monitoring progress
- JSON API endpoints

## Configuration

Configuration is managed via environment variables (see [`app/config.py`](app/config.py)):

| Variable | Description | Default |
|----------|-------------|---------|
| `MT_API_TOKEN` | M-Team API token | **Required** |
| `NODE_ID` | Unique node identifier | Auto-generated UUID |
| `PG_HOST` | PostgreSQL host | `127.0.0.1` |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_USER` | PostgreSQL user | `postgres` |
| `PG_PASSWORD` | PostgreSQL password | `postgres` |
| `QB_URL` | qBittorrent URL | `http://127.0.0.1:8084` |
| `DOWNLOAD_PATH` | Download directory | `~/downloads` |
| `TOTAL_SIZE` | Total process size limit | `100G` |
| `SINGLE_TORRENT_SIZE_LIMIT` | Single torrent size limit | `10G` |
| `HTTP_PROXY` | HTTP proxy | None |
| `DEBUG` | Debug mode | `False` |
| `SCRAPE_LIMIT` | Scrape batch limit | `100` |

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
# Run the node
task node

# Run the scraper
task scrape

# Run the web server
task dev:server

# Run all checks (linting, formatting, type checking)
task default
```

### Docker

```bash
# Build the image
docker build -t pt-repost .

# Run the container
docker run -d \
  --env-file .env \
  -v /path/to/downloads:/downloads \
  pt-repost node
```

## Code Style & Conventions

- **Formatting**: Black with line-length 100
- **Linting**: Ruff with preview mode enabled
- **Type Checking**: MyPy with strict mode
- **Imports**: Sorted with Ruff (I rule)
- **Error Handling**: Custom exceptions for domain-specific errors
- **Logging**: Using `sslog` library

### Key Patterns

1. **Dataclasses**: Used extensively for data models (e.g., [`Torrent`](app/torrent.py:76), [`Config`](app/config.py:24))
2. **Pydantic**: For validation and type coercion (e.g., [`TorrentDetail`](app/mt.py:126))
3. **Context Managers**: For resource management (e.g., database connections, locks)
4. **Distributed Locking**: PostgreSQL advisory locks for multi-node coordination (see [`app/dlock/__init__.py`](app/db/dlock/__init__.py))

## Status Constants

Job and item statuses are defined in [`app/const.py`](app/const.py):

- `pending` - Initial state
- `downloading` - Currently downloading
- `uploading` - Downloaded, posting to site
- `done` - Completed successfully
- `failed` - Failed with error
- `skipped` - Skipped (e.g., size limit)
- `removed-by-site` - Removed from M-Team
- `removed-by-client` - Removed from qBittorrent

## API Endpoints

The FastAPI server (see [`app/server.py`](app/server.py)) provides:

- `GET /` - Web interface
- `GET /rss` - RSS feed of completed items
- `GET /progress` - Progress monitoring page

## Error Handling

The application handles various error scenarios:

- **Network Errors**: Retries with exponential backoff (see [`httpx_network_errors`](app/mt.py:49))
- **Rate Limiting**: Sleeps for 10 minutes when rate limited by M-Team
- **Invalid Torrents**: Marks as invalid in database
- **Download Failures**: Tags with `process-error` in qBittorrent

## Testing

```bash
# Run type checking
mypy .

# Run linting
ruff check .

# Run formatting
black --check .
```

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
3. Update database queries in [`app/db.py`](app/db.py) if needed

## Troubleshooting

### Common Issues

1. **Database Connection Errors**: Check PostgreSQL is running and credentials are correct
2. **qBittorrent Connection Errors**: Verify qBittorrent is running and API is accessible
3. **MediaInfo Not Found**: Ensure MediaInfo is installed and in PATH
4. **Rate Limiting**: M-Team API has rate limits; the app handles this automatically

### Logs

The application uses `sslog` for logging. Check logs for:
- Download progress
- API errors
- Database operations
- Lock acquisition

## Dependencies

Key dependencies (see [`pyproject.toml`](pyproject.toml)):

- `qbittorrent-api` - qBittorrent Web API client
- `httpx` - Async HTTP client
- `psycopg` - PostgreSQL adapter
- `asyncpg` - Async PostgreSQL driver
- `pydantic` - Data validation
- `fastapi` - Web framework
- `bencode2` - Torrent file parsing
- `rapidocr-onnxruntime` - OCR for subtitle detection

## Project Structure

```
pt-repost/
├── app/
│   ├── __init__.py
│   ├── application.py      # Main application logic
│   ├── config.py           # Configuration management
│   ├── const.py            # Constants
│   ├── db.py               # Database operations
│   ├── hardcode_subtitle.py # Subtitle detection
│   ├── main.py             # CLI entry point
│   ├── mediainfo.py        # MediaInfo extraction
│   ├── mt.py               # M-Team API client
│   ├── patterns.py         # Regex patterns
│   ├── scrape.py           # M-Team scraper
│   ├── server.py           # FastAPI server
│   ├── torrent.py          # Torrent parsing
│   ├── utils.py            # Utility functions
│   ├── dlock/              # Distributed locking
│   │   └── __init__.py
│   ├── sql/                # SQL schemas
│   │   └── schema.sql
│   └── templates/          # Jinja2 templates
│       ├── index.html.j2
│       ├── progress.html.j2
│       └── rss-item.html.j2
├── .github/                # GitHub workflows
├── Dockerfile              # Docker configuration
├── pyproject.toml          # Python project config
├── taskfile.yaml           # Task runner config
├── uv.lock                 # Dependency lock file
└── main.py                 # Entry point
```

## Notes for AI Agents

- The project uses Python 3.12.12 specifically
- Configuration is primarily via environment variables
- The application is designed to run as a long-lived service
- Multi-node support via PostgreSQL advisory locks
- Rate limiting is handled automatically for M-Team API
- Video files are processed for mediainfo and hardcoded subtitle detection
- The system is designed for private tracker automation workflows
