# AGENTS.md - AI Agent Guide for pt-repost

## Project Overview

**pt-repost** downloads torrents from M-Team, processes local media files to extract metadata, detects hardcoded subtitles, and prepares reposting data. The codebase has three long-running modes: a download node, a background scraper, and a FastAPI web server.

## Code Map

- `app/node.py` - Node loop, qBittorrent integration, local mediainfo extraction, hardcoded-subtitle detection, RPC polling
- `app/scrape.py` - Thread discovery, API mediainfo fetch, torrent download, and `selected_size` backfill
- `app/server.py` - FastAPI dashboard, JSON endpoints, daily stats cache, node and RPC views
- `app/rpc.py` - RPC method definitions, payload validation, queue polling, and enqueue helpers
- `app/sql/schema.sql` - PostgreSQL schema; acts as both init SQL and migration source
- `app/const.py` - Status, tag, lock, and category constants
- `taskfile.yaml` - Standard local commands
- `pyproject.toml` - Dependency and tooling configuration

## Runtime Invariants

- Python 3.12.12 project with environment-driven config in `app/config.py`
- Node loop order matters: heartbeat -> process RPC commands -> process qBittorrent torrents -> pick new jobs
- `app/node.py` and `app/scrape.py` use psycopg-based sync DB access; `app/server.py` uses asyncpg
- `app/sql/schema.sql` is both initial schema and migration source; use additive SQL such as `ALTER ... IF NOT EXISTS`
- The service is designed to run continuously; preserve retry, cooldown, and background-loop behavior when refactoring

## Skills

- `qb-torrent-lifecycle` - qBittorrent state machine, tags, file selection, and cleanup rules for `app/node.py`
- `thread-lifecycle` - Canonical thread-table stage predicates used by scrape, node, and server queries
- `rpc-system` - `node_command` queue, payload types, handler registration, and RPC history behavior
- `scrape-mteam` - Search cursor, scrape scheduling, rate limits, detail and mediainfo fetch, torrent download flow
- `server-dashboard` - Dashboard queries, `daily_stats` caching and backfill, thread pages, node pages, and admin APIs

## Development Workflow

- Install or update dependencies: `uv sync`
- Run checks: `task default`
- Run node locally: `task dev` or `task node`
- Run scraper: `task scrape`
- Run web server: `task dev:server`

## Change Guidelines

- Keep statuses, tags, and selected-category definitions in `app/const.py` as the source of truth
- Treat this project as an application rather than a reusable library: preserve data compatibility during refactors, but backward compatibility of internal code interfaces is not required
- When changing thread or job state queries, keep `app/node.py`, `app/scrape.py`, and `app/server.py` aligned with `thread-lifecycle`
- When changing qBittorrent processing, keep `app/node.py` aligned with `qb-torrent-lifecycle`
- When adding server-to-node actions, update `app/rpc.py`, `app/node.py`, and `app/server.py` together
- Keep subsystem-specific procedures in skills rather than expanding `AGENTS.md`
