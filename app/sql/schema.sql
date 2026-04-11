create table if not exists thread (
    tid int primary key,
    size int8 not null default 0,
    mediainfo text not null default '',
    hard_coded_subtitle bool not null default false,
    info_hash text not null default '',
    seeders int8 not null default 0,
    category int4 not null default 0,
    deleted bool default false,
    created_at timestamptz not null default current_timestamp,
    upload_at timestamptz not null default '1970-01-01 00:00:00Z',
    mediainfo_at timestamptz default null
);

create table if not exists job(
    tid int,
    node_id uuid,
    info_hash text not null,
    progress float8 not null default 0,
    failed_reason text not null default '',
    status text not null default '',
    start_download_time timestamptz default null,
    updated_at timestamptz default current_timestamp,
    completed_at timestamptz default null,
    primary key (tid, node_id)
);

create table if not exists node (
  id uuid primary key,
  last_seen timestamptz not null
);

create table if not exists torrent (
    tid int8 primary key,
    info_hash text not null,
    content bytea not null,
    created_at timestamptz not null default current_timestamp
);

create index if not exists torrent_info_hash on torrent (info_hash);

create table if not exists config (
    key text primary key,
    value text not null
);

alter table thread add column if not exists selected_size int8 not null default 0;
alter table thread add column if not exists torrent_fetched_at timestamptz default null;
alter table job drop column if exists download_size;

-- backfill torrent_fetched_at for threads that already have a torrent record
update thread set torrent_fetched_at = torrent.created_at
from torrent
where torrent.tid = thread.tid and thread.torrent_fetched_at is null;

-- job: lookup by info_hash (hot path: update status/progress every minute)
create index if not exists job_info_hash on job (info_hash);

-- job: lookup by node_id + status (hot path: fetch downloading jobs per node)
create index if not exists job_node_id_status on job (node_id, status);

-- thread: update mediainfo by info_hash after local extraction
create index if not exists thread_info_hash on thread (info_hash) where info_hash != '';

-- thread: scrape_detail / scrape_mediainfo (pending mediainfo)
create index if not exists thread_pending_mediainfo on thread (category, tid)
  where deleted = false and mediainfo_at is null;

-- thread: fetch_torrent (pending torrent download)
create index if not exists thread_pending_torrent on thread (category, seeders)
  where deleted = false and mediainfo_at is not null and mediainfo = '' and info_hash = '';

-- thread: pick_job (pending to download)
create index if not exists thread_pending_download on thread (category, selected_size)
  where mediainfo = '' and info_hash != '' and selected_size > 0 and seeders != 0;

-- thread: daily fetched size chart (torrent_fetched_at range scan)
create index if not exists thread_torrent_fetched_at on thread (torrent_fetched_at, category)
  where torrent_fetched_at is not null and selected_size > 0;

-- pre-aggregated daily stats cache for chart endpoints
create table if not exists daily_stats (
    day date primary key,
    downloaded_bytes int8 not null default 0,
    downloaded_count int4 not null default 0,
    fetched_bytes int8 not null default 0,
    fetched_count int4 not null default 0,
    thread_count int4 not null default 0,
    torrent_count int4 not null default 0,
    mediainfo_count int4 not null default 0,
    node_downloaded jsonb not null default '{}'::jsonb
);
