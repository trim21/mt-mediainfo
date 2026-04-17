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
    node_id text,
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
  id text primary key,
  last_seen timestamptz not null
);

create table if not exists config (
    key text primary key,
    value text not null
);

alter table thread add column if not exists selected_size int8 not null default 0;
alter table thread add column if not exists torrent_fetched_at timestamptz default null;
alter table job drop column if exists download_size;

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
alter table job add column if not exists dlspeed int8 not null default 0;
alter table job add column if not exists eta int8 not null default -1;

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

-- job: filter by status without node_id (server pages: downloading, failed, removed, index stats)
create index if not exists job_status_updated_at on job (status, updated_at desc);

-- thread: daily stats aggregation by created_at range
create index if not exists thread_created_at on thread (created_at);

-- thread: daily stats aggregation by mediainfo_at range
create index if not exists thread_mediainfo_at on thread (mediainfo_at)
  where mediainfo_at is not null;

-- thread: /threads/done listing (filter done + order by tid desc)
create index if not exists thread_done on thread (category, tid desc)
  where mediainfo != '' and info_hash != '';

-- RPC command queue: server dispatches commands to nodes
create table if not exists node_command (
    id bigserial primary key,
    node_id text not null,
    method text not null,
    payload text not null default '{}',
    result text,
    error text,
    created_at timestamptz not null default now(),
    executed_at timestamptz
);

create index if not exists idx_node_command_pending
    on node_command (node_id) where executed_at is null;
