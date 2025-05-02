create table if not exists thread (
    tid int primary key,
    size int8 not null default 0,
    mediainfo text not null default '',
    info_hash text not null default '',
    seeders int8 not null default 0,
    category int4 not null default 0,
    deleted bool default false
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
    primary key (tid, node_id)
);

create table if not exists node (
  id uuid primary key,
  last_seen timestamptz not null
);

create table if not exists torrent (
    -- hex info hash
    tid int8 primary key,
    info_hash text not null,
    content bytea not null
);

create index if not exists torrent_info_hash on torrent (info_hash);
