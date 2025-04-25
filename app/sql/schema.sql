create table if not exists torrent (
    tid int primary key,
--     torrent_data bytea not null,
    size int8 not null default 0,
    mediainfo text not null default '',
    info_hash text not null default '',
    pick_node uuid default null,
    start_download_time timestamptz default null,
    category int4 not null default 0,
    deleted bool default false
);

create table if not exists node (
  id uuid primary key,
  last_seen timestamptz not null
)
