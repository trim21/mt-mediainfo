create table if not exists torrent (
    tid int primary key,
    torrent_data bytea not null,
    size int8 not null,
    mediainfo text not null,
    info_hash text not null,
    pick_node uuid,
    start_download_time timestamptz
);

create table if not exists node (
  id uuid primary key,
  last_seen timestamptz not null
)
