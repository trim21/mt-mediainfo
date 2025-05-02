create table if not exists torrent (
    -- hex info hash
    info_hash text primary key,
    content bytea not null
)
