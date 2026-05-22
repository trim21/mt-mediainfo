create table if not exists job_download_size (
  info_hash text not null,
  node_id text not null,
  size int8 not null,
  recorded_at timestamptz not null default current_timestamp
);

create index if not exists job_download_size_lookup
  on job_download_size (info_hash, node_id, recorded_at desc);
