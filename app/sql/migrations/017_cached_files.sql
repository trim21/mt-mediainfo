create table
  if not exists thread_file_cache (
    tid int8 primary key,
    files bytea not null,
    created_at timestamptz not null default current_timestamp
  );
