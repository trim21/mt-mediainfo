alter table
  thread
add
  column if not exists exported_at int not null default 0;
