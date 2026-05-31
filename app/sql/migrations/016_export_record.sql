create table
  if not exists export_record (
    export_date text primary key,
    status text not null,
    error text not null default '',
    exported_count int not null default 0,
    created_at timestamptz not null default current_timestamp
  );
