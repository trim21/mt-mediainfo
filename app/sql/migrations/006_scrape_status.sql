create table
  if not exists scrape_status (
    name text primary key,
    last_run_at timestamptz,
    last_result text not null default '',
    next_allowed_at timestamptz
  );
