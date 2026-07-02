alter table
  job
add
  column if not exists last_progress_at timestamptz default null;
