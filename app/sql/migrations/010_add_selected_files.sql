alter table
  thread
add
  column if not exists selected_files jsonb not null default '[]'::jsonb;
