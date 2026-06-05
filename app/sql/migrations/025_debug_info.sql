alter table
  node
add
  column if not exists debug_info jsonb not null default '{}'::jsonb;


alter table
  job
add
  column if not exists debug_info jsonb not null default '{}'::jsonb;
