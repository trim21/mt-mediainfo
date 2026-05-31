alter table
  job
add
  column if not exists removed_reason text not null default '';
