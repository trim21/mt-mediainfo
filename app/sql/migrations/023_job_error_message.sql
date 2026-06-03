alter table
  job
add
  column if not exists error_message text not null default '';
