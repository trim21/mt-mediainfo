update
  job
set
  last_progress_at = start_download_time
where
  last_progress_at is null;


alter table
  job
alter column
  last_progress_at
set
  default current_timestamp,
alter column
  last_progress_at
set
  not null;
