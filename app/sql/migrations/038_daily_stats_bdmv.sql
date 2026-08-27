alter table
  daily_stats
add
  column if not exists bdmv_downloaded_bytes int8 not null default 0;


alter table
  daily_stats
add
  column if not exists bdmv_downloaded_count int4 not null default 0;


delete from
  daily_stats;


delete from
  config
where
  key = 'eta:download';
