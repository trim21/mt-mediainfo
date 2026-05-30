alter table
  thread rename column mediainfo_at to api_mediainfo_at;


alter table
  thread
add
  column if not exists api_mediainfo text not null default '';


alter table
  thread
add
  column if not exists generated_mediainfo_at timestamptz default null;


update
  thread
set
  api_mediainfo_at = null
where
  api_mediainfo_at is not null;
