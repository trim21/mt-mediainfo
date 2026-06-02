alter table
  thread
add
  column if not exists selected_index int[];


update
  thread
set
  selected_files = '[]'::jsonb
where
  selected_files != '[]'::jsonb;


create
or replace view pending_download_threads as
select
  *
from
  thread
where
  deleted = false
  and seeders != 0
  and mediainfo = ''
  and api_mediainfo = ''
  and info_hash != ''
  and selected_size > 0
  and selected_index is not null
  and array_length(selected_index, 1) > 0;
