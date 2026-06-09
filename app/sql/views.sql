create
or replace view pending_mediainfo_threads as
select
  *
from
  thread
where
  deleted = false
  and seeders != 0
  and api_mediainfo_at is null
  and api_mediainfo = '';


create
or replace view pending_torrent_threads as
select
  *
from
  thread
where
  deleted = false
  and seeders != 0
  and api_mediainfo_at is not null
  and mediainfo = ''
  and api_mediainfo = ''
  and info_hash = ''
  and torrent_invalid = '';


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
  and selected_size > 0;


create
or replace view completed_threads as
select
  *
from
  thread
where
  seeders != 0
  and deleted = false
  and (
    (
      mediainfo != ''
      and info_hash != ''
    )
    or api_mediainfo != ''
  );


create
or replace view skipped_threads as
select
  *
from
  thread
where
  deleted = false
  and seeders != 0
  and (
    torrent_invalid != ''
    or (
      selected_size <= 0
      and info_hash != ''
    )
  );


create
or replace view dormant_threads as
select
  *
from
  thread
where
  deleted
  or seeders = 0;
