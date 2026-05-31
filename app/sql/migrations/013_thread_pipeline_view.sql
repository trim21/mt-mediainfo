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


drop index if exists thread_pending_mediainfo;


drop index if exists thread_pending_torrent;


drop index if exists thread_pending_download;


create index thread_pending_mediainfo on thread (api_mediainfo_at, category, tid);


create index thread_pending_torrent on thread (info_hash, category, seeders);


create index thread_pending_download on thread (info_hash, category, selected_size);


create index thread_completed on thread (info_hash, category, tid);


create index thread_dormant on thread (deleted, category, tid);
