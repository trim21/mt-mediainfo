alter table thread add column if not exists torrent_invalid text not null default '';

-- backfill existing sentinel values
update thread set torrent_invalid = 'parse error', mediainfo = '' where mediainfo = 'invalid torrent';
update thread set torrent_invalid = 'file error', mediainfo = '' where mediainfo = 'torrent file error';
update thread set torrent_invalid = 'parse error', mediainfo = '' where mediainfo = 'torrent parse error';

-- recreate partial index to include torrent_invalid filter
drop index if exists thread_pending_torrent;
create index thread_pending_torrent on thread (category, seeders)
  where deleted = false and mediainfo_at is not null and mediainfo = '' and info_hash = '' and torrent_invalid = '';
