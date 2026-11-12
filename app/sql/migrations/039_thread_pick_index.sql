-- The picker releases the lock with `order by seeders desc, priority desc, tid asc
-- limit N for update skip locked`. This index matches that order and mirrors the
-- pending_download_threads predicate so the planner can serve both the pick query
-- and the "is anything pending" probe from the index instead of sorting the whole
-- pending set. Keep the predicate in sync with that view.
create index if not exists thread_pick_seeders on thread (seeders desc, priority desc, tid)
where
  deleted = false
  and seeders != 0
  and mediainfo = ''
  and api_mediainfo = ''
  and info_hash != ''
  and selected_size > 0;
