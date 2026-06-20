-- Remove all currently downloading BDMV jobs.
-- The downloader will pick up orphan torrents on its next loop iteration
-- (it already handles unmanaged torrent cleanup).
delete from
  job_download_size
where
  info_hash in (
    select
      j.info_hash
    from
      job j
      join thread t on t.tid = j.tid
    where
      t.type = 'bdmv'
      and j.status = 'downloading'
  );


delete from
  job
where
  status = 'downloading'
  and tid in (
    select
      tid
    from
      thread
    where
      type = 'bdmv'
  );
