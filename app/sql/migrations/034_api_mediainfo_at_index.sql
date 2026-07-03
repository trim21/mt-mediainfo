create index if not exists thread_api_mediainfo_at on thread (api_mediainfo_at)
where
  api_mediainfo_at is not null;
