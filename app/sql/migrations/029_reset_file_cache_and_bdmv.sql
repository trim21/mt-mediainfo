-- Clear thread_file_cache so it can be repopulated with full-path data.
-- Old cache entries stored bare filenames (f.name); new entries store
-- full paths ("/".join(f.path)) so BDMV disc grouping works correctly.
truncate table thread_file_cache;


-- Distinguish BDMV threads from normal mediainfo threads.
alter table
  thread
add
  column if not exists type text not null default ''
