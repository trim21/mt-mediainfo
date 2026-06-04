create index if not exists job_download_size_node_lookup on job_download_size (node_id, info_hash, recorded_at desc);
