create index if not exists job_status_completed_at on job (status, completed_at)
where
  status = 'done'
  and completed_at is not null;
