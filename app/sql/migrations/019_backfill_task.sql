create table
  if not exists backfill_task (
    name text not null,
    tid int8 not null,
    status text not null default 'pending',
    error text not null default '',
    created_at timestamptz not null default current_timestamp,
    updated_at timestamptz,
    primary key (name, tid)
  );


create index if not exists backfill_task_pending on backfill_task (name, tid)
where
  status = 'pending';
