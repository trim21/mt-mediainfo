create table
  progress (
    info_hash text not null,
    size integer not null,
    recorded_at real not null
  );


create index progress_lookup on progress (info_hash, recorded_at desc);
