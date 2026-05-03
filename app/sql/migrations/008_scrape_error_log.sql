create table
  if not exists scrape_error (
    id bigserial primary key,
    tid int not null,
    op text not null,
    code text not null default '',
    message text not null default '',
    created_at timestamptz not null default CURRENT_TIMESTAMP
  );


create index if not exists scrape_error_tid on scrape_error (tid);


create index if not exists scrape_error_created_at on scrape_error (created_at desc);
