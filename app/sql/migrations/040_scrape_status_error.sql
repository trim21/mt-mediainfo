alter table
  scrape_status
add
  column if not exists error text not null default '';
