alter table
  scrape_status
add
  column if not exists category text not null default '';


alter table
  scrape_status
add
  column if not exists detail text not null default '';
