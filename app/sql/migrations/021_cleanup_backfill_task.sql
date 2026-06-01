delete from
  backfill_task;


delete from
  scrape_status
where
  category = 'backfill';
