insert into
  node (id, last_seen, alias)
values
  ('removed', '1970-01-01 00:00:00Z', 'removed')
on conflict
  (id)
do nothing
;
