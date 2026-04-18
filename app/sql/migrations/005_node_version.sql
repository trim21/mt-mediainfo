alter table
  node
add
  column if not exists version text not null default '';
