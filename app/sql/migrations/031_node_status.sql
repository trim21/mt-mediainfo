alter table
  node
add
  column if not exists status text not null default '';
