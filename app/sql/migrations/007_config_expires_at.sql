alter table
  config
add
  column if not exists expires_at timestamptz;
