-- Deprecate debug_info: set all values to empty string and fix default.
-- The column is kept; code no longer reads or writes it.
update
  job
set
  debug_info = '';


update
  node
set
  debug_info = '';


alter table
  job
alter column
  debug_info
set
  default '';


alter table
  node
alter column
  debug_info
set
  default '';
