alter table job alter column node_id type text using node_id::text;
alter table node alter column id type text using id::text;
alter table node_command alter column node_id type text using node_id::text;
