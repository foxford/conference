-- This file should undo anything in `up.sql`
alter table janus_backend add column handle_id int8;
alter table janus_backend add column session_id int8;