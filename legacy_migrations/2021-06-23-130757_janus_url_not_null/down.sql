-- This file should undo anything in `up.sql`
alter table janus_backend alter column janus_url drop not null;
