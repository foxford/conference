-- We don't need historical data in `recording` table so just clean it up before altering.
truncate table recording;

alter table recording add column started_at timestamptz not null;

alter table recording drop column time;
alter table recording add column time int8range[] not null;
