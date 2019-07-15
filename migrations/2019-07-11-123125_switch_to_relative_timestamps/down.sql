truncate table recording;

alter table recording drop column started_at;

alter table recording drop column time;
alter table recording add column time tstzrange[] not null;
