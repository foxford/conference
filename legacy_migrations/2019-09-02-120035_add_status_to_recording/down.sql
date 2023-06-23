alter table recording drop constraint recording_check;

alter table recording alter column started_at set not null;
alter table recording alter column time set not null;

alter table recording drop column status;
drop type recording_status;
