create type recording_status as ENUM ('ready', 'missing');
alter table recording add column status recording_status;

update recording set status = 'ready';
alter table recording alter column status set not null;

alter table recording alter column started_at drop not null;
alter table recording alter column time drop not null;

alter table recording add constraint recording_check check (
  (
    status = 'ready'
    and started_at is not null
    and time is not null
  ) or (
    status = 'missing'
    and started_at is null
    and time is null
  )
);
