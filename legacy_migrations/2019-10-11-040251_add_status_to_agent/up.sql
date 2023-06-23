create type agent_status as ENUM ('in_progress', 'ready');
alter table agent add column status agent_status not null default 'in_progress';
update agent set status = 'ready';
