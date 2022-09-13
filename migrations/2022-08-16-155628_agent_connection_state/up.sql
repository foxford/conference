CREATE TYPE agent_connection_status as ENUM ('in_progress', 'connected');
ALTER TABLE agent_connection add column status agent_connection_status not null default 'in_progress';
UPDATE agent_connection SET status = 'connected';
