UPDATE agent SET status = 'ready' WHERE status = 'connected';
ALTER TYPE agent_status RENAME TO agent_status_old;
CREATE TYPE agent_status AS ENUM ('in_progress', 'ready');
ALTER TABLE agent ALTER COLUMN status DROP DEFAULT;
ALTER TABLE agent ALTER COLUMN status TYPE agent_status USING status::text::agent_status;
ALTER TABLE agent ALTER COLUMN status SET DEFAULT 'in_progress';
DROP TYPE agent_status_old;
