-- Create `agent_connection`.
CREATE TABLE agent_connection (
    agent_id UUID NOT NULL,
    handle_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (agent_id) REFERENCES agent (id) ON DELETE CASCADE,
    PRIMARY KEY (agent_id)
);

-- Remove `connected` status since it's now indicated by `agent_connection` row presence.
UPDATE agent SET status = 'ready' WHERE status = 'connected';
ALTER TYPE agent_status RENAME TO agent_status_old;
CREATE TYPE agent_status AS ENUM ('in_progress', 'ready');
ALTER TABLE agent ALTER COLUMN status DROP DEFAULT;
ALTER TABLE agent ALTER COLUMN status TYPE agent_status USING status::text::agent_status;
ALTER TABLE agent ALTER COLUMN status SET DEFAULT 'in_progress';
DROP TYPE agent_status_old;

-- Unused table.
DROP TABLE agent_stream;
