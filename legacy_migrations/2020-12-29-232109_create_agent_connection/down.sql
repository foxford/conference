-- Put back `agent_stream` table.
CREATE TABLE agent_stream (
    id UUID DEFAULT gen_random_uuid(),
    sent_by UUID NOT NULL,
    label TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (sent_by) REFERENCES agent (id) ON DELETE CASCADE,
    PRIMARY KEY (id)
);

-- Put back `connected` status.
ALTER TYPE agent_status RENAME TO agent_status_old;
CREATE TYPE agent_status AS ENUM ('in_progress', 'ready', 'connected');
ALTER TABLE agent ALTER COLUMN status DROP DEFAULT;
ALTER TABLE agent ALTER COLUMN status TYPE agent_status USING status::text::agent_status;
ALTER TABLE agent ALTER COLUMN status SET DEFAULT 'in_progress';
DROP TYPE agent_status_old;

-- Drop new `agent_connection table`.
DROP TABLE agent_connection;
