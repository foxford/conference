-- Create janus_backend_handle table.
CREATE TABLE janus_backend_handle (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    backend_id agent_id NOT NULL,
    handle_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (backend_id) REFERENCES janus_backend (id) ON DELETE CASCADE,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX janus_backend_handle_backend_id_handle_id_idx
ON janus_backend_handle (backend_id, handle_id);

-- Add agent_connection.janus_backend_handle_id.
DELETE FROM agent_connection;
ALTER TABLE agent_connection ADD COLUMN janus_backend_handle_id UUID NOT NULL;

ALTER TABLE agent_connection
ADD FOREIGN KEY (janus_backend_handle_id) REFERENCES janus_backend_handle (id) ON DELETE CASCADE;

CREATE INDEX agent_connection_janus_backend_handle_id_idx
ON agent_connection (janus_backend_handle_id);
