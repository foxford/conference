-- Your SQL goes here
DROP INDEX room_backend_id;
CREATE INDEX IF NOT EXISTS room_time ON room USING GIST (time) WHERE backend_id IS NOT NULL;
