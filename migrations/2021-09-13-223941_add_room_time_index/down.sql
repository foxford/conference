-- This file should undo anything in `up.sql`
DROP INDEX room_time;
CREATE INDEX IF NOT EXISTS room_backend_id ON room (backend_id);
