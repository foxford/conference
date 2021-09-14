-- Your SQL goes here
CREATE INDEX IF NOT EXISTS room_backend_id ON room (backend_id);
CREATE INDEX IF NOT EXISTS rtc_room_id ON rtc (room_id);
