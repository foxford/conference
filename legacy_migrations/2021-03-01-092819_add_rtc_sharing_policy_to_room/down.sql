ALTER TABLE room DROP column rtc_sharing_policy;
DROP TYPE rtc_sharing_policy;

ALTER TABLE room ADD CONSTRAINT room_backend_id_check CHECK (backend_id IS NULL OR backend != 'janus');
CREATE UNIQUE INDEX rtc_unique_room_id ON rtc (room_id);

DROP TRIGGER rtc_insert_trigger ON rtc;
DROP FUNCTION on_rtc_insert;

ALTER TABLE rtc DROP COLUMN created_by;
