-- https://github.com/ankane/strong_migrations#setting-not-null-on-an-existing-column
ALTER TABLE room ADD CONSTRAINT room_classroom_id_null CHECK (room.classroom_id IS NOT NULL) NOT VALID;
ALTER TABLE room VALIDATE CONSTRAINT room_classroom_id_null;

ALTER TABLE room ALTER COLUMN classroom_id SET NOT NULL;

ALTER TABLE room DROP CONSTRAINT room_classroom_id_null;
