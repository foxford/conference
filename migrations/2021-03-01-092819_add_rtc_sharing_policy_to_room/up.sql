-- Add rtc.created_by.
ALTER TABLE rtc ADD COLUMN created_by agent_id NULL;
UPDATE rtc SET created_by = '("(anonymous,example.com)",web)'::agent_id;
ALTER TABLE rtc ALTER COLUMN created_by SET NOT NULL;

UPDATE rtc
SET created_by = jrs.sent_by
FROM (
    SELECT DISTINCT ON (rtc_id) *
    FROM janus_rtc_stream
    ORDER BY rtc_id, created_at
) AS jrs
WHERE jrs.rtc_id = rtc.id;

-- Add room.rtc_sharing_policy.
CREATE TYPE rtc_sharing_policy AS ENUM ('none', 'shared', 'owned');
ALTER TABLE room ADD COLUMN rtc_sharing_policy rtc_sharing_policy NOT NULL DEFAULT 'none';
UPDATE room SET rtc_sharing_policy = 'shared' WHERE backend = 'janus';

-- Update room_backend_id check constraint.
ALTER TABLE room DROP CONSTRAINT room_backend_id_check;
ALTER TABLE room ADD CONSTRAINT room_backend_id_check CHECK (backend_id IS NULL OR rtc_sharing_policy != 'none');

-- Apply RTC sharing policy within a trigger to limit RTCs number in the room:
DROP INDEX rtc_unique_room_id;

CREATE OR REPLACE FUNCTION on_rtc_insert() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    rtc_sharing_policy rtc_sharing_policy;
BEGIN
    CASE (SELECT r.rtc_sharing_policy FROM room AS r WHERE id = NEW.room_id)
    WHEN 'none' THEN
        -- RTC creation not allowed.
        RAISE EXCEPTION 'creating RTC within a room with `none` RTC sharing policy is not allowed';
    WHEN 'shared' THEN
        -- Only single RTC allowed in the room.
        IF (SELECT COUNT(id) FROM rtc WHERE room_id = NEW.room_id) = 0 THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'creating multiple RTCs within a room with `shared` RTC sharing policy is not allowed';
        END IF;
    WHEN 'owned' THEN
        -- Only single RTC per agent allowed in the room.
        IF (SELECT COUNT(id) FROM rtc WHERE room_id = NEW.room_id AND created_by = NEW.created_by) = 0 THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'creating multiple RTCs per agent within a room with `owned` RTC sharing policy is not allowed';
        END IF;
    END CASE;
END;
$$;

DO $$ BEGIN
    CREATE TRIGGER rtc_insert_trigger BEFORE INSERT
    ON rtc FOR EACH ROW EXECUTE FUNCTION on_rtc_insert();
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
