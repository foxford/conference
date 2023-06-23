ALTER TABLE room ADD COLUMN backend_id agent_id;

ALTER TABLE room
ADD CONSTRAINT room_backend_id_check
CHECK (backend_id IS NULL OR backend = 'janus');

UPDATE room AS r
SET backend_id = rec.backend_id
FROM rtc,
     recording AS rec 
WHERE rtc.room_id = r.id
AND   rec.rtc_id = rtc.id
AND   r.backend = 'janus';

ALTER TABLE recording DROP COLUMN backend_id;
