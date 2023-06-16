ALTER TABLE recording ADD COLUMN backend_id agent_id;

UPDATE recording AS rec
SET backend_id = r.backend_id
FROM rtc,
     room AS r
WHERE rtc.id = rec.rtc_id
AND   r.id = rtc.room_id
AND   r.backend = 'janus';

ALTER TABLE room DROP COLUMN backend_id;
