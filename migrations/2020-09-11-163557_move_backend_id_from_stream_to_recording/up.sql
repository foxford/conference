ALTER TABLE recording ADD COLUMN backend_id agent_id NULL;
UPDATE recording SET backend_id = '("(janus,svc.netology-group.services)",unknown)'::agent_id;
ALTER TABLE recording ALTER COLUMN backend_id SET NOT NULL;

UPDATE recording AS rec
SET backend_id = jrs.backend_id
FROM janus_rtc_stream AS jrs
WHERE jrs.rtc_id = rec.rtc_id;

DELETE FROM janus_rtc_stream
WHERE backend_id NOT IN (SELECT id from janus_backend);

ALTER TABLE janus_rtc_stream
ADD CONSTRAINT janus_rtc_stream_backend_id_fkey
FOREIGN KEY (backend_id)
REFERENCES janus_backend(id)
ON DELETE CASCADE;

CREATE UNIQUE INDEX recording_rtc_id_idx ON recording (rtc_id);
