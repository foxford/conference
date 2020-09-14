DROP INDEX recording_rtc_id_idx;

ALTER TABLE janus_rtc_stream
DROP CONSTRAINT janus_rtc_stream_backend_id_fkey;

ALTER TABLE recording DROP COLUMN backend_id;
