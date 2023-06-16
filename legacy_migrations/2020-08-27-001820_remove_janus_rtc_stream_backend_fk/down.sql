ALTER TABLE janus_rtc_stream
ADD CONSTRAINT janus_rtc_stream_backend_id_fkey
FOREIGN KEY (backend_id)
REFERENCES janus_backend(id)
ON DELETE CASCADE;
