CREATE TABLE rtc_writer_config_snapshot (
  id UUID DEFAULT gen_random_uuid(),
  rtc_id UUID NOT NULL,
  send_video BOOLEAN,
  send_audio BOOLEAN,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE,
  PRIMARY KEY (id)
);
