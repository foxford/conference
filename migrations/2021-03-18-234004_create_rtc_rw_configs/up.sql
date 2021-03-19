CREATE TABLE rtc_reader_config (
  rtc_id UUID NOT NULL,
  reader_id agent_id NOT NULL,
  receive_video BOOLEAN NOT NULL,
  receive_audio BOOLEAN NOT NULL,

  FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE,
  PRIMARY KEY (rtc_id, reader_id)
);

CREATE TABLE rtc_writer_config (
  rtc_id UUID NOT NULL,
  send_video BOOLEAN NOT NULL,
  send_audio BOOLEAN NOT NULL,
  audio_remb BIGINT CHECK (video_remb IS NULL OR video_remb > 0),
  video_remb BIGINT CHECK (audio_remb IS NULL OR audio_remb > 0),

  FOREIGN KEY (rtc_id) REFERENCES rtc (id) ON DELETE CASCADE,
  PRIMARY KEY (rtc_id)
);
