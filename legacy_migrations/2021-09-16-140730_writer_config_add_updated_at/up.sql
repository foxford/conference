-- Your SQL goes here
alter table rtc_writer_config add column updated_at timestamptz not null default now();

CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON rtc_writer_config
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();
