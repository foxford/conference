ALTER TABLE recording DROP CONSTRAINT recording_check;

ALTER TYPE recording_status RENAME TO recording_status_old;
CREATE TYPE recording_status AS ENUM ('in_progress', 'ready', 'missing');
ALTER TABLE recording ALTER COLUMN status DROP DEFAULT;
ALTER TABLE recording ALTER COLUMN status TYPE recording_status USING status::text::recording_status;
ALTER TABLE recording ALTER COLUMN status SET DEFAULT 'in_progress';
DROP TYPE recording_status_old;

ALTER TABLE recording ADD CONSTRAINT recording_check CHECK (
  (
    status = 'ready'
    AND started_at IS NOT NULL
    AND segments IS NOT NULL
  ) OR (
    status IN ('in_progress', 'missing')
    AND started_at IS NULL
    AND segments IS NULL
  )
);
