-- This file should undo anything in `up.sql`
ALTER TABLE recording ADD CONSTRAINT recording_check CHECK (
  (
    status = 'ready'
    AND started_at IS NOT NULL
    AND segments IS NOT NULL
  ) OR (
    status = 'missing'
    AND started_at IS NULL
    AND segments IS NULL
  )
);
