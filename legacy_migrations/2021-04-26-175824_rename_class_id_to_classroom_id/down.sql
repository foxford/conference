-- This file should undo anything in `up.sql`
ALTER TABLE room RENAME COLUMN classroom_id TO class_id;
