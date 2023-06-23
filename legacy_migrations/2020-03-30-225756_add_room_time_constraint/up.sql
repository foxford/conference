ALTER TABLE room ADD CONSTRAINT room_time_presence CHECK (time <> 'empty');
