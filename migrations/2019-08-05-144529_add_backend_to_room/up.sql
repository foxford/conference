create type room_backend as ENUM ('none', 'janus');
alter table room add column backend room_backend NOT NULL DEFAULT 'none';
