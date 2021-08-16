create table orphaned_room (
    id uuid primary key references room,
    host_left_time timestamptz not null
);
create index orphaned_room_host_left_time on orphaned_room(host_left_time);