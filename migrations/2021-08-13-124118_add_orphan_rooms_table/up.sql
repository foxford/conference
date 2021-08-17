create table orphaned_room (
    id uuid primary key references room,
    host_left_at timestamptz not null
);
create index orphaned_room_host_left_at on orphaned_room(host_left_at);