create table "group" (
    id uuid default gen_random_uuid(),
    room_id uuid not null,
    number integer not null,
    
    foreign key (room_id) references room (id) on delete cascade,
    unique (room_id, number),
    primary key (id)
);
