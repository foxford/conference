create table rtc (
    id uuid default gen_random_uuid(),

    room_id uuid not null,

    foreign key (room_id) references room (id) on delete cascade,
    primary key (id)
)