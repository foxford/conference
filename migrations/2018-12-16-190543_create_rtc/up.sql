create table rtc (
    id uuid default gen_random_uuid(),

    room_id uuid not null,
    jsep jsonb,

    foreign key (room_id) references room (id) on delete cascade,
    primary key (id)
)