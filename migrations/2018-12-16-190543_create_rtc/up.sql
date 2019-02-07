create table rtc (
    id uuid default gen_random_uuid(),

    state rtc_state,
    room_id uuid not null,
    stored boolean not null,
    created_at timestamptz not null default now(),

    foreign key (room_id) references room (id) on delete cascade,
    primary key (id)
)