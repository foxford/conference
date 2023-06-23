create table if not exists group_agent (
    id uuid default gen_random_uuid(),
    room_id uuid not null,
    groups jsonb not null default '[]',

    foreign key (room_id) references room (id) on delete cascade,
    unique (room_id),
    primary key (id)
);
