create table agent (
    id uuid default gen_random_uuid(),
    agent_id agent_id not null,
    room_id uuid not null,
    created_at timestamptz not null default now(),

    foreign key (room_id) references room (id) on delete cascade,
    unique (agent_id, room_id),
    primary key (id)
);

create table agent_stream (
    id uuid default gen_random_uuid(),
    sent_by uuid not null,
    label text not null,
    created_at timestamptz not null default now(),

    foreign key(sent_by) references agent (id) on delete cascade,
    primary key (id)
);
