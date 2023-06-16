create table room (
    id uuid default gen_random_uuid(),

    time tstzrange not null,
    audience text not null,
    created_at timestamptz not null default now(),

    primary key (id)
)