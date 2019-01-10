create table room (
    id uuid default gen_random_uuid(),

    time tstzrange not null,
    audience text not null,

    primary key (id)
)