create table room (
    id uuid default gen_random_uuid(),

    time tstzrange not null,
    owner_id account_id not null,

    primary key (id)
)