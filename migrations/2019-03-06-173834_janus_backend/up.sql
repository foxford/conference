create table janus_backend (
    id agent_id,

    session_id int8 not null,
    created_at timestamptz not null default now(),

    primary key (id)
)