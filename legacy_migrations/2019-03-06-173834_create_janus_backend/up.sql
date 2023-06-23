create table janus_backend (
    id agent_id,

    handle_id int8 not null,
    session_id int8 not null,
    created_at timestamptz not null default now(),

    primary key (id)
)