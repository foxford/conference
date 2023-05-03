create table if not exists outbox (
    id bigserial,
    entity_type text not null,
    stage jsonb not null,
    delivery_deadline_at timestamptz not null,
    error_kind text null,
    retry_count int not null default 0,
    created_at timestamptz not null default now(),

    primary key (entity_type, id)
);
