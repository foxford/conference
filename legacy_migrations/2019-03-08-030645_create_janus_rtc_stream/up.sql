create table janus_rtc_stream (
    id uuid,

    handle_id int8 not null,
    rtc_id uuid not null,
    backend_id agent_id not null,
    label text not null,
    sent_by agent_id not null,
    time tstzrange,
    created_at timestamptz not null default now(),

    check((sent_by).label is not null),
    check(((sent_by).account_id).label is not null),
    check(((sent_by).account_id).audience is not null),
    foreign key (rtc_id) references rtc (id) on delete cascade,
    foreign key (backend_id) references janus_backend (id) on delete cascade,
    primary key (id)
)