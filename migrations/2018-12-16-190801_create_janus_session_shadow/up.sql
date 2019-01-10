create table janus_session_shadow (
    rtc_id uuid,

    session_id int8 not null,
    location_id agent_id not null,

    check((location_id).label is not null),
    check(((location_id).account_id).label is not null),
    check(((location_id).account_id).audience is not null),
    unique (rtc_id, session_id, location_id),
    foreign key (rtc_id) references rtc (id) on delete cascade,
    primary key (rtc_id)
)