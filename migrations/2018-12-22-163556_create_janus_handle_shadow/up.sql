create table janus_handle_shadow (
    handle_id int8,
    rtc_id uuid,

    reply_to agent_id not null,

    check((reply_to).label is not null),
    check(((reply_to).account_id).label is not null),
    check(((reply_to).account_id).audience is not null),
    foreign key (rtc_id) references rtc (id) on delete cascade,
    primary key (handle_id, rtc_id)
)