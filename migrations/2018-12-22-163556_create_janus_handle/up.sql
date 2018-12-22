create table janus_handle_shadow (
    handle_id int8,
    rtc_id uuid,

    owner_id agent_id not null,

    foreign key (rtc_id) references rtc (id) on delete cascade,
    primary key (handle_id, rtc_id)
)