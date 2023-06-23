create table recording (
    rtc_id uuid,
    time tstzrange[] not null,

    foreign key (rtc_id) references rtc (id) on delete cascade,
    primary key (rtc_id)
)
