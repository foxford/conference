create type rtc_state as (
    label text,
    sent_by agent_id,
    sent_at timestamptz
);