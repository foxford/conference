table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    agent (id) {
        id -> Uuid,
        agent_id -> Agent_id,
        room_id -> Uuid,
        created_at -> Timestamptz,
        status -> Agent_status,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    agent_connection (agent_id) {
        agent_id -> Uuid,
        handle_id -> Int8,
        created_at -> Timestamptz,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    janus_backend (id) {
        id -> Agent_id,
        handle_id -> Int8,
        session_id -> Int8,
        created_at -> Timestamptz,
        capacity -> Nullable<Int4>,
        balancer_capacity -> Nullable<Int4>,
        api_version -> Text,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    janus_rtc_stream (id) {
        id -> Uuid,
        handle_id -> Int8,
        rtc_id -> Uuid,
        backend_id -> Agent_id,
        label -> Text,
        sent_by -> Agent_id,
        time -> Nullable<Tstzrange>,
        created_at -> Timestamptz,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    recording (rtc_id) {
        rtc_id -> Uuid,
        started_at -> Nullable<Timestamptz>,
        segments -> Nullable<Array<Int8range>>,
        status -> Recording_status,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    room (id) {
        id -> Uuid,
        time -> Tstzrange,
        audience -> Text,
        created_at -> Timestamptz,
        backend -> Room_backend,
        reserve -> Nullable<Int4>,
        tags -> Json,
        backend_id -> Nullable<Agent_id>,
        rtc_sharing_policy -> Rtc_sharing_policy,
        class_id -> Nullable<Uuid>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    rtc (id) {
        id -> Uuid,
        room_id -> Uuid,
        created_at -> Timestamptz,
        created_by -> Agent_id,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    rtc_reader_config (rtc_id, reader_id) {
        rtc_id -> Uuid,
        reader_id -> Agent_id,
        receive_video -> Bool,
        receive_audio -> Bool,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    rtc_writer_config (rtc_id) {
        rtc_id -> Uuid,
        send_video -> Bool,
        send_audio -> Bool,
        video_remb -> Nullable<Int8>,
    }
}

joinable!(agent -> room (room_id));
joinable!(agent_connection -> agent (agent_id));
joinable!(janus_rtc_stream -> janus_backend (backend_id));
joinable!(janus_rtc_stream -> rtc (rtc_id));
joinable!(recording -> rtc (rtc_id));
joinable!(rtc -> room (room_id));
joinable!(rtc_reader_config -> rtc (rtc_id));
joinable!(rtc_writer_config -> rtc (rtc_id));

allow_tables_to_appear_in_same_query!(
    agent,
    agent_connection,
    janus_backend,
    janus_rtc_stream,
    recording,
    room,
    rtc,
    rtc_reader_config,
    rtc_writer_config,
);
