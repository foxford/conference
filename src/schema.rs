table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    janus_backend (id) {
        id -> Agent_id,
        handle_id -> Int8,
        session_id -> Int8,
        created_at -> Timestamptz,
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
        started_at -> Timestamptz,
        time -> Array<Int8range>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;
    use crate::db::room::RoomBackendMapping;

    room (id) {
        id -> Uuid,
        time -> Tstzrange,
        audience -> Text,
        created_at -> Timestamptz,
        backend -> RoomBackendMapping,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    rtc (id) {
        id -> Uuid,
        room_id -> Uuid,
        created_at -> Timestamptz,
    }
}

joinable!(janus_rtc_stream -> janus_backend (backend_id));
joinable!(janus_rtc_stream -> rtc (rtc_id));
joinable!(recording -> rtc (rtc_id));
joinable!(rtc -> room (room_id));

allow_tables_to_appear_in_same_query!(janus_backend, janus_rtc_stream, recording, room, rtc,);
