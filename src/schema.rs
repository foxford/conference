table! {
    use diesel::sql_types::*;
    use crate::sql::*;

    janus_handle_shadow (handle_id, rtc_id) {
        handle_id -> Int8,
        rtc_id -> Uuid,
        owner_id -> Agent_id,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::sql::*;

    janus_session_shadow (rtc_id) {
        rtc_id -> Uuid,
        session_id -> Int8,
        location_id -> Agent_id,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::sql::*;

    room (id) {
        id -> Uuid,
        time -> Tstzrange,
        owner_id -> Account_id,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::sql::*;

    rtc (id) {
        id -> Uuid,
        room_id -> Uuid,
        owner_id -> Account_id,
    }
}

joinable!(janus_handle_shadow -> rtc (rtc_id));
joinable!(janus_session_shadow -> rtc (rtc_id));
joinable!(rtc -> room (room_id));

allow_tables_to_appear_in_same_query!(janus_handle_shadow, janus_session_shadow, room, rtc,);
