use std::ops::Bound;

use chrono::{offset::Utc, Duration};
use diesel::pg::PgConnection;
use uuid::Uuid;

use crate::db::room;

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_demo_room(conn: &PgConnection, audience: &str) {
    use std::str::FromStr;

    let id =
        Uuid::from_str("00000001-0000-1000-a000-000000000000").expect("Error generating room id");

    let time = (Bound::Unbounded, Bound::Unbounded);

    let _ = room::InsertQuery::new(time, &audience)
        .id(&id)
        .execute(conn);
}
