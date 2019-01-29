use crate::db::room;
use chrono::{offset::Utc, Duration};
use diesel::pg::PgConnection;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_demo_room(conn: &PgConnection, audience: &str) {
    use std::str::FromStr;

    let id =
        Uuid::from_str("00000001-0000-1000-a000-000000000000").expect("Error generating room id");

    let start = Utc::now();
    let end = start + Duration::weeks(53);

    let _ = room::InsertQuery::new(&start..&end, &audience)
        .id(&id)
        .execute(conn);
}
