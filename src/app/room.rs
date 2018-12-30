use crate::app::model::room;
use crate::transport::AccountId;
use diesel::pg::PgConnection;
use std::collections::Bound;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_demo_room(conn: &PgConnection) {
    use std::str::FromStr;

    let id =
        Uuid::from_str("00000001-0000-1000-a000-000000000000").expect("Error generating room id");
    let time = (Bound::Unbounded, Bound::Unbounded);
    let owner_id = AccountId::new("admin", "example.org");

    let _ = room::InsertQuery::new(time, &owner_id)
        .id(&id)
        .execute(conn);
}
