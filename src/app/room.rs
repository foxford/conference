use crate::schema::room;
use crate::transport::AccountId;
use chrono::{DateTime, Utc};
use std::collections::Bound;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

//#[derive(Debug, Queryable)]
//pub(crate) struct Room {
//    id: Uuid,
//    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
//    owner_id: AccountId,
//}

#[derive(Debug, Insertable)]
#[table_name = "room"]
pub(crate) struct CreateParameters<'a> {
    id: Option<&'a Uuid>,
    time: (Bound<&'a DateTime<Utc>>, Bound<&'a DateTime<Utc>>),
    owner_id: &'a AccountId,
}

////////////////////////////////////////////////////////////////////////////////

//pub(crate) struct State;
//
//impl State {
//    pub(crate) fn new() -> Self {
//        Self {}
//    }
//
//    pub(crate) fn create(&self, params: CreateParameters) -> Result<Room, diesel::result::Error> {
//        use diesel::RunQueryDsl;
//        use crate::schema::room::dsl::room;
//
//        // TODO: replace with db connection pool
//        let conn = crate::establish_connection();
//        diesel::insert_into(room).values(&params).get_result::<Room>(&conn)
//    }
//
//    pub(crate) fn list(&self) -> Result<Vec<Room>, diesel::result::Error> {
//        use diesel::RunQueryDsl;
//        use crate::schema::room::dsl::room;
//
//        // TODO: replace with db connection pool
//        let conn = crate::establish_connection();
//        crate::schema::room::table.load::<Room>(&conn)
//    }
//}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_demo_room() {
    use crate::schema::room::dsl::room;
    use diesel::RunQueryDsl;
    use std::str::FromStr;

    let id =
        Uuid::from_str("00000001-0000-1000-a000-000000000000").expect("Error generating room id");
    let account_id = AccountId::new("admin", "example.org");
    let time = (Bound::Unbounded, Bound::Unbounded);
    let params = CreateParameters {
        id: Some(&id),
        owner_id: &account_id,
        time: time,
    };

    // TODO: replace with db connection pool
    let conn = crate::establish_connection();
    let _ = diesel::insert_into(room).values(&params).execute(&conn);
}
