use crate::schema::room;
use crate::transport::AccountId;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use std::collections::Bound;
use uuid::Uuid;

#[derive(Debug, Queryable)]
pub(crate) struct Record {
    id: Uuid,
    time: (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    owner_id: AccountId,
}

#[derive(Debug, Insertable)]
#[table_name = "room"]
pub(crate) struct InsertQuery<'a> {
    id: Option<&'a Uuid>,
    time: (Bound<&'a DateTime<Utc>>, Bound<&'a DateTime<Utc>>),
    owner_id: &'a AccountId,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(
        time: (Bound<&'a DateTime<Utc>>, Bound<&'a DateTime<Utc>>),
        owner_id: &'a AccountId,
    ) -> Self {
        Self {
            id: None,
            time,
            owner_id,
        }
    }

    pub(crate) fn id(self, id: &'a Uuid) -> Self {
        Self {
            id: Some(id),
            time: self.time,
            owner_id: self.owner_id,
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Record, Error> {
        use crate::schema::room::dsl::room;
        use diesel::RunQueryDsl;

        diesel::insert_into(room)
            .values(self)
            .get_result::<Record>(conn)
    }
}
