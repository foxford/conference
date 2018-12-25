use crate::schema::rtc;
use crate::transport::AccountId;
use diesel::result::Error;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Queryable)]
pub(crate) struct Record {
    id: Uuid,
    room_id: Uuid,
    owner_id: AccountId,
}

impl Record {
    pub(crate) fn room_id(&self) -> &Uuid {
        &self.room_id
    }

    pub(crate) fn owner_id(&self) -> &AccountId {
        &self.owner_id
    }

    pub(crate) fn id(&self) -> &Uuid {
        &self.id
    }
}

#[derive(Debug, Insertable)]
#[table_name = "rtc"]
pub(crate) struct InsertQuery<'a> {
    id: Option<&'a Uuid>,
    room_id: &'a Uuid,
    owner_id: &'a AccountId,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(room_id: &'a Uuid, owner_id: &'a AccountId) -> Self {
        Self {
            id: None,
            room_id,
            owner_id,
        }
    }

    pub(crate) fn id(self, id: &'a Uuid) -> Self {
        Self {
            id: Some(id),
            room_id: self.room_id,
            owner_id: self.owner_id,
        }
    }

    pub(crate) fn execute(&self) -> Result<Record, Error> {
        use crate::schema::rtc::dsl::rtc;
        use diesel::RunQueryDsl;

        // TODO: replace with db connection pool
        let conn = crate::establish_connection();

        diesel::insert_into(rtc)
            .values(self)
            .get_result::<Record>(&conn)
    }
}
