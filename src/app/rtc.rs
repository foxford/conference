use crate::schema::rtc;
use crate::transport::AccountId;
use serde_derive::Deserialize;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Queryable)]
pub(crate) struct Rtc {
    id: Uuid,
    room_id: Uuid,
    owner_id: AccountId,
}

#[derive(Debug, Insertable)]
#[table_name = "rtc"]
pub(crate) struct NewRtc<'a> {
    id: Option<&'a Uuid>,
    room_id: &'a Uuid,
    owner_id: &'a AccountId,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct CreateParameters {
    room_id: Uuid,
    owner_id: Option<AccountId>,
}

pub(crate) struct State;

impl State {
    pub(crate) fn create(&self, params: &CreateParameters, subject: &AccountId) -> Result<Rtc, diesel::result::Error> {
        use diesel::RunQueryDsl;
        use crate::schema::rtc::dsl::rtc;

        let value = NewRtc {
            id: None,
            room_id: &params.room_id,
            owner_id: params.owner_id.as_ref().unwrap_or_else(|| subject),
        };

        // TODO: replace with db connection pool
        let conn = crate::establish_connection();
        diesel::insert_into(rtc)
            .values(&value)
            .get_result::<Rtc>(&conn)
    }
}
