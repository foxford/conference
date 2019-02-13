use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::result::Error;
use serde_derive::{Deserialize, Serialize};
use svc_agent::AgentId;
use uuid::Uuid;

use crate::schema::{room, rtc};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(room::Object, foreign_key = "room_id")]
#[table_name = "rtc"]
pub(crate) struct Object {
    id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<RtcState>,
    room_id: Uuid,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    pub(crate) fn id(&self) -> &Uuid {
        &self.id
    }

    pub(crate) fn room_id(&self) -> &Uuid {
        &self.room_id
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize, Serialize, FromSqlRow, AsExpression)]
#[sql_type = "sql::Rtc_state"]
pub(crate) struct RtcState {
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sent_by: Option<AgentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "crate::serde::ts_seconds_option")]
    sent_at: Option<DateTime<Utc>>,
}

impl RtcState {
    pub(crate) fn new(
        label: &str,
        sent_by: Option<AgentId>,
        sent_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            label: label.to_owned(),
            sent_by,
            sent_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FindQuery<'a> {
    id: &'a Uuid,
}

impl<'a> FindQuery<'a> {
    pub(crate) fn new(id: &'a Uuid) -> Self {
        Self { id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        rtc::table.find(self.id).get_result(conn).optional()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ListQuery<'a> {
    room_id: Option<&'a Uuid>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl<'a> ListQuery<'a> {
    pub(crate) fn new() -> Self {
        Self {
            room_id: None,
            offset: None,
            limit: None,
        }
    }

    pub(crate) fn from_options(
        room_id: Option<&'a Uuid>,
        offset: Option<i64>,
        limit: Option<i64>,
    ) -> Self {
        Self {
            room_id: room_id,
            offset: offset,
            limit: limit,
        }
    }

    pub(crate) fn room_id(self, room_id: &'a Uuid) -> Self {
        Self {
            room_id: Some(room_id),
            offset: self.offset,
            limit: self.limit,
        }
    }

    pub(crate) fn offset(self, offset: i64) -> Self {
        Self {
            room_id: self.room_id,
            offset: Some(offset),
            limit: self.limit,
        }
    }

    pub(crate) fn limit(self, limit: i64) -> Self {
        Self {
            room_id: self.room_id,
            offset: self.offset,
            limit: Some(limit),
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        let mut q = rtc::table.into_boxed();
        if let Some(room_id) = self.room_id {
            q = q.filter(rtc::room_id.eq(room_id));
        }
        if let Some(offset) = self.offset {
            q = q.offset(offset);
        }
        if let Some(limit) = self.limit {
            q = q.limit(limit);
        }
        q.order_by(rtc::created_at).get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "rtc"]
pub(crate) struct InsertQuery<'a> {
    id: Option<&'a Uuid>,
    room_id: &'a Uuid,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(room_id: &'a Uuid) -> Self {
        Self { id: None, room_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::rtc::dsl::rtc;
        use diesel::RunQueryDsl;

        diesel::insert_into(rtc).values(self).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Identifiable, AsChangeset)]
#[table_name = "rtc"]
pub(crate) struct UpdateQuery<'a> {
    id: &'a Uuid,
    state: Option<&'a RtcState>,
}

impl<'a> UpdateQuery<'a> {
    pub(crate) fn new(id: &'a Uuid) -> Self {
        Self { id, state: None }
    }

    pub(crate) fn state(self, state: &'a RtcState) -> Self {
        Self {
            id: self.id,
            state: Some(state),
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        diesel::update(self).set(self).get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) fn update_state(id: &Uuid, conn: &PgConnection) -> Result<Object, Error> {
    use diesel::prelude::*;

    let q = format!(
        "update rtc set state.sent_at = now() where id = '{}' ::uuid returning *",
        id,
    );
    diesel::sql_query(q).get_result(conn)
}

// NOTE: erase all state fields but 'label' in order to be able to recognize a previously created rtc
pub(crate) fn delete_state(id: &Uuid, conn: &PgConnection) -> Result<Object, Error> {
    use diesel::prelude::*;

    let q = format!(
        "update rtc set state.sent_by = null, state.sent_at = null where id = '{}' ::uuid returning *",
        id,
    );
    diesel::sql_query(q).get_result(conn)
}

////////////////////////////////////////////////////////////////////////////////

pub mod sql {

    use chrono::{DateTime, Utc};
    use diesel::deserialize::{self, FromSql};
    use diesel::pg::Pg;
    use diesel::serialize::{self, Output, ToSql, WriteTuple};
    use diesel::sql_types::{Nullable, Record, Text, Timestamptz};
    use std::io::Write;
    use svc_agent::sql::Agent_id;
    use svc_agent::AgentId;

    use super::RtcState;

    #[derive(SqlType, QueryId)]
    #[postgres(type_name = "rtc_state")]
    #[allow(non_camel_case_types)]
    pub struct Rtc_state;

    impl ToSql<Rtc_state, Pg> for RtcState {
        fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
            WriteTuple::<(Text, Nullable<Agent_id>, Nullable<Timestamptz>)>::write_tuple(
                &(&self.label, &self.sent_by, &self.sent_at),
                out,
            )
        }
    }

    impl FromSql<Rtc_state, Pg> for RtcState {
        fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
            let (label, sent_by, sent_at): (String, Option<AgentId>, Option<DateTime<Utc>>) =
                FromSql::<Record<(Text, Nullable<Agent_id>, Nullable<Timestamptz>)>, Pg>::from_sql(
                    bytes,
                )?;
            Ok(RtcState::new(&label, sent_by, sent_at))
        }
    }

}
