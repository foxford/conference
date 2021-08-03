use std::fmt;

use crate::db;
use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use diesel_derive_enum::DbEnum;
use serde::{Deserialize, Serialize};
use svc_agent::AgentId;
use uuid::Uuid;

use super::{recording::Object as Recording, room::Object as Room};
use crate::schema::{recording, rtc};
use derive_more::{Display, FromStr};
use diesel_derive_newtype::DieselNewType;

////////////////////////////////////////////////////////////////////////////////

pub type AllColumns = (rtc::id, rtc::room_id, rtc::created_at, rtc::created_by);

pub const ALL_COLUMNS: AllColumns = (rtc::id, rtc::room_id, rtc::created_at, rtc::created_by);

////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug, Deserialize, Serialize, Display, Copy, Clone, DieselNewType, Hash, PartialEq, Eq, FromStr,
)]
pub struct Id(Uuid);

impl Id {
    pub fn random() -> Self {
        Id(Uuid::new_v4())
    }
}

#[derive(Clone, Copy, Debug, DbEnum, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[DieselType = "Rtc_sharing_policy"]
pub enum SharingPolicy {
    None,
    Shared,
    Owned,
}

impl fmt::Display for SharingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let serialized = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        write!(f, "{}", serialized)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(
    Clone, Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations,
)]
#[belongs_to(Room, foreign_key = "room_id")]
#[table_name = "rtc"]
pub struct Object {
    id: Id,
    room_id: db::room::Id,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
    created_by: AgentId,
}

impl Object {
    pub fn id(&self) -> Id {
        self.id
    }

    pub fn room_id(&self) -> db::room::Id {
        self.room_id
    }

    pub fn created_by(&self) -> &AgentId {
        &self.created_by
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct FindQuery {
    id: Option<Id>,
}

impl FindQuery {
    pub fn new() -> Self {
        Self { id: None }
    }

    pub fn id(mut self, id: Id) -> Self {
        self.id = Some(id);
        self
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Option<Object>, Error> {
        use diesel::prelude::*;

        match self.id {
            Some(id) => rtc::table.find(id).get_result(conn).optional(),
            _ => Err(Error::QueryBuilderError(
                "id is required parameters of the query".into(),
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ListQuery<'a> {
    room_id: Option<db::room::Id>,
    created_by: Option<&'a [&'a AgentId]>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl<'a> ListQuery<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn room_id(self, room_id: db::room::Id) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub fn created_by(self, created_by: &'a [&'a AgentId]) -> Self {
        Self {
            created_by: Some(created_by),
            ..self
        }
    }

    pub fn offset(self, offset: i64) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    pub fn limit(self, limit: i64) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        let mut q = rtc::table.into_boxed();

        if let Some(room_id) = self.room_id {
            q = q.filter(rtc::room_id.eq(room_id));
        }

        if let Some(created_by) = self.created_by {
            q = q.filter(rtc::created_by.eq_any(created_by))
        }

        if let Some(offset) = self.offset {
            q = q.offset(offset);
        }

        if let Some(limit) = self.limit {
            q = q.limit(limit);
        }

        q.order_by(rtc::created_at.asc()).get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct ListWithReadyRecordingQuery {
    room_id: db::room::Id,
}

impl ListWithReadyRecordingQuery {
    pub fn new(room_id: db::room::Id) -> Self {
        Self { room_id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<(Object, Option<Recording>)>, Error> {
        use diesel::prelude::*;

        rtc::table
            .left_join(recording::table)
            .filter(rtc::room_id.eq(self.room_id))
            .filter(recording::status.eq(db::recording::Status::Ready))
            .get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "rtc"]
pub struct InsertQuery<'a> {
    id: Option<Id>,
    room_id: db::room::Id,
    created_by: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub fn new(room_id: db::room::Id, created_by: &'a AgentId) -> Self {
        Self {
            id: None,
            room_id,
            created_by,
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::rtc::dsl::rtc;
        use diesel::RunQueryDsl;

        diesel::insert_into(rtc).values(self).get_result(conn)
    }
}
