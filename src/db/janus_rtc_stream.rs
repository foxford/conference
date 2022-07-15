// in order to support Rust 1.62
// `diesel::AsChangeset` or `diesel::Insertable` causes this clippy warning
#![allow(clippy::extra_unused_lifetimes)]

use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use svc_agent::AgentId;
use uuid::Uuid;

use crate::{
    backend::janus::client::HandleId,
    db,
    schema::{janus_rtc_stream, rtc},
};
use derive_more::{Display, FromStr};
use diesel_derive_newtype::DieselNewType;

////////////////////////////////////////////////////////////////////////////////

pub type Time = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

////////////////////////////////////////////////////////////////////////////////

type AllColumns = (
    janus_rtc_stream::id,
    janus_rtc_stream::handle_id,
    janus_rtc_stream::rtc_id,
    janus_rtc_stream::backend_id,
    janus_rtc_stream::label,
    janus_rtc_stream::sent_by,
    janus_rtc_stream::time,
    janus_rtc_stream::created_at,
);
const ALL_COLUMNS: AllColumns = (
    janus_rtc_stream::id,
    janus_rtc_stream::handle_id,
    janus_rtc_stream::rtc_id,
    janus_rtc_stream::backend_id,
    janus_rtc_stream::label,
    janus_rtc_stream::sent_by,
    janus_rtc_stream::time,
    janus_rtc_stream::created_at,
);

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

#[derive(Debug, Deserialize, Serialize, Identifiable, Queryable, QueryableByName, Associations)]
#[table_name = "janus_rtc_stream"]
pub struct Object {
    id: Id,
    handle_id: HandleId,
    rtc_id: db::rtc::Id,
    backend_id: AgentId,
    label: String,
    sent_by: AgentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<Time>,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    pub fn id(&self) -> Id {
        self.id
    }

    #[cfg(test)]
    pub fn handle_id(&self) -> HandleId {
        self.handle_id
    }

    pub fn rtc_id(&self) -> db::rtc::Id {
        self.rtc_id
    }

    pub fn backend_id(&self) -> &AgentId {
        &self.backend_id
    }

    #[cfg(test)]
    pub fn label(&self) -> &str {
        self.label.as_ref()
    }

    pub fn sent_by(&self) -> &AgentId {
        &self.sent_by
    }

    pub fn time(&self) -> Option<Time> {
        self.time
    }

    #[cfg(test)]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn set_time(&mut self, time: Option<Time>) -> &mut Self {
        self.time = time;
        self
    }
}

////////////////////////////////////////////////////////////////////////////////

const ACTIVE_SQL: &str = r#"(
    lower("janus_rtc_stream"."time") is not null
    and upper("janus_rtc_stream"."time") is null
)"#;

#[derive(Debug, Default)]
pub struct ListQuery {
    room_id: Option<db::room::Id>,
    rtc_id: Option<db::rtc::Id>,
    time: Option<Time>,
    active: Option<bool>,
    offset: Option<i64>,
    limit: Option<i64>,
}

impl ListQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn room_id(self, room_id: db::room::Id) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub fn rtc_id(self, rtc_id: db::rtc::Id) -> Self {
        Self {
            rtc_id: Some(rtc_id),
            ..self
        }
    }

    pub fn time(self, time: Time) -> Self {
        Self {
            time: Some(time),
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
        use diesel::{dsl::sql, prelude::*, sql_types::Tstzrange};

        let mut q = janus_rtc_stream::table.into_boxed();
        if let Some(rtc_id) = self.rtc_id {
            q = q.filter(janus_rtc_stream::rtc_id.eq(rtc_id));
        }
        if let Some(time) = self.time {
            q = q.filter(sql("time && ").bind::<Tstzrange, _>(time));
        }
        match self.active {
            None => (),
            Some(true) => q = q.filter(sql(ACTIVE_SQL)),
            Some(false) => q = q.filter(sql(&format!("not {}", ACTIVE_SQL))),
        }
        if let Some(offset) = self.offset {
            q = q.offset(offset);
        }
        if let Some(limit) = self.limit {
            q = q.limit(limit);
        }

        if let Some(room_id) = self.room_id {
            return q
                .inner_join(rtc::table)
                .filter(rtc::room_id.eq(room_id))
                .select(ALL_COLUMNS)
                .order_by(janus_rtc_stream::created_at.desc())
                .get_results(conn);
        }

        q.order_by(janus_rtc_stream::created_at.desc())
            .get_results(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable, AsChangeset)]
#[table_name = "janus_rtc_stream"]
pub struct InsertQuery<'a> {
    id: Id,
    handle_id: HandleId,
    rtc_id: db::rtc::Id,
    backend_id: &'a AgentId,
    label: &'a str,
    sent_by: &'a AgentId,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        id: Id,
        handle_id: HandleId,
        rtc_id: db::rtc::Id,
        backend_id: &'a AgentId,
        label: &'a str,
        sent_by: &'a AgentId,
    ) -> Self {
        Self {
            id,
            handle_id,
            rtc_id,
            backend_id,
            label,
            sent_by,
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::janus_rtc_stream::dsl::janus_rtc_stream;
        use diesel::RunQueryDsl;

        diesel::insert_into(janus_rtc_stream)
            .values(self)
            .get_result(conn)
    }
}

////////////////////////////////////////////////////////////////////////////////

const START_TIME_SQL: &str = "(TSTZRANGE(NOW(), NULL, '[)'))";

pub fn start(id: db::janus_rtc_stream::Id, conn: &PgConnection) -> Result<Option<Object>, Error> {
    use diesel::{dsl::sql, prelude::*};

    diesel::update(janus_rtc_stream::table.filter(janus_rtc_stream::id.eq(id)))
        .set(janus_rtc_stream::time.eq(sql(START_TIME_SQL)))
        .get_result(conn)
        .optional()
}

// Close the stream with current timestamp.
// Fall back to start + 1 ms when closing instantly after starting because lower and upper
// values of a range can't be equal in Postgres.
const STOP_TIME_SQL: &str = r#"
    (
        CASE WHEN "time" IS NOT NULL THEN
            TSTZRANGE(
                LOWER("time"),
                GREATEST(NOW(), LOWER("time") + '1 millisecond'::INTERVAL),
                '[)'
            )
        END
    )
"#;

pub fn stop(id: db::janus_rtc_stream::Id, conn: &PgConnection) -> Result<Option<Object>, Error> {
    use diesel::{dsl::sql, prelude::*};

    diesel::update(janus_rtc_stream::table.filter(janus_rtc_stream::id.eq(id)))
        .set(janus_rtc_stream::time.eq(sql(STOP_TIME_SQL)))
        .get_result(conn)
        .optional()
}
