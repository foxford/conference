use crate::schema::outbox;
use chrono::{serde::ts_seconds, DateTime, Utc};
use diesel::{result::Error, PgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use svc_nats_client::EventId;

type AllColumns = (
    outbox::id,
    outbox::entity_type,
    outbox::stage,
    outbox::delivery_deadline_at,
    outbox::error_kind,
    outbox::retry_count,
    outbox::created_at,
);

const ALL_COLUMNS: AllColumns = (
    outbox::id,
    outbox::entity_type,
    outbox::stage,
    outbox::delivery_deadline_at,
    outbox::error_kind,
    outbox::retry_count,
    outbox::created_at,
);

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Clone)]
#[table_name = "outbox"]
#[primary_key(entity_type, id)]
pub struct Object {
    id: i64,
    entity_type: String,
    stage: JsonValue,
    #[serde(with = "ts_seconds")]
    delivery_deadline_at: DateTime<Utc>,
    error_kind: Option<String>,
    retry_count: i32,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    pub fn id(&self) -> EventId {
        EventId::from((self.entity_type.clone(), self.id))
    }

    pub fn retry_count(&self) -> i32 {
        self.retry_count
    }

    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    pub fn stage(&self) -> &JsonValue {
        &self.stage
    }

    pub fn delivery_deadline_at(&self) -> DateTime<Utc> {
        self.delivery_deadline_at
    }
}

pub struct ListQuery {
    limit: i64,
}

impl ListQuery {
    pub fn new(limit: i64) -> Self {
        Self { limit }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::dsl::sql;
        use diesel::prelude::*;

        outbox::table
            .filter(sql("delivery_deadline_at <= now()"))
            .limit(self.limit)
            .select(ALL_COLUMNS)
            .for_update()
            .skip_locked()
            .get_results(conn)
    }
}

#[derive(Debug, Insertable)]
#[table_name = "outbox"]
pub struct InsertQuery<'a> {
    entity_type: &'a str,
    stage: JsonValue,
    delivery_deadline_at: DateTime<Utc>,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        entity_type: &'a str,
        stage: JsonValue,
        delivery_deadline_at: DateTime<Utc>,
    ) -> Self {
        Self {
            entity_type,
            stage,
            delivery_deadline_at,
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<EventId, Error> {
        use crate::schema::outbox::dsl::*;

        let record = diesel::insert_into(outbox)
            .values(self)
            .get_result::<Object>(conn)?;

        Ok(record.id())
    }
}

#[derive(Debug, AsChangeset)]
#[table_name = "outbox"]
pub struct UpdateQuery<'a> {
    id: &'a EventId,
    delivery_deadline_at: DateTime<Utc>,
    error_kind: &'a str,
}

impl<'a> UpdateQuery<'a> {
    pub fn new(id: &'a EventId, delivery_deadline_at: DateTime<Utc>, error_kind: &'a str) -> Self {
        Self {
            id,
            delivery_deadline_at,
            error_kind,
        }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        let query = outbox::table
            .filter(outbox::id.eq(self.id.sequence_id()))
            .filter(outbox::entity_type.eq(self.id.entity_type()));

        diesel::update(query)
            .set((self, outbox::retry_count.eq(outbox::retry_count + 1)))
            .get_result(conn)
    }
}

pub struct DeleteQuery<'a> {
    id: &'a EventId,
}

impl<'a> DeleteQuery<'a> {
    pub fn new(id: &'a EventId) -> Self {
        Self { id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        let query = outbox::table
            .filter(outbox::id.eq(self.id.sequence_id()))
            .filter(outbox::entity_type.eq(self.id.entity_type()));

        diesel::delete(query).get_result(conn)
    }
}

pub struct FindQuery<'a> {
    id: &'a EventId,
}

impl<'a> FindQuery<'a> {
    pub fn new(id: &'a EventId) -> Self {
        Self { id }
    }

    pub fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        outbox::table
            .filter(outbox::id.eq(self.id.sequence_id()))
            .filter(outbox::entity_type.eq(&self.id.entity_type()))
            .for_update()
            .skip_locked()
            .get_result(conn)
    }
}
