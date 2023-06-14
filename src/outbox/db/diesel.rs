use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use svc_nats_client::EventId;

use crate::schema::outbox;

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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at
            FROM outbox
            WHERE
                delivery_deadline_at <= now()
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
            self.limit
        )
        .fetch_all(conn)
        .await
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<EventId> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO outbox (entity_type, stage, delivery_deadline_at)
            VALUES ($1, $2, $3)
            RETURNING
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at
            "#,
            self.entity_type,
            self.stage,
            self.delivery_deadline_at,
        )
        .fetch_one(conn)
        .await
        .map(|r| r.id())
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

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            UPDATE outbox
            SET
                delivery_deadline_at = $1,
                retry_count = retry_count + 1
            WHERE
                id = $2 AND
                entity_type = $3
            RETURNING
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at
            "#,
            self.delivery_deadline_at,
            self.id.sequence_id(),
            self.id.entity_type(),
        )
        .fetch_one(conn)
        .await
    }
}

pub struct DeleteQuery<'a> {
    id: &'a EventId,
}

impl<'a> DeleteQuery<'a> {
    pub fn new(id: &'a EventId) -> Self {
        Self { id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            DELETE FROM outbox
            WHERE
                id = $1 AND
                entity_type = $2
            RETURNING
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at
            "#,
            self.id.sequence_id(),
            self.id.entity_type()
        )
        .fetch_one(conn)
        .await
    }
}

pub struct FindQuery<'a> {
    id: &'a EventId,
}

impl<'a> FindQuery<'a> {
    pub fn new(id: &'a EventId) -> Self {
        Self { id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at
            FROM outbox
            WHERE
                id = $1 AND
                entity_type = $2
            FOR UPDATE SKIP LOCKED
            "#,
            self.id.sequence_id(),
            self.id.entity_type(),
        )
        .fetch_one(conn)
        .await
    }
}
