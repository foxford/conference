use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use svc_events::EventId;

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    operation: String,
}

impl Object {
    pub fn id(&self) -> EventId {
        EventId::from((self.entity_type.clone(), self.operation.clone(), self.id))
    }

    pub fn retry_count(&self) -> i32 {
        self.retry_count
    }

    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    pub fn operation(&self) -> &str {
        &self.operation
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
                created_at,
                operation
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

#[derive(Debug)]
pub struct InsertQuery<'a> {
    entity_type: &'a str,
    stage: JsonValue,
    delivery_deadline_at: DateTime<Utc>,
    operation: &'a str,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        entity_type: &'a str,
        stage: JsonValue,
        delivery_deadline_at: DateTime<Utc>,
        operation: &'a str,
    ) -> Self {
        Self {
            entity_type,
            stage,
            delivery_deadline_at,
            operation,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<EventId> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO outbox (entity_type, stage, delivery_deadline_at, operation)
            VALUES ($1, $2, $3, $4)
            RETURNING
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at,
                operation
            "#,
            self.entity_type,
            self.stage,
            self.delivery_deadline_at,
            self.operation
        )
        .fetch_one(conn)
        .await
        .map(|r| r.id())
    }
}

#[derive(Debug)]
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
                retry_count = retry_count + 1,
                error_kind = $2
            WHERE
                id = $3 AND
                entity_type = $4 AND
                operation = $5
            RETURNING
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at,
                operation
            "#,
            self.delivery_deadline_at,
            self.error_kind,
            self.id.sequence_id(),
            self.id.entity_type(),
            self.id.operation()
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
                entity_type = $2 AND
                operation = $3
            RETURNING
                id,
                entity_type,
                stage,
                delivery_deadline_at,
                error_kind,
                retry_count,
                created_at,
                operation
            "#,
            self.id.sequence_id(),
            self.id.entity_type(),
            self.id.operation()
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
                created_at,
                operation
            FROM outbox
            WHERE
                id = $1 AND
                entity_type = $2 AND
                operation = $3
            FOR UPDATE SKIP LOCKED
            "#,
            self.id.sequence_id(),
            self.id.entity_type(),
            self.id.operation()
        )
        .fetch_one(conn)
        .await
    }
}
