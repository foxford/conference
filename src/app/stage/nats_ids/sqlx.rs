use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use svc_events::EventId;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Object {
    id: i64,
    entity_type: String,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}

impl Object {
    pub fn id(&self) -> EventId {
        // TODO: add operation
        EventId::from((self.entity_type.clone(), "".to_owned(), self.id))
    }

    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }
}

pub struct ListQuery {
    limit: i64,
}

#[derive(Debug)]
pub struct InsertQuery<'a> {
    entity_type: &'a str,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        entity_type: &'a str,
    ) -> Self {
        Self {
            entity_type,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<EventId> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO nats_event_ids (entity_type)
            VALUES ($1)
            RETURNING
                id,
                entity_type,
                created_at
            "#,
            self.entity_type,
        )
        .fetch_one(conn)
        .await
        .map(|r| r.id())
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
                created_at
            FROM nats_event_ids
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
