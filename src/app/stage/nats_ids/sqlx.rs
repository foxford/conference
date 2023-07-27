use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};

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

