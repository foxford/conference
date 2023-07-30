use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

use svc_events::EventId;


pub struct NextSeqId {
    pub value: i64,
}

impl NextSeqId {
    pub fn to_event_id(&self) -> EventId {
        EventId::from(("conference_internal_event".to_string(), "".to_string(), self.value))
    }
}

pub async fn get_next_seq_id(conn: &mut PgConnection) -> sqlx::Result<NextSeqId> {
    sqlx::query_as!(
        NextSeqId,
        r#"SELECT nextval('nats_event_seq_id') as "value!: i64";"#
    )
    .fetch_one(conn)
    .await
}

