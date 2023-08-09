use sqlx::PgConnection;

use svc_events::EventId;

pub struct NextSeqId {
    pub value: i64,
}

impl NextSeqId {
    pub fn to_event_id(&self, operation: &str) -> EventId {
        EventId::from(("video_group".to_string(), operation.to_string(), self.value))
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
