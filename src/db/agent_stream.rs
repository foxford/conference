use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use super::agent::Object as Agent;
use crate::schema::agent_stream;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, QueryableByName, Associations)]
#[belongs_to(Agent, foreign_key = "sent_by")]
#[table_name = "agent_stream"]
pub(crate) struct Object {
    id: Uuid,
    sent_by: Uuid,
    label: String,
    #[serde(with = "ts_seconds")]
    created_at: DateTime<Utc>,
}
