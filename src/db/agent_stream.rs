use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
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

impl Object {
    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn sent_by(&self) -> Uuid {
        self.sent_by
    }

    pub(crate) fn label(&self) -> &str {
        self.label.as_ref()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "agent_stream"]
pub(crate) struct InsertQuery<'a> {
    id: Option<Uuid>,
    sent_by: Uuid,
    label: &'a str,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(sent_by: Uuid, label: &'a str) -> Self {
        Self {
            id: None,
            sent_by,
            label,
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::schema::agent_stream::dsl::agent_stream;
        use diesel::RunQueryDsl;

        diesel::insert_into(agent_stream)
            .values(self)
            .get_result(conn)
    }
}
