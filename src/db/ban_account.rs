use chrono::{DateTime, Utc};
use svc_agent::AgentId;
use svc_authn::{AccountId, Authenticable};
use uuid::Uuid;

#[derive(Debug)]
pub struct Object {
    #[allow(unused)]
    classroom_id: Uuid,
    #[allow(unused)]
    target: AccountId,
    #[allow(unused)]
    created_at: DateTime<Utc>,
}

pub struct InsertQuery<'a> {
    classroom_id: Uuid,
    target: &'a AccountId,
}

impl<'a> InsertQuery<'a> {
    pub fn new(classroom_id: Uuid, target: &'a AccountId) -> Self {
        Self {
            classroom_id,
            target,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO ban_account (classroom_id, target)
            VALUES ($1, $2)
            ON CONFLICT (classroom_id, target) DO NOTHING
            RETURNING
                classroom_id,
                target as "target: AccountId",
                created_at
            "#,
            self.classroom_id,
            self.target as &AccountId
        )
        .fetch_optional(conn)
        .await
    }
}

pub struct DeleteQuery<'a> {
    target: &'a AccountId,
    classroom_id: Uuid,
}

impl<'a> DeleteQuery<'a> {
    pub fn new(classroom_id: Uuid, target: &'a AccountId) -> Self {
        Self {
            target,
            classroom_id,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            DELETE FROM ban_account
            WHERE
                target = $1
            AND classroom_id = $2
            RETURNING
                classroom_id,
                target as "target: AccountId",
                created_at
            "#,
            self.target as &AccountId,
            self.classroom_id
        )
        .fetch_optional(conn)
        .await
    }
}

pub struct FindQuery<'a> {
    agent: &'a AgentId,
}

impl<'a> FindQuery<'a> {
    pub fn new(agent: &'a AgentId) -> Self {
        Self { agent }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        // the cheapest approach as shown by psql explain
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                ba.classroom_id,
                ba.target AS "target: AccountId",
                ba.created_at
            FROM ban_account AS ba
            INNER JOIN room AS r
            ON r.classroom_id = ba.classroom_id
            WHERE
                ba.target = $1
            "#,
            self.agent.as_account_id() as &AccountId,
        )
        .fetch_optional(conn)
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::{db, prelude::*};

    use super::*;

    #[sqlx::test]
    fn finds_ban_account_entry(pool: sqlx::PgPool) {
        let db = db::TestDb::new(pool);
        let mut conn = db.get_conn().await;

        let room = shared_helpers::insert_room(&mut conn).await;
        let agent = TestAgent::new("web", "agent", USR_AUDIENCE);

        InsertQuery::new(room.classroom_id(), agent.account_id())
            .execute(&mut conn)
            .await
            .expect("failed to insert ban account entry");

        let ban_account_entry = FindQuery::new(agent.agent_id())
            .execute(&mut conn)
            .await
            .expect("failed to execute find query");

        assert!(ban_account_entry.is_some());
    }
}
