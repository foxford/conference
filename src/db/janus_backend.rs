use crate::backend::janus::{
    client::{HandleId, SessionId},
    JANUS_API_VERSION,
};
use crate::db;
use chrono::{DateTime, Utc};
use svc_agent::AgentId;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Object {
    pub id: AgentId,
    pub handle_id: HandleId,
    pub session_id: SessionId,
    pub created_at: DateTime<Utc>,
    pub capacity: Option<i32>,
    pub balancer_capacity: Option<i32>,
    pub api_version: String,
    pub group: Option<String>,
    pub janus_url: String,
}

impl Object {
    pub fn id(&self) -> &AgentId {
        &self.id
    }

    pub fn handle_id(&self) -> HandleId {
        self.handle_id
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn janus_url(&self) -> &str {
        &self.janus_url
    }

    /// Get a reference to the object's group.
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct FindQuery<'a> {
    id: &'a AgentId,
}

impl<'a> FindQuery<'a> {
    pub fn new(id: &'a AgentId) -> Self {
        Self { id }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT
                id as "id: AgentId",
                handle_id as "handle_id: HandleId",
                session_id as "session_id: SessionId",
                created_at,
                capacity,
                balancer_capacity,
                api_version,
                "group",
                janus_url
            FROM janus_backend
            WHERE
                id = $1
            LIMIT 1
            "#,
            self.id as &AgentId
        )
        .fetch_optional(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UpsertQuery<'a> {
    id: &'a AgentId,
    handle_id: HandleId,
    session_id: SessionId,
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
    api_version: String,
    group: Option<&'a str>,
    janus_url: &'a str,
}

impl<'a> UpsertQuery<'a> {
    pub fn new(
        id: &'a AgentId,
        handle_id: HandleId,
        session_id: SessionId,
        janus_url: &'a str,
    ) -> Self {
        Self {
            id,
            handle_id,
            session_id,
            capacity: None,
            balancer_capacity: None,
            api_version: JANUS_API_VERSION.to_string(),
            group: None,
            janus_url,
        }
    }

    pub fn capacity(self, capacity: i32) -> Self {
        Self {
            capacity: Some(capacity),
            ..self
        }
    }

    pub fn balancer_capacity(self, balancer_capacity: i32) -> Self {
        Self {
            balancer_capacity: Some(balancer_capacity),
            ..self
        }
    }

    pub fn group(self, group: &'a str) -> Self {
        Self {
            group: Some(group),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO janus_backend
                (id, handle_id, session_id, capacity, balancer_capacity, api_version, "group", janus_url)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO UPDATE
            SET
                handle_id         = $2,
                session_id        = $3,
                capacity          = COALESCE($4, janus_backend.capacity),
                balancer_capacity = COALESCE($5, janus_backend.balancer_capacity),
                api_version       = $6,
                "group"           = COALESCE($7, janus_backend."group"),
                janus_url         = $8
            RETURNING
                id as "id: AgentId",
                handle_id as "handle_id: HandleId",
                session_id as "session_id: SessionId",
                created_at,
                capacity,
                balancer_capacity,
                api_version,
                "group",
                janus_url
            "#,
            self.id as &AgentId,
            self.handle_id as HandleId,
            self.session_id as SessionId,
            self.capacity,
            self.balancer_capacity,
            self.api_version,
            self.group,
            self.janus_url
        )
        .fetch_one(conn)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct DeleteQuery<'a> {
    id: &'a AgentId,
    session_id: SessionId,
    handle_id: HandleId,
}

impl<'a> DeleteQuery<'a> {
    pub fn new(id: &'a AgentId, session_id: SessionId, handle_id: HandleId) -> Self {
        Self {
            id,
            session_id,
            handle_id,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<u64> {
        sqlx::query!(
            r#"
            DELETE FROM janus_backend
            WHERE
                id = $1 AND
                session_id = $2 AND
                handle_id = $3
            "#,
            self.id as &AgentId,
            self.session_id as SessionId,
            self.handle_id as HandleId
        )
        .execute(conn)
        .await
        .map(|r| r.rows_affected())
    }
}

////////////////////////////////////////////////////////////////////////////////

// Returns the most loaded backend capable to host the room with its reserve considering:
// - room opening period;
// - actual number of online agents;
// - optional backend capacity;
// - optional room reserve;
// - writer's bitrate;
// - possible multiple RTCs in each room.
pub async fn most_loaded(
    room_id: db::room::Id,
    group: Option<&str>,
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<Option<Object>> {
    sqlx::query_as!(
        Object,
        r#"
        WITH
            room_load AS (
                SELECT
                    a.room_id,
                    SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
                FROM agent AS a
                INNER JOIN agent_connection AS ac
                ON ac.agent_id = a.id
                LEFT JOIN rtc_writer_config AS rwc
                ON rwc.rtc_id = ac.rtc_id
                GROUP BY a.room_id
            ),
            active_room AS (
                SELECT *
                FROM room
                WHERE backend_id IS NOT NULL
                AND   time @> NOW()
            ),
            janus_backend_load AS (
                SELECT
                    backend_id,
                    SUM(GREATEST(taken, reserve)) AS load
                FROM (
                    SELECT DISTINCT ON(backend_id, room_id)
                        ar.backend_id,
                        ar.id                   AS room_id,
                        COALESCE(rl.taken, 0)   AS taken,
                        COALESCE(ar.reserve, 0) AS reserve
                    FROM active_room AS ar
                    LEFT JOIN room_load AS rl
                    ON rl.room_id = ar.id
                ) AS sub
                GROUP BY backend_id
            )
        SELECT
            jb.id as "id: AgentId",
            jb.handle_id as "handle_id: HandleId",
            jb.session_id as "session_id: SessionId",
            jb.created_at,
            jb.capacity,
            jb.balancer_capacity,
            jb.api_version,
            jb."group",
            jb.janus_url
        FROM janus_backend AS jb
        LEFT JOIN janus_backend_load AS jbl
        ON jbl.backend_id = jb.id
        LEFT JOIN room AS r2
        ON 1 = 1
        WHERE r2.id = $1
        AND   COALESCE(jb.balancer_capacity, jb.capacity, 2147483647) - COALESCE(jbl.load, 0) >= COALESCE(r2.reserve, 1)
        AND   jb.api_version = $2
        AND   ($3::text IS NULL OR jb."group" = $3::text)
        ORDER BY COALESCE(jbl.load, 0) DESC, RANDOM()
        LIMIT 1
        "#,
        room_id as db::room::Id,
        JANUS_API_VERSION,
        group,
    ).fetch_optional(conn).await
}

// The same as above but finds the least loaded backend instead without considering the reserve.
pub async fn least_loaded(
    room_id: db::room::Id,
    group: Option<&str>,
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<Option<Object>> {
    sqlx::query_as!(
        Object,
        r#"
        WITH
            room_load AS (
                SELECT
                    a.room_id,
                    SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
                FROM agent AS a
                INNER JOIN agent_connection AS ac
                ON ac.agent_id = a.id
                LEFT JOIN rtc_writer_config AS rwc
                ON rwc.rtc_id = ac.rtc_id
                GROUP BY a.room_id
            ),
            active_room AS (
                SELECT *
                FROM room
                WHERE backend_id IS NOT NULL
                AND   time @> NOW()
            ),
            janus_backend_load AS (
                SELECT
                    backend_id,
                    SUM(taken) AS load
                FROM (
                    SELECT DISTINCT ON(backend_id, room_id)
                        ar.backend_id,
                        ar.id                 AS room_id,
                        COALESCE(rl.taken, 0) AS taken
                    FROM active_room AS ar
                    LEFT JOIN room_load AS rl
                    ON rl.room_id = ar.id
                ) AS sub
                GROUP BY backend_id
            ),
            least_loaded AS (
                SELECT jb.*
                FROM janus_backend AS jb
                LEFT JOIN janus_backend_load AS jbl
                ON jbl.backend_id = jb.id
                LEFT JOIN room AS r2
                ON 1 = 1
                WHERE r2.id = $1
                AND   jb.api_version = $2
                AND   ($3::text IS NULL OR jb."group" = $3::text)
                ORDER BY
                    COALESCE(jb.balancer_capacity, jb.capacity, 2147483647) - COALESCE(jbl.load, 0) DESC
                LIMIT 3
            )
        SELECT
            id as "id: AgentId",
            handle_id as "handle_id: HandleId",
            session_id as "session_id: SessionId",
            created_at,
            capacity,
            balancer_capacity,
            api_version,
            "group",
            janus_url
        FROM least_loaded
        ORDER BY RANDOM()
        LIMIT 1
        "#,
        room_id as db::room::Id,
        JANUS_API_VERSION,
        group,
    )
    .fetch_optional(conn)
    .await
}

////////////////////////////////////////////////////////////////////////////////

struct FreeCapacityQueryRow {
    free_capacity: i32,
}

// Similar to the previous one but returns the number of free slots for the room on the backend
// that hosts the active stream for the given RTC.
pub async fn free_capacity(
    rtc_id: db::rtc::Id,
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<i32> {
    sqlx::query_as!(
        FreeCapacityQueryRow,
        r#"
        WITH
            room_load AS (
                SELECT
                    a.room_id,
                    SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
                FROM agent AS a
                INNER JOIN agent_connection AS ac
                ON ac.agent_id = a.id
                LEFT JOIN rtc_writer_config AS rwc
                ON rwc.rtc_id = ac.rtc_id
                GROUP BY a.room_id
            ),
            active_room AS (
                SELECT *
                FROM room
                WHERE backend_id IS NOT NULL
                AND   time @> NOW()
            ),
            janus_backend_load AS (
                SELECT
                    backend_id,
                    SUM(taken) AS total_taken,
                    SUM(reserve) AS total_reserve,
                    SUM(GREATEST(taken, reserve)) AS load
                FROM (
                    SELECT DISTINCT ON(backend_id, room_id)
                        ar.backend_id,
                        ar.id                   AS room_id,
                        COALESCE(rl.taken, 0)   AS taken,
                        COALESCE(ar.reserve, 0) AS reserve
                    FROM active_room AS ar
                    LEFT JOIN room_load AS rl
                    ON rl.room_id = ar.id
                ) AS sub
                GROUP BY backend_id
            )
        SELECT
            (
                CASE
                    WHEN COALESCE(jb.capacity, 2147483647) <= COALESCE(jbl.total_taken, 0) THEN 0
                    ELSE (
                        GREATEST(
                            (
                                CASE
                                    WHEN COALESCE(ar.reserve, 0) > COALESCE(rl.taken, 0)
                                        THEN LEAST(
                                            COALESCE(ar.reserve, 0) - COALESCE(rl.taken, 0),
                                            COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.total_taken, 0)
                                        )
                                    ELSE
                                        GREATEST(COALESCE(jb.capacity, 2147483647) - COALESCE(jbl.load, 0), 0)
                                END
                            ),
                        1)
                    )
                END
            )::INT AS "free_capacity!: i32"
        FROM rtc
        LEFT JOIN active_room AS ar
        ON ar.id = rtc.room_id
        LEFT JOIN room_load as rl
        ON rl.room_id = rtc.room_id
        LEFT JOIN janus_backend AS jb
        ON jb.id = ar.backend_id
        LEFT JOIN janus_backend_load AS jbl
        ON jbl.backend_id = jb.id
        WHERE rtc.id = $1
        "#,
        rtc_id as db::room::Id,
        )
        .fetch_one(conn)
        .await
        .map(|r| r.free_capacity)
}

////////////////////////////////////////////////////////////////////////////////

pub struct TotalCapacityResult {
    total_capacity: Option<i64>,
}

impl TotalCapacityResult {
    pub fn total_capacity(&self) -> i64 {
        self.total_capacity.as_ref().copied().unwrap_or(0)
    }
}

pub async fn total_capacity(conn: &mut sqlx::PgConnection) -> sqlx::Result<TotalCapacityResult> {
    sqlx::query_as!(
        TotalCapacityResult,
        r#"
        SELECT SUM(capacity) as "total_capacity: i64"
        FROM janus_backend
        "#
    )
    .fetch_one(conn)
    .await
}

pub struct CountResult {
    pub count: i64,
}

pub async fn count(conn: &mut sqlx::PgConnection) -> sqlx::Result<CountResult> {
    sqlx::query_as!(
        CountResult,
        r#"
        SELECT count(id) as "count!: i64"
        FROM janus_backend
        "#
    )
    .fetch_one(conn)
    .await
}

#[derive(Debug)]
pub struct ReserveLoadQueryLoad {
    pub backend_id: AgentId,
    pub load: i64,
    pub taken: i64,
}

pub async fn reserve_load_for_each_backend(
    conn: &mut sqlx::PgConnection,
) -> sqlx::Result<Vec<ReserveLoadQueryLoad>> {
    sqlx::query_as!(
        ReserveLoadQueryLoad,
        r#"
        WITH
        room_load AS (
            SELECT
                a.room_id,
                SUM(COALESCE(rwc.video_remb, 1000000) / 1000000.0) AS taken
            FROM agent AS a
            INNER JOIN agent_connection AS ac
            ON ac.agent_id = a.id
            LEFT JOIN rtc_writer_config AS rwc
            ON rwc.rtc_id = ac.rtc_id
            GROUP BY a.room_id
        ),
        active_room AS (
            SELECT *
            FROM room
            WHERE backend_id IS NOT NULL
            AND   time @> NOW()
        ),
        janus_backend_load AS (
            SELECT
                backend_id,
                SUM(reserve) AS load,
                SUM(taken) AS taken
            FROM (
                SELECT DISTINCT ON(backend_id, room_id)
                    ar.backend_id,
                    ar.id                   AS room_id,
                    COALESCE(rl.taken, 0)   AS taken,
                    COALESCE(ar.reserve, 0) AS reserve
                FROM active_room AS ar
                LEFT JOIN room_load AS rl
                ON rl.room_id = ar.id
            ) AS sub
            GROUP BY backend_id
        )
    SELECT
        jb.id AS "backend_id: AgentId",
        COALESCE(jbl.load, 0)::BIGINT as "load!: i64",
        COALESCE(jbl.taken, 0)::BIGINT as "taken!: i64"
    FROM janus_backend jb
    LEFT OUTER JOIN janus_backend_load jbl
    ON jb.id = jbl.backend_id;
        "#
    )
    .fetch_all(conn)
    .await
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use std::ops::Bound;

    use crate::{
        backend::janus::client::{HandleId, SessionId},
        db::rtc::SharingPolicy as RtcSharingPolicy,
        test_helpers::{db::TestDb, prelude::*},
    };

    #[sqlx::test]
    async fn reserve_load_for_each_backend(pool: sqlx::PgPool) {
        // Insert an rtc and janus backend.
        let now = Utc::now();

        let mut conn = TestDb::new(pool).get_conn().await;

        // Insert janus backends.
        let backend1 = shared_helpers::insert_janus_backend(
            &mut conn,
            "test",
            SessionId::random(),
            HandleId::random(),
        )
        .await;
        let backend2 = shared_helpers::insert_janus_backend(
            &mut conn,
            "test",
            SessionId::random(),
            HandleId::random(),
        )
        .await;
        let backend3 = shared_helpers::insert_janus_backend(
            &mut conn,
            "test",
            SessionId::random(),
            HandleId::random(),
        )
        .await;

        let room1 = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(now),
                Bound::Excluded(now + Duration::hours(1)),
            ))
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .backend_id(backend1.id())
            .reserve(200)
            .insert(&mut conn)
            .await;

        let room2 = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(now),
                Bound::Excluded(now + Duration::hours(1)),
            ))
            .reserve(300)
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .backend_id(backend1.id())
            .insert(&mut conn)
            .await;

        let room3 = factory::Room::new()
            .audience(USR_AUDIENCE)
            .time((
                Bound::Included(now),
                Bound::Excluded(now + Duration::hours(1)),
            ))
            .reserve(400)
            .rtc_sharing_policy(RtcSharingPolicy::Shared)
            .backend_id(backend2.id())
            .insert(&mut conn)
            .await;

        shared_helpers::insert_rtc_with_room(&mut conn, &room1).await;
        shared_helpers::insert_rtc_with_room(&mut conn, &room2).await;
        shared_helpers::insert_rtc_with_room(&mut conn, &room3).await;

        let loads = super::reserve_load_for_each_backend(&mut conn)
            .await
            .expect("Db query failed");
        assert_eq!(loads.len(), 3);

        [backend1, backend2, backend3]
            .iter()
            .zip([500, 400, 0].iter())
            .for_each(|(backend, expected_load)| {
                let b = loads
                    .iter()
                    .find(|load| load.backend_id == *backend.id())
                    .expect("Failed to find backend in query results");

                assert_eq!(b.load, *expected_load as i64);
            });
    }
}
