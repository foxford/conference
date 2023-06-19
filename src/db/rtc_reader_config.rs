use chrono::{DateTime, Utc};
use svc_agent::AgentId;

use crate::{db, db::rtc::Object as Rtc};

////////////////////////////////////////////////////////////////////////////////

pub struct Object {
    #[allow(unused)]
    rtc_id: db::rtc::Id,
    reader_id: AgentId,
    receive_video: bool,
    receive_audio: bool,
}

impl Object {
    #[cfg(test)]
    pub fn rtc_id(&self) -> db::rtc::Id {
        self.rtc_id
    }

    pub fn reader_id(&self) -> &AgentId {
        &self.reader_id
    }

    pub fn receive_video(&self) -> bool {
        self.receive_video
    }

    pub fn receive_audio(&self) -> bool {
        self.receive_audio
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ListWithRtcQuery<'a> {
    room_id: db::room::Id,
    reader_ids: &'a [&'a AgentId],
}

struct ListWithRtcRow {
    rtc_id: db::rtc::Id,
    reader_id: AgentId,
    receive_video: bool,
    receive_audio: bool,
    room_id: db::room::Id,
    created_by: AgentId,
    created_at: DateTime<Utc>,
}

impl ListWithRtcRow {
    fn split(self) -> (Object, Rtc) {
        (
            Object {
                rtc_id: self.rtc_id,
                reader_id: self.reader_id,
                receive_video: self.receive_video,
                receive_audio: self.receive_audio,
            },
            Rtc {
                id: self.rtc_id,
                room_id: self.room_id,
                created_at: self.created_at,
                created_by: self.created_by,
            },
        )
    }
}

impl<'a> ListWithRtcQuery<'a> {
    pub fn new(room_id: db::room::Id, reader_ids: &'a [&'a AgentId]) -> Self {
        Self {
            room_id,
            reader_ids,
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Vec<(Object, Rtc)>> {
        sqlx::query_as!(
            ListWithRtcRow,
            r#"
            SELECT
                rrc.rtc_id as "rtc_id: db::rtc::Id",
                rrc.reader_id as "reader_id: AgentId",
                rrc.receive_video,
                rrc.receive_audio,
                rtc.room_id as "room_id: db::room::Id",
                rtc.created_by as "created_by: AgentId",
                rtc.created_at
            FROM rtc_reader_config as rrc
            INNER JOIN rtc
            ON rrc.rtc_id = rtc.id
            WHERE
                rtc.room_id = $1 AND
                rrc.reader_id = ANY($2)
            "#,
            self.room_id as db::room::Id,
            self.reader_ids as &[&AgentId],
        )
        .fetch_all(conn)
        .await
        .map(|r| r.into_iter().map(|o| o.split()).collect())
    }
}

pub async fn read_config(
    rtc_id: db::rtc::Id,
    connection: &mut sqlx::PgConnection,
) -> sqlx::Result<Vec<Object>> {
    sqlx::query_as!(
        Object,
        r#"
        SELECT
            rtc_id as "rtc_id: db::rtc::Id",
            reader_id as "reader_id: AgentId",
            receive_video,
            receive_audio
        FROM rtc_reader_config
        WHERE
            rtc_id = $1
        "#,
        rtc_id as db::rtc::Id
    )
    .fetch_all(connection)
    .await
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct UpsertQuery<'a> {
    rtc_id: db::rtc::Id,
    reader_id: &'a AgentId,
    receive_video: Option<bool>,
    receive_audio: Option<bool>,
}

impl<'a> UpsertQuery<'a> {
    pub fn new(rtc_id: db::rtc::Id, reader_id: &'a AgentId) -> Self {
        Self {
            rtc_id,
            reader_id,
            receive_video: None,
            receive_audio: None,
        }
    }

    pub fn receive_video(self, receive_video: bool) -> Self {
        Self {
            receive_video: Some(receive_video),
            ..self
        }
    }

    pub fn receive_audio(self, receive_audio: bool) -> Self {
        Self {
            receive_audio: Some(receive_audio),
            ..self
        }
    }

    pub async fn execute(&self, conn: &mut sqlx::PgConnection) -> sqlx::Result<Object> {
        let receive_video = self.receive_video.unwrap_or(true);
        let receive_audio = self.receive_audio.unwrap_or(true);

        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO rtc_reader_config
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (rtc_id, reader_id) DO UPDATE
            SET
                receive_video = $3,
                receive_audio = $4
            RETURNING
                rtc_id as "rtc_id: db::rtc::Id",
                reader_id as "reader_id: AgentId",
                receive_video,
                receive_audio
            "#,
            self.rtc_id as db::rtc::Id,
            self.reader_id as &AgentId,
            receive_video,
            receive_audio,
        )
        .fetch_one(conn)
        .await
    }
}

pub async fn batch_insert(
    conn: &mut sqlx::PgConnection,
    rtc_ids: &[db::rtc::Id],
    reader_ids: &[&AgentId],
    receive_video: &[bool],
    receive_audio: &[bool],
) -> sqlx::Result<Vec<Object>> {
    sqlx::query_as!(
        Object,
        r#"
        INSERT INTO rtc_reader_config
        -- array of agent_id unnests to 2 arrays account_id[] and label[]
        -- so we merge them after unnest back to agent_id type
        SELECT rtc_id, (reader_account_id, reader_label)::agent_id, receive_video, receive_audio
        FROM UNNEST($1::uuid[], $2::agent_id[], $3::bool[], $4::bool[])
            AS t(rtc_id, reader_account_id, reader_label, receive_video, receive_audio)
        ON CONFLICT (rtc_id, reader_id) DO UPDATE
        SET
            receive_video = EXCLUDED.receive_video,
            receive_audio = EXCLUDED.receive_audio
        RETURNING
            rtc_id as "rtc_id: db::rtc::Id",
            reader_id as "reader_id: AgentId",
            receive_video,
            receive_audio
        "#,
        rtc_ids as &[db::rtc::Id],
        reader_ids as &[&AgentId],
        receive_video,
        receive_audio
    )
    .fetch_all(conn)
    .await
}
