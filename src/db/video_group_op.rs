use sqlx::PgConnection;

pub struct NextSeqId {
    pub value: i64,
}

pub async fn get_next_seq_id(conn: &mut PgConnection) -> sqlx::Result<NextSeqId> {
    sqlx::query_as!(
        NextSeqId,
        r#"SELECT nextval('video_group_entity_seq_id') as "value!: i64";"#
    )
    .fetch_one(conn)
    .await
}
