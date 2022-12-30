use crate::outbox::{
    db::diesel::Object,
    pipeline::Error,
    pipeline::{ErrorKind, PipelineError, PipelineErrorExt},
    EventId, StageHandle,
};
use anyhow::anyhow;
use chrono::Duration;
use diesel::{
    r2d2::{ConnectionManager, Pool, PooledConnection},
    Connection, PgConnection,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub struct Pipeline {
    db: Arc<Pool<ConnectionManager<PgConnection>>>,
    try_wake_interval: Duration,
    max_delivery_interval: Duration,
}

impl Pipeline {
    pub fn new(
        db: Arc<Pool<ConnectionManager<PgConnection>>>,
        try_wake_interval: Duration,
        max_delivery_interval: Duration,
    ) -> Self {
        Self {
            db,
            try_wake_interval,
            max_delivery_interval,
        }
    }

    async fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || db.get().error(ErrorKind::DbConnAcquisitionFailed))
            .await
            .error(ErrorKind::DbConnAcquisitionFailed)?
    }

    fn load_single_record<T>(conn: &PgConnection, id: &EventId) -> Result<(Object, T), Error>
    where
        T: Serialize + DeserializeOwned,
    {
        let record = crate::outbox::db::diesel::FindQuery::new(id).execute(conn)?;
        let stage = serde_json::from_value::<T>(record.stage().to_owned())
            .error(ErrorKind::DeserializationFailed)?;

        Ok((record, stage))
    }

    fn load_multiple_records<T>(
        conn: &PgConnection,
        records_per_try: i64,
    ) -> Result<Vec<(Object, T)>, Error>
    where
        T: Serialize + DeserializeOwned,
    {
        let records = crate::outbox::db::diesel::ListQuery::new(records_per_try)
            .execute(conn)
            .error(ErrorKind::LoadStagesFailed)?;

        let mut result = Vec::with_capacity(records.len());
        for record in records {
            let stage = serde_json::from_value::<T>(record.stage().to_owned())
                .error(ErrorKind::DeserializationFailed)?;

            result.push((record, stage));
        }

        Ok(result)
    }

    fn handle_record<C, T>(
        &self,
        conn: &PgConnection,
        ctx: C,
        record: Object,
        stage: T,
    ) -> Result<Option<EventId>, Error>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Send + Sync + 'static,
    {
        let id = record.id();

        let rt = tokio::runtime::Handle::current();
        let event_id = id.clone();
        let result = std::thread::spawn(move || {
            rt.block_on(<T as StageHandle>::handle(&stage, &ctx, &event_id))
        })
        .join()
        .map_err(|_| {
            PipelineError::new(
                ErrorKind::RunningStageFailed,
                anyhow!("failed to join thread").into(),
            )
        })?;

        match result {
            Ok(Some(next_stage)) => {
                let record = crate::outbox::db::diesel::DeleteQuery::new(&id)
                    .execute(conn)
                    .error(ErrorKind::DeleteStageFailed)?;

                let json =
                    serde_json::to_value(&next_stage).error(ErrorKind::SerializationFailed)?;

                let event_id = crate::outbox::db::diesel::InsertQuery::new(
                    record.entity_type(),
                    json,
                    record.delivery_deadline_at(),
                )
                .execute(conn)
                .error(ErrorKind::InsertStageFailed)?;

                Ok(Some(event_id))
            }
            Ok(None) => {
                crate::outbox::db::diesel::DeleteQuery::new(&id)
                    .execute(conn)
                    .error(ErrorKind::DeserializationFailed)?;

                Ok(None)
            }
            Err(error) => {
                let delivery_deadline_at = crate::outbox::util::next_delivery_deadline_at(
                    record.retry_count(),
                    record.delivery_deadline_at(),
                    self.try_wake_interval,
                    self.max_delivery_interval,
                );

                crate::outbox::db::diesel::UpdateQuery::new(
                    &id,
                    delivery_deadline_at,
                    error.code() as i16,
                )
                .execute(conn)
                .error(ErrorKind::UpdateStageFailed)?;

                Err(error.into())
            }
        }
    }
}

#[async_trait::async_trait]
impl super::Pipeline for Pipeline {
    async fn run_single_stage<T, C>(&self, ctx: C, id: EventId) -> Result<(), Error>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static,
    {
        let mut id = id;

        loop {
            let conn = self.get_conn().await?;

            let result = conn.transaction(|| {
                let (record, stage): (Object, T) = Self::load_single_record(&conn, &id)?;

                self.handle_record(&conn, ctx.clone(), record, stage)
            });

            match result {
                Ok(Some(next_id)) => {
                    id = next_id;
                }
                Ok(None) => {
                    break;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    async fn run_multiple_stages<T, C>(&self, ctx: C, records_per_try: i64) -> Result<(), Error>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static,
    {
        loop {
            let conn = self.get_conn().await?;

            conn.transaction(|| {
                let records: Vec<(Object, T)> =
                    Self::load_multiple_records(&conn, records_per_try)?;

                if records.is_empty() {
                    return Ok::<_, Error>(());
                }

                for (record, stage) in records {
                    // In case of error, we try to handle another record
                    let _ = self.handle_record(&conn, ctx.clone(), record, stage);
                }

                Ok(())
            })?;
        }
    }
}

impl From<diesel::result::Error> for Error {
    fn from(source: diesel::result::Error) -> Self {
        let error = PipelineError::new(ErrorKind::DbQueryFailed, Box::new(source));
        Self::PipelineError(error)
    }
}
