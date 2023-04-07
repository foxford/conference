use crate::outbox::{
    db::diesel::Object,
    error::{ErrorKind, PipelineError, PipelineErrorExt, PipelineErrors},
    pipeline::MultipleStagePipelineResult,
    EventId, StageHandle,
};
use chrono::Duration;
use diesel::{
    r2d2::{ConnectionManager, Pool},
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

    fn load_single_record<T>(
        conn: &PgConnection,
        id: &EventId,
    ) -> Result<(Object, T), PipelineError>
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
    ) -> Result<Vec<(Object, T)>, PipelineError>
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

    // This function is not async, because we run this function inside
    // the diesel transaction which is not async too.
    fn handle_record<C, T>(
        &self,
        conn: &PgConnection,
        ctx: &C,
        record: Object,
        stage: T,
    ) -> Result<Option<EventId>, PipelineError>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static,
    {
        let ctx = ctx.clone();
        let id = record.id();
        let event_id = id.clone();
        let rt = tokio::runtime::Handle::current();
        // The `block_on` function in this case doesn't block the main thread of tokio,
        // because it's called inside the `spawn_blocking` function call.
        let result = rt.block_on(<T as StageHandle>::handle(&stage, &ctx, &event_id));

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
                    .error(ErrorKind::DeleteStageFailed)?;

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
                    error.kind(),
                )
                .execute(conn)
                .error(ErrorKind::UpdateStageFailed)?;

                Err(error.into())
            }
        }
    }
}

impl From<diesel::result::Error> for PipelineError {
    fn from(source: diesel::result::Error) -> Self {
        PipelineError::new(ErrorKind::DbQueryFailed, Box::new(source))
    }
}

#[async_trait::async_trait]
impl super::Pipeline for Pipeline {
    async fn run_single_stage<T, C>(&self, ctx: C, id: EventId) -> Result<(), PipelineError>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static,
    {
        let mut id = id;

        loop {
            let result = crate::util::spawn_blocking({
                let ctx = ctx.clone();
                let db = self.db.clone();
                let this = self.clone();

                move || {
                    let conn = db.get().error(ErrorKind::DbConnAcquisitionFailed)?;

                    conn.transaction(|| {
                        let (record, stage): (Object, T) = Self::load_single_record(&conn, &id)?;

                        this.handle_record(&conn, &ctx, record, stage)
                    })
                }
            })
            .await?;

            match result {
                Some(next_id) => {
                    id = next_id;
                }
                None => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn run_multiple_stages<T, C>(
        &self,
        ctx: C,
        records_per_try: i64,
    ) -> Result<MultipleStagePipelineResult, PipelineErrors>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static,
    {
        crate::util::spawn_blocking({
            let ctx = ctx.clone();
            let db = self.db.clone();
            let this = self.clone();
            let mut errors = PipelineErrors::new();

            move || {
                let conn = db.get().error(ErrorKind::DbConnAcquisitionFailed)?;

                conn.transaction::<_, PipelineErrors, _>(|| {
                    let records: Vec<(Object, T)> =
                        Self::load_multiple_records(&conn, records_per_try)?;

                    if records.is_empty() {
                        // Exit from the closure
                        return Ok(MultipleStagePipelineResult::Done);
                    }

                    for (record, stage) in records {
                        // In case of error, we try to handle another record
                        if let Err(err) = this.handle_record(&conn, &ctx, record, stage) {
                            errors.add(err);
                        }
                    }

                    if !errors.is_empty() {
                        return Err(errors);
                    }

                    Ok(MultipleStagePipelineResult::Continue)
                })
            }
        })
        .await
    }
}
