use chrono::Duration;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::Connection;

use crate::outbox::{
    db::sqlx::Object,
    error::{ErrorKind, PipelineError, PipelineErrorExt, PipelineErrors},
    pipeline::MultipleStagePipelineResult,
    EventId, StageHandle,
};

#[derive(Clone)]
pub struct Pipeline {
    db: sqlx::PgPool,
    try_wake_interval: Duration,
    max_delivery_interval: Duration,
}

impl Pipeline {
    pub fn new(
        db: sqlx::PgPool,
        try_wake_interval: Duration,
        max_delivery_interval: Duration,
    ) -> Self {
        Self {
            db,
            try_wake_interval,
            max_delivery_interval,
        }
    }

    async fn load_single_record<T>(
        conn: &mut sqlx::PgConnection,
        id: &EventId,
    ) -> Result<(Object, T), PipelineError>
    where
        T: Serialize + DeserializeOwned,
    {
        let record = crate::outbox::db::sqlx::FindQuery::new(id)
            .execute(conn)
            .await?;
        let stage = serde_json::from_value::<T>(record.stage().to_owned())
            .error(ErrorKind::DeserializationFailed)?;

        Ok((record, stage))
    }

    async fn load_multiple_records<T>(
        conn: &mut sqlx::PgConnection,
        records_per_try: i64,
    ) -> Result<Vec<(Object, T)>, PipelineError>
    where
        T: Serialize + DeserializeOwned,
    {
        let records = crate::outbox::db::sqlx::ListQuery::new(records_per_try)
            .execute(conn)
            .await
            .error(ErrorKind::LoadStagesFailed)?;

        let mut result = Vec::with_capacity(records.len());
        for record in records {
            let stage = serde_json::from_value::<T>(record.stage().to_owned())
                .error(ErrorKind::DeserializationFailed)?;

            result.push((record, stage));
        }

        Ok(result)
    }

    async fn handle_record<C, T>(
        &self,
        conn: &mut sqlx::PgConnection,
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
        let event_id = record.id();

        let result = <T as StageHandle>::handle(&stage, &ctx, &event_id).await;

        match result {
            Ok(Some(next_stage)) => {
                let record = crate::outbox::db::sqlx::DeleteQuery::new(&event_id)
                    .execute(conn)
                    .await
                    .error(ErrorKind::DeleteStageFailed)?;

                let json =
                    serde_json::to_value(&next_stage).error(ErrorKind::SerializationFailed)?;

                let event_id = crate::outbox::db::sqlx::InsertQuery::new(
                    record.entity_type(),
                    json,
                    record.delivery_deadline_at(),
                    record.operation(),
                )
                .execute(conn)
                .await
                .error(ErrorKind::InsertStageFailed)?;

                Ok(Some(event_id))
            }
            Ok(None) => {
                crate::outbox::db::sqlx::DeleteQuery::new(&event_id)
                    .execute(conn)
                    .await
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

                crate::outbox::db::sqlx::UpdateQuery::new(
                    &event_id,
                    delivery_deadline_at,
                    error.kind(),
                )
                .execute(conn)
                .await
                .error(ErrorKind::UpdateStageFailed)?;

                Err(error.into())
            }
        }
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
            let mut conn = self
                .db
                .acquire()
                .await
                .error(ErrorKind::DbConnAcquisitionFailed)?;

            let this = self.clone();
            let ctx = ctx.clone();

            let result = conn
                .transaction(|conn| {
                    Box::pin(async move {
                        let (record, stage): (Object, T) =
                            Self::load_single_record(conn, &id).await?;

                        this.handle_record(conn, &ctx, record, stage).await
                    })
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
        let mut conn = self
            .db
            .acquire()
            .await
            .error(ErrorKind::DbConnAcquisitionFailed)?;
        let this = self.clone();

        conn.transaction::<_, _, PipelineErrors>(|conn| {
            Box::pin(async move {
                let mut errors = PipelineErrors::new();
                let records: Vec<(Object, T)> =
                    Self::load_multiple_records(conn, records_per_try).await?;

                if records.is_empty() {
                    // Exit from the closure
                    return Ok(MultipleStagePipelineResult::Done);
                }

                for (record, stage) in records {
                    // In case of error, we try to handle another record
                    if let Err(err) = this.handle_record(conn, &ctx, record, stage).await {
                        errors.add(err);
                    }
                }

                if !errors.is_empty() {
                    return Err(errors);
                }

                Ok(MultipleStagePipelineResult::Continue)
            })
        })
        .await
    }
}
