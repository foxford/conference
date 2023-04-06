use crate::{
    app::{context::GlobalContext, error::Error as AppError, stage::AppStage},
    outbox::{
        error::ErrorKind,
        pipeline::{diesel::Pipeline as DieselPipeline, Pipeline},
    },
};
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle, time::MissedTickBehavior};
use tracing::{error, info};

pub fn run(
    ctx: Arc<dyn GlobalContext>,
    mut shutdown_rx: watch::Receiver<()>,
) -> anyhow::Result<JoinHandle<()>> {
    info!("Outbox handler started");

    let outbox_config = ctx.config().outbox;
    let try_wake_interval = outbox_config.try_wake_interval.to_std()?;

    let task = tokio::spawn(async move {
        let mut check_interval = tokio::time::interval(try_wake_interval);
        check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        'outer: loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    let pipeline = DieselPipeline::new(
                        ctx.db().clone(),
                        outbox_config.try_wake_interval,
                        outbox_config.max_delivery_interval,
                    );

                    'inner: loop {
                        let result = pipeline
                            .run_multiple_stages::<AppStage, _>(ctx.clone(), outbox_config.messages_per_try)
                            .await;

                        match result {
                            Ok(maybe_errors) => match maybe_errors {
                                None => {
                                    break 'inner;
                                }
                                Some(errors) => {
                                    for err in errors {
                                        if let ErrorKind::StageError(kind) = &err.kind {
                                            ctx.metrics().observe_outbox_error(kind);
                                        }

                                        error!(%err, "failed to complete stage");
                                        AppError::from(err).notify_sentry();
                                    }

                                    continue 'inner;
                                }
                            },
                            Err(err) => {
                                error!(%err);
                                AppError::from(err).notify_sentry();

                                break 'inner;
                            }
                        }
                    }
                }
                // Graceful shutdown
                _ = shutdown_rx.changed() => {
                    break 'outer;
                }
            }
        }
    });

    Ok(task)
}
