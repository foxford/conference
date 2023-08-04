use crate::app::{
    context::GlobalContext,
    error::{Error, ErrorExt, ErrorKind},
};
use anyhow::{anyhow, Context};
use std::sync::Arc;
use svc_events::{EventId, EventV1 as Event};
use uuid::Uuid;

pub const SUBJECT_PREFIX: &str = "classroom";

pub async fn send_nats_notification(
    ctx: Arc<dyn GlobalContext + Send + Sync>,
    classroom_id: Uuid,
    event: Event,
    id: &EventId,
) -> Result<(), Error> {
    let event = svc_events::Event::from(event.clone());

    let payload = serde_json::to_vec(&event)
        .context("invalid payload")
        .error(ErrorKind::InvalidPayload)?;

    let subject = svc_nats_client::Subject::new(
        SUBJECT_PREFIX.to_string(),
        classroom_id,
        id.entity_type().to_string(),
    );

    let event = svc_nats_client::event::Builder::new(
        subject,
        payload,
        id.to_owned(),
        ctx.agent_id().to_owned(),
    )
    .build();

    ctx.nats_client()
        .ok_or_else(|| anyhow!("nats client not found"))
        .error(ErrorKind::NatsClientNotFound)?
        .publish(&event)
        .await
        .error(ErrorKind::NatsPublishFailed)?;

    Ok(())
}
