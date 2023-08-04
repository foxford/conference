use anyhow::anyhow;
use std::sync::Arc;
use svc_agent::AgentId;
use uuid::Uuid;

use svc_events::{Event, EventId};

use crate::app::{
    context::GlobalContext,
    error::{Error, ErrorExt, ErrorKind as AppErrorKind},
};

const SUBJECT_PREFIX: &str = "classroom";

#[derive(Default)]
pub struct Options {
    receiver_id: Option<AgentId>,
}

impl Options {
    #[allow(dead_code)]
    pub fn receiver_id(self, r_id: AgentId) -> Self {
        Self {
            receiver_id: Some(r_id),
        }
    }
}

pub async fn publish_event(
    ctx: Arc<dyn GlobalContext + Send + Sync>,
    classroom_id: Uuid,
    id: &EventId,
    event: Event,
    opts: Options,
) -> Result<(), Error> {
    let subject = svc_nats_client::Subject::new(
        SUBJECT_PREFIX.to_string(),
        classroom_id,
        id.entity_type().to_string(),
    );

    let payload = serde_json::to_vec(&event).error(AppErrorKind::InvalidPayload)?;

    let event_b = svc_nats_client::event::Builder::new(
        subject,
        payload,
        id.to_owned(),
        ctx.agent_id().to_owned(),
    );

    let event_b = match opts.receiver_id {
        Some(r_id) => event_b.receiver_id(r_id),
        None => event_b,
    };

    let event = event_b.build();

    ctx.nats_client()
        .ok_or_else(|| anyhow!("nats client not found"))
        .error(AppErrorKind::NatsClientNotFound)?
        .publish(&event)
        .await
        .error(AppErrorKind::NatsPublishFailed)
}
