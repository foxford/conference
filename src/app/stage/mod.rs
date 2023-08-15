use crate::{
    app::{context::GlobalContext, error::Error, stage},
    db::{self, room::FindQueryable},
};
use anyhow::{anyhow, Context};
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use svc_events::{Event, EventV1, VideoGroupEventV1};
use svc_nats_client::{
    consumer::{FailureKind, FailureKindExt, HandleMessageFailure},
    Subject,
};

pub mod video_group;

pub async fn route_message(
    ctx: Arc<dyn GlobalContext + Sync + Send>,
    msg: Arc<svc_nats_client::Message>,
) -> Result<(), HandleMessageFailure<anyhow::Error>> {
    let subject = Subject::from_str(&msg.subject)
        .context("parse nats subject")
        .permanent()?;

    let event = serde_json::from_slice::<Event>(msg.payload.as_ref())
        .context("parse nats payload")
        .permanent()?;

    let classroom_id = subject.classroom_id();
    let room = {
        let mut conn = ctx
            .get_conn()
            .await
            .map_err(anyhow::Error::from)
            .transient()?;

        db::room::FindQuery::by_classroom_id(classroom_id)
            .execute(&mut conn)
            .await
            .context("find room by classroom_id")
            .transient()?
            .ok_or(anyhow!(
                "failed to get room by classroom_id: {}",
                classroom_id
            ))
            .permanent()?
    };

    tracing::info!(?event, class_id = %classroom_id);

    let headers = svc_nats_client::Headers::try_from(msg.headers.clone().unwrap_or_default())
        .context("parse nats headers")
        .permanent()?;
    let agent_id = headers.sender_id();
    let event_id = headers.event_id();

    let r: Result<(), HandleMessageFailure<Error>> = match event {
        Event::V1(
            ev @ EventV1::VideoGroupCreateIntent(_)
            | ev @ EventV1::VideoGroupDeleteIntent(_)
            | ev @ EventV1::VideoGroupUpdateIntent(_),
        ) => {
            let (backend_id, new_event) = match ev {
                EventV1::VideoGroupCreateIntent(e) => (
                    e.backend_id,
                    VideoGroupEventV1::Created {
                        created_at: e.created_at,
                    },
                ),
                EventV1::VideoGroupDeleteIntent(e) => (
                    e.backend_id,
                    VideoGroupEventV1::Deleted {
                        created_at: e.created_at,
                    },
                ),
                EventV1::VideoGroupUpdateIntent(e) => (
                    e.backend_id,
                    VideoGroupEventV1::Updated {
                        created_at: e.created_at,
                    },
                ),
                _ => unreachable!(),
            };
            // handle_video_group_intent_event(
            stage::video_group::handle_intent(
                ctx.clone(),
                event_id,
                room,
                agent_id.clone(),
                classroom_id,
                backend_id,
                new_event,
            )
            .await
        }
        _ => {
            // ignore
            Ok(())
        }
    };

    FailureKindExt::map_err(r, |e| anyhow!(e))
}
