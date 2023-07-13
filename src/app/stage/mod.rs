use crate::{
    app::{
        context::GlobalContext,
        error::Error,
        stage::video_group::{
            VideoGroupSendMqttNotification, VideoGroupSendNatsNotification,
            VideoGroupUpdateJanusConfig,
        },
    },
    backend::janus::client::update_agent_reader_config::{
        UpdateReaderConfigRequest, UpdateReaderConfigRequestBody,
        UpdateReaderConfigRequestBodyConfigItem,
    },
    db::{self, room::FindQueryable},
    outbox::{error::StageError, StageHandle},
};
use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use svc_authn::Authenticable;
use svc_events::{
    ban::{BanAcceptedV1, BanVideoStreamingCompletedV1},
    Event, EventId, EventV1,
};
use svc_nats_client::{
    consumer::{FailureKind, FailureKindExt, HandleMessageFailure},
    Subject,
};

use super::error::{ErrorExt, ErrorKind};

pub mod video_group;

#[allow(clippy::enum_variant_names)]
#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "name")]
pub enum AppStage {
    VideoGroupUpdateJanusConfig(VideoGroupUpdateJanusConfig),
    VideoGroupSendNatsNotification(VideoGroupSendNatsNotification),
    VideoGroupSendMqttNotification(VideoGroupSendMqttNotification),
}

#[async_trait::async_trait]
impl StageHandle for AppStage {
    type Context = Arc<dyn GlobalContext + Send + Sync>;
    type Stage = AppStage;

    async fn handle(&self, ctx: &Self::Context, id: &EventId) -> Result<Option<Self>, StageError> {
        match self {
            AppStage::VideoGroupUpdateJanusConfig(s) => s.handle(ctx, id).await,
            AppStage::VideoGroupSendNatsNotification(s) => s.handle(ctx, id).await,
            AppStage::VideoGroupSendMqttNotification(s) => s.handle(ctx, id).await,
        }
    }
}

impl From<Error> for StageError {
    fn from(error: Error) -> Self {
        StageError::new(error.error_kind().kind().into(), Box::new(error))
    }
}

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
    let _agent_id = headers.sender_id();

    let r = match event {
        Event::V1(EventV1::BanAccepted(e)) => {
            handle_ban_accepted(ctx.as_ref(), e, &room, subject, &headers).await
        }
        _ => {
            // ignore
            Ok(())
        }
    };

    FailureKindExt::map_err(r, |e| anyhow!(e))
}

async fn handle_ban_accepted(
    ctx: &(dyn GlobalContext + Sync),
    e: BanAcceptedV1,
    room: &db::room::Object,
    subject: Subject,
    headers: &svc_nats_client::Headers,
) -> Result<(), HandleMessageFailure<Error>> {
    let room_backend = room
        .backend_id()
        .ok_or_else(|| anyhow!("room has no backend"))
        .error(ErrorKind::BackendNotFound)
        .transient()?;

    let mut conn = ctx.get_conn().await.transient()?;

    let rtcs = db::rtc::ListQuery::new()
        .room_id(room.id)
        .execute(&mut conn)
        .await
        .error(ErrorKind::DbQueryFailed)
        .transient()?;

    let target_rtc = rtcs
        .iter()
        .find(|r| *r.created_by().as_account_id() == e.target_account)
        .ok_or_else(|| anyhow!("agent is not in the room"))
        .error(ErrorKind::AgentNotConnected)
        .transient()?;

    let (mut rtc_ids, mut agent_ids, mut receive_video_values, mut receive_audio_values) =
        (vec![], vec![], vec![], vec![]);

    let mut configs = vec![];

    for rtc in rtcs.iter() {
        let receive_video = room
            .host()
            // always receive video for hosts
            .map(|h| h == rtc.created_by() || !e.ban)
            .unwrap_or(!e.ban);

        let receive_audio = !e.ban;
        let stream_id = target_rtc.id;
        let reader_id = rtc.created_by();

        let cfg = UpdateReaderConfigRequestBodyConfigItem {
            reader_id: reader_id.clone(),
            stream_id,
            receive_video,
            receive_audio,
        };
        configs.push(cfg);

        rtc_ids.push(stream_id);
        agent_ids.push(reader_id);
        receive_video_values.push(receive_video);
        receive_audio_values.push(receive_audio);
    }

    let mut tx = conn.begin().await.map_err(Error::from).transient()?;

    if e.ban {
        db::ban_account::InsertQuery::new(e.classroom_id, &e.target_account)
            .execute(&mut tx)
            .await
            .error(ErrorKind::DbQueryFailed)
            .transient()?;
    } else {
        db::ban_account::DeleteQuery::new(e.classroom_id, &e.target_account)
            .execute(&mut tx)
            .await
            .error(ErrorKind::DbQueryFailed)
            .transient()?;
    }

    db::rtc_reader_config::batch_insert(
        &mut tx,
        &rtc_ids,
        &agent_ids,
        &receive_video_values,
        &receive_audio_values,
    )
    .await
    .error(ErrorKind::DbQueryFailed)
    .transient()?;

    tx.commit().await.map_err(Error::from).transient()?;

    let janus_backend = db::janus_backend::FindQuery::new(room_backend)
        .execute(&mut conn)
        .await
        .error(ErrorKind::DbQueryFailed)
        .transient()?
        .ok_or_else(|| anyhow!("Janus backend not found"))
        .error(ErrorKind::BackendNotFound)
        // backend could get offline and will be available next time
        .transient()?;

    let request = UpdateReaderConfigRequest {
        session_id: janus_backend.session_id(),
        handle_id: janus_backend.handle_id(),
        body: UpdateReaderConfigRequestBody::new(configs),
    };

    ctx.janus_clients()
        .get_or_insert(&janus_backend)
        .error(ErrorKind::BackendClientCreationFailed)
        .transient()?
        .reader_update(request)
        .await
        .context("Reader update")
        .error(ErrorKind::BackendRequestFailed)
        .transient()?;

    let event_id = headers.event_id();
    let event = BanVideoStreamingCompletedV1::new_from_accepted(e, event_id.clone());
    let event = Event::from(event);

    let payload = serde_json::to_vec(&event)
        .error(ErrorKind::InvalidPayload)
        .permanent()?;

    let event_id = EventId::from((
        event_id.entity_type().to_owned(),
        "video_streaming_completed".to_owned(),
        event_id.sequence_id(),
    ));

    let event = svc_nats_client::event::Builder::new(
        subject,
        payload,
        event_id.to_owned(),
        ctx.agent_id().to_owned(),
    )
    .build();

    ctx.nats_client()
        .ok_or_else(|| anyhow!("nats client not found"))
        .error(ErrorKind::NatsClientNotFound)
        .transient()?
        .publish(&event)
        .await
        .error(ErrorKind::NatsPublishFailed)
        .transient()?;

    Ok(())
}
