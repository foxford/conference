use anyhow::Result;

use futures::stream;

use svc_agent::mqtt::{
    IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties, ShortTermTimingProperties,
};
use tracing::error;

use crate::{
    app::{context::Context, endpoint, error::Error as AppError, message_handler::MessageStream},
    db::{agent_connection, janus_rtc_stream},
};

use self::client::{create_handle::OpaqueId, IncomingEvent};

////////////////////////////////////////////////////////////////////////////////

pub const JANUS_API_VERSION: &str = "v1";

pub async fn handle_event<C: Context>(context: &mut C, event: IncomingEvent) -> MessageStream {
    handle_event_impl(context, event)
        .await
        .unwrap_or_else(|err| {
            error!(?err, "Failed to handle an event from janus");
            err.notify_sentry();
            Box::new(stream::empty())
        })
}

async fn handle_event_impl<C: Context>(
    context: &mut C,
    payload: IncomingEvent,
) -> Result<MessageStream, AppError> {
    match payload {
        IncomingEvent::WebRtcUp(inev) => {
            // If the event relates to a publisher's handle,
            // we will find the corresponding stream and send event w/ updated stream object
            // to the room's topic.
            let conn = context.get_conn().await?;
            let start_timestamp = context.start_timestamp();
            crate::util::spawn_blocking(move || {
                if let Some(rtc_stream) = janus_rtc_stream::start(inev.opaque_id.stream_id, &conn)?
                {
                    let room = endpoint::helpers::find_room_by_rtc_id(
                        rtc_stream.rtc_id(),
                        endpoint::helpers::RoomTimeRequirement::Open,
                        &conn,
                    )?;

                    let event =
                        endpoint::rtc_stream::update_event(room.id(), rtc_stream, start_timestamp)?;

                    Ok(Box::new(stream::once(std::future::ready(
                        Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>
                    ))) as MessageStream)
                } else {
                    Ok(Box::new(stream::empty()) as MessageStream)
                }
            })
            .await
        }
        IncomingEvent::HangUp(inev) => handle_hangup_detach(context, inev.opaque_id).await,
        IncomingEvent::Detached(inev) => handle_hangup_detach(context, inev.opaque_id).await,
        IncomingEvent::Event(resp) => {
            let uri = format!("rooms/{}/events", resp.opaque_id.room_id);
            let timing = ShortTermTimingProperties::until_now(context.start_timestamp());
            let props = OutgoingEventProperties::new("rtc_stream.agent_speaking", timing);
            let event = OutgoingEvent::broadcast(resp.plugindata.data, props, &uri);

            Ok(Box::new(stream::once(std::future::ready(
                Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>
            ))) as MessageStream)
        }
    }
}

async fn handle_hangup_detach<C: Context>(
    context: &mut C,
    opaque_id: OpaqueId,
) -> Result<MessageStream, AppError> {
    // If the event relates to the publisher's handle,
    // we will find the corresponding stream and send an event w/ updated stream object
    // to the room's topic.
    let conn = context.get_conn().await?;
    let start_timestamp = context.start_timestamp();
    crate::util::spawn_blocking(move || {
        if let Some(rtc_stream) = janus_rtc_stream::stop(opaque_id.stream_id, &conn)? {
            // Publish the update event only if the stream object has been changed.
            // If there's no actual media stream, the object wouldn't contain its start time.
            if rtc_stream.time().is_some() {
                // Disconnect agents.
                agent_connection::BulkDisconnectByRtcQuery::new(rtc_stream.rtc_id())
                    .execute(&conn)?;

                // Send rtc_stream.update event.
                let event = endpoint::rtc_stream::update_event(
                    opaque_id.room_id,
                    rtc_stream,
                    start_timestamp,
                )?;

                let boxed_event =
                    Box::new(event) as Box<dyn IntoPublishableMessage + Send + Sync + 'static>;
                return Ok(Box::new(stream::once(std::future::ready(boxed_event))) as MessageStream);
            }
        }
        Ok::<_, AppError>(Box::new(stream::empty()) as MessageStream)
    })
    .await
}

////////////////////////////////////////////////////////////////////////////////
pub mod client;
pub mod client_pool;
pub mod metrics;
pub mod online_handler;
