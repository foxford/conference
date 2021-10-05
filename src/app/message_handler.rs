use crate::{
    app::{
        context::{AppMessageContext, Context, GlobalContext, MessageContext},
        endpoint,
        error::{Error as AppError, ErrorKind as AppErrorKind},
        API_VERSION,
    },
    backend::{janus, janus::handle_event},
};
use anyhow::anyhow;
use anyhow::Context as AnyhowContext;
use chrono::{DateTime, Utc};
use futures::{stream, Stream, StreamExt};
use std::{future::Future, pin::Pin};
use svc_agent::{
    mqtt::{
        Agent, IncomingEvent, IncomingMessage, IncomingRequest, IncomingRequestProperties,
        IncomingResponse, IntoPublishableMessage, OutgoingResponse, ResponseStatus,
        ShortTermTimingProperties,
    },
    Addressable, Authenticable,
};
use svc_error::Error as SvcError;
use tracing::{error, field::display, info, warn, Span};
use tracing_attributes::instrument;
use uuid::Uuid;

pub type MessageStream =
    Box<dyn Stream<Item = Box<dyn IntoPublishableMessage + Send>> + Send + Unpin>;

pub struct MessageHandler<C: GlobalContext> {
    agent: Agent,
    global_context: C,
}

impl<C: GlobalContext + Sync> MessageHandler<C> {
    pub fn new(agent: Agent, global_context: C) -> Self {
        Self {
            agent,
            global_context,
        }
    }

    pub fn agent(&self) -> &Agent {
        &self.agent
    }

    pub fn global_context(&self) -> &C {
        &self.global_context
    }

    #[instrument(name = "trace_id", skip(self, message, topic), fields(request_id = %Uuid::new_v4()))]
    pub async fn handle(&self, message: Result<IncomingMessage<String>, String>, topic: &str) {
        let mut msg_context = AppMessageContext::new(&self.global_context, Utc::now());

        match message {
            Ok(ref msg) => {
                self.handle_message(&mut msg_context, msg, topic).await;
            }
            Err(err_msg) => {
                error!(%err_msg, "Error receiving a message");
                let sentry_error =
                    AppError::new(AppErrorKind::MessageReceivingFailed, anyhow!(err_msg));
                sentry_error.notify_sentry();
            }
        }
    }

    #[instrument(name = "trace_id", skip(self, message), fields(
        rtc_id,
        event_kind = ?message.event_kind(),
        room_id,
        rtc_stream_id,
        request_id = message.trace_id().map_or("", |x| x.as_str())))]
    pub async fn handle_events(&self, message: janus::client::IncomingEvent) {
        if let Some(opaque_id) = message.opaque_id() {
            Span::current()
                .record("room_id", &display(opaque_id.room_id))
                .record("rtc_stream_id", &display(opaque_id.stream_id));
        }
        info!("Janus notification received");
        let mut msg_context = AppMessageContext::new(&self.global_context, Utc::now());

        let messages = handle_event(&mut msg_context, message).await;

        self.publish_outgoing_messages(messages).await;
        info!("Janus notifications sent");
    }

    async fn handle_message(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        message: &IncomingMessage<String>,
        topic: &str,
    ) {
        match message {
            IncomingMessage::Request(req) => self.handle_request(msg_context, req, topic).await,
            IncomingMessage::Response(resp) => self.handle_response(msg_context, resp, topic).await,
            IncomingMessage::Event(event) => self.handle_event(msg_context, event, topic).await,
        }
    }

    #[instrument(skip(self, msg_context, request), fields(
        agent_id = %request.properties().as_agent_id(),
        account_id = %request.properties().as_account_id(),
        method = request.properties().method()
    ))]
    async fn handle_request(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        request: &IncomingRequest<String>,
        topic: &str,
    ) {
        info!("Request received");
        let outgoing_message_stream = endpoint::route_request(msg_context, request, topic)
            .await
            .unwrap_or_else(|| {
                error_response(
                    ResponseStatus::METHOD_NOT_ALLOWED,
                    "about:blank",
                    "Unknown method",
                    "Unknown method",
                    request.properties(),
                    msg_context.start_timestamp(),
                )
            });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await;
        info!("Response sent");
    }

    #[instrument(skip(self, msg_context, response), fields(
        agent_id = %response.properties().as_agent_id(),
        account_id = %response.properties().as_account_id(),
    ))]
    async fn handle_response(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        response: &IncomingResponse<String>,
        topic: &str,
    ) {
        info!("Response received");
        let raw_corr_data = response.properties().correlation_data();

        let outgoing_message_stream =
            endpoint::route_response(msg_context, response, raw_corr_data, topic).await;

        self.publish_outgoing_messages(outgoing_message_stream)
            .await;
        info!("Notification sent");
    }

    #[instrument(skip(self, msg_context, event), fields(
        agent_id = %event.properties().as_agent_id(),
        account_id = %event.properties().as_account_id(),
        label = ?event.properties().label()
    ))]
    async fn handle_event(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        event: &IncomingEvent<String>,
        topic: &str,
    ) {
        info!("Event received");
        let outgoing_message_stream = endpoint::route_event(msg_context, event, topic)
            .await
            .unwrap_or_else(|| {
                warn!("Unexpected event label");
                Box::new(stream::empty())
            });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await;
        info!("Notifications sent");
    }

    async fn publish_outgoing_messages(&self, mut message_stream: MessageStream) {
        let mut agent = self.agent.clone();

        while let Some(message) = message_stream.next().await {
            publish_message(&mut agent, message);
        }
    }
}

fn error_response(
    status: ResponseStatus,
    kind: &str,
    title: &str,
    detail: &str,
    reqp: RequestParams,
    start_timestamp: DateTime<Utc>,
) -> MessageStream {
    let err = SvcError::builder()
        .status(status)
        .kind(kind, title)
        .detail(detail)
        .build();

    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let props = reqp.to_response(status, timing);
    let resp = OutgoingResponse::unicast(err, props, reqp, API_VERSION);
    let boxed_resp = Box::new(resp) as Box<dyn IntoPublishableMessage + Send>;
    Box::new(stream::once(std::future::ready(boxed_resp)))
}

pub fn publish_message(agent: &mut Agent, message: Box<dyn IntoPublishableMessage>) {
    if let Err(err) = agent.publish_publishable(message) {
        error!(?err, "Failed to publish message");
        AppError::new(AppErrorKind::PublishFailed, err).notify_sentry();
    }
}

///////////////////////////////////////////////////////////////////////////////

// These auto-traits are being defined on all request/event handlers.
// They do parsing of the envelope and payload, call the handler and perform error handling.
// So we don't implement these generic things in each handler.
// We just need to specify the payload type and specific logic.

pub trait RequestEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        req: &'async_trait IncomingRequest<String>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// Can't use `#[async_trait]` macro here because it's not smart enough to add `'async_trait`
// lifetime to `H` type parameter. The creepy stuff around the actual implementation is what
// this macro expands to based on https://github.com/dtolnay/async-trait#explanation.
impl<'async_trait, H: 'async_trait + Sync + endpoint::RequestHandler>
    RequestEnvelopeHandler<'async_trait> for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        req: &'async_trait IncomingRequest<String>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>
    where
        Self: Sync + 'async_trait,
    {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::RequestHandler, C: Context>(
            context: &mut C,
            req: &IncomingRequest<String>,
        ) -> MessageStream {
            // Parse the envelope with the payload type specified in the handler.
            let payload = IncomingRequest::convert_payload::<H::Payload>(req);
            let reqp = req.properties();

            match payload {
                // Call handler.
                Ok(payload) => {
                    let app_result = H::handle(context, payload, reqp).await;
                    context.metrics().observe_app_result(&app_result);
                    app_result.unwrap_or_else(|err| {
                        error!(?err, "Failed to handle request");
                        err.notify_sentry();
                        error_response(
                            err.status(),
                            err.kind(),
                            err.title(),
                            &err.source().to_string(),
                            reqp,
                            context.start_timestamp(),
                        )
                    })
                }
                // Bad envelope or payload format => 400.
                Err(err) => error_response(
                    ResponseStatus::BAD_REQUEST,
                    reqp.method(),
                    H::ERROR_TITLE,
                    &err.to_string(),
                    reqp,
                    context.start_timestamp(),
                ),
            }
        }

        Box::pin(handle_envelope::<H, C>(context, req))
    }
}

pub trait ResponseEnvelopeHandler<'async_trait, CD> {
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        envelope: &'async_trait IncomingResponse<String>,
        corr_data: &'async_trait CD,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// This is the same as with the above.
impl<'async_trait, H: 'async_trait + endpoint::ResponseHandler>
    ResponseEnvelopeHandler<'async_trait, H::CorrelationData> for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        response: &'async_trait IncomingResponse<String>,
        corr_data: &'async_trait H::CorrelationData,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>> {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::ResponseHandler, C: Context>(
            context: &mut C,
            response: &IncomingResponse<String>,
            corr_data: &H::CorrelationData,
        ) -> MessageStream {
            // Parse response envelope with the payload from the handler.
            let payload = IncomingResponse::convert_payload::<H::Payload>(response);
            let respp = response.properties();

            match payload {
                // Call handler.
                Ok(payload) => H::handle(context, payload, respp, corr_data)
                    .await
                    .unwrap_or_else(|err| {
                        error!(?err, "Failed to handle response");

                        err.notify_sentry();
                        Box::new(stream::empty())
                    }),
                Err(err) => {
                    // Bad envelope or payload format.
                    error!(?err, "Failed to parse response");
                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, response, corr_data))
    }
}

pub trait EventEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        event: &'async_trait IncomingEvent<String>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// This is the same as with the above.
impl<'async_trait, H: 'async_trait + endpoint::EventHandler> EventEnvelopeHandler<'async_trait>
    for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        event: &'async_trait IncomingEvent<String>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>> {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::EventHandler, C: Context>(
            context: &mut C,
            event: &IncomingEvent<String>,
        ) -> MessageStream {
            let payload = IncomingEvent::convert_payload::<H::Payload>(event);
            let evp = event.properties();

            // Parse event envelope with the payload from the handler.
            match payload {
                // Call handler.
                Ok(payload) => H::handle(context, payload, evp)
                    .await
                    .unwrap_or_else(|err| {
                        error!(?err, "Failed to handle event");

                        err.notify_sentry();
                        Box::new(stream::empty())
                    }),
                Err(err) => {
                    error!(?err, "Failed to parse event");
                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, event))
    }
}

////////////////////////////////////////////////////////////////////////////////

impl endpoint::CorrelationData {
    pub fn dump(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).context("Failed to dump correlation data")
    }

    pub fn parse(raw_corr_data: &str) -> anyhow::Result<Self> {
        serde_json::from_str::<Self>(raw_corr_data).context("Failed to parse correlation data")
    }
}
