use std::future::Future;
use std::pin::Pin;

use async_std::prelude::*;
use async_std::stream::{self, Stream};
use chrono::{DateTime, Utc};
use futures_util::pin_mut;
use log::{error, warn};
use svc_agent::mqtt::{
    Agent, IncomingEvent, IncomingMessage, IncomingRequest, IncomingRequestProperties,
    IncomingResponse, IntoPublishableMessage, OutgoingResponse, ResponseStatus,
    ShortTermTimingProperties,
};
use svc_error::{extension::sentry, Error as SvcError};

use crate::app::{context::Context, endpoint, API_VERSION};

pub(crate) type MessageStream =
    Box<dyn Stream<Item = Box<dyn IntoPublishableMessage + Send>> + Send + Unpin>;

pub(crate) struct MessageHandler<C: Context> {
    agent: Agent,
    context: C,
}

impl<C: Context + Sync> MessageHandler<C> {
    pub(crate) fn new(agent: Agent, context: C) -> Self {
        Self { agent, context }
    }

    pub(crate) fn agent(&self) -> &Agent {
        &self.agent
    }

    pub(crate) fn context(&self) -> &C {
        &self.context
    }

    pub(crate) async fn handle(
        &self,
        message: &Result<IncomingMessage<String>, String>,
        topic: &str,
    ) {
        match message {
            Ok(ref msg) => {
                if let Err(err) = self.handle_message(msg, topic).await {
                    self.report_error(message, &err.to_string()).await;
                }
            }
            Err(e) => {
                self.report_error(message, e).await;
            }
        }
    }

    async fn report_error(&self, message: &Result<IncomingMessage<String>, String>, err: &str) {
        error!("Error processing a message = '{:?}': {}", message, err);

        let svc_error = SvcError::builder()
            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
            .kind("message_handler", "Message handling error")
            .detail(&err.to_string())
            .build();

        sentry::send(svc_error)
            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
    }

    async fn handle_message(
        &self,
        message: &IncomingMessage<String>,
        topic: &str,
    ) -> Result<(), SvcError> {
        let start_timestamp = Utc::now();

        match message {
            IncomingMessage::Request(req) => self.handle_request(req, topic, start_timestamp).await,
            IncomingMessage::Response(resp) => {
                self.handle_response(resp, topic, start_timestamp).await
            }
            IncomingMessage::Event(event) => self.handle_event(event, topic, start_timestamp).await,
        }
    }

    async fn handle_request(
        &self,
        req: &IncomingRequest<String>,
        topic: &str,
        start_timestamp: DateTime<Utc>,
    ) -> Result<(), SvcError> {
        let outgoing_message_stream =
            endpoint::route_request(&self.context, req, topic, start_timestamp)
                .await
                .unwrap_or_else(|| {
                    error_response(
                        ResponseStatus::METHOD_NOT_ALLOWED,
                        "about:blank",
                        "Unknown method",
                        &format!("Unknown method '{}'", req.properties().method()),
                        req.properties(),
                        start_timestamp,
                    )
                });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn handle_response(
        &self,
        envelope: &IncomingResponse<String>,
        topic: &str,
        start_timestamp: DateTime<Utc>,
    ) -> Result<(), SvcError> {
        let outgoing_message_stream =
            endpoint::route_response(&self.context, envelope, topic, start_timestamp)
                .await
                .unwrap_or_else(|| {
                    warn!("Unhandled response");
                    Box::new(stream::empty())
                });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn handle_event(
        &self,
        event: &IncomingEvent<String>,
        topic: &str,
        start_timestamp: DateTime<Utc>,
    ) -> Result<(), SvcError> {
        let outgoing_message_stream =
            endpoint::route_event(&self.context, event, topic, start_timestamp)
                .await
                .unwrap_or_else(|| {
                    let label = event.properties().label().unwrap_or("none");
                    warn!("Unexpected event with label = '{}'", label);
                    Box::new(stream::empty())
                });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn publish_outgoing_messages(
        &self,
        message_stream: MessageStream,
    ) -> Result<(), SvcError> {
        let mut agent = self.agent.clone();
        pin_mut!(message_stream);

        while let Some(message) = message_stream.next().await {
            publish_message(&mut agent, message)?;
        }

        Ok(())
    }
}

fn error_response(
    status: ResponseStatus,
    kind: &str,
    title: &str,
    detail: &str,
    reqp: &IncomingRequestProperties,
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
    Box::new(stream::once(boxed_resp))
}

pub(crate) fn publish_message(
    agent: &mut Agent,
    message: Box<dyn IntoPublishableMessage>,
) -> Result<(), SvcError> {
    agent
        .publish_publishable(message)
        .map_err(|err| format!("Failed to publish message: {}", err))
        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
}

///////////////////////////////////////////////////////////////////////////////

// These auto-traits are being defined on all request/event handlers.
// They do parsing of the envelope and payload, call the handler and perform error handling.
// So we don't implement these generic things in each handler.
// We just need to specify the payload type and specific logic.

pub(crate) trait RequestEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        req: &'async_trait IncomingRequest<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// Can't use `#[async_trait]` macro here because it's not smart enough to add `'async_trait`
// lifetime to `H` type parameter. The creepy stuff around the actual implementation is what
// this macro expands to based on https://github.com/dtolnay/async-trait#explanation.
impl<'async_trait, H: 'async_trait + Sync + endpoint::RequestHandler>
    RequestEnvelopeHandler<'async_trait> for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        req: &'async_trait IncomingRequest<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>
    where
        Self: Sync + 'async_trait,
    {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::RequestHandler, C: Context>(
            context: &C,
            req: &IncomingRequest<String>,
            start_timestamp: DateTime<Utc>,
        ) -> MessageStream {
            // Parse the envelope with the payload type specified in the handler.
            let payload = IncomingRequest::convert_payload::<H::Payload>(req);
            let reqp = req.properties();
            match payload {
                // Call handler.
                Ok(payload) => H::handle(context, payload, reqp, start_timestamp)
                    .await
                    .unwrap_or_else(|mut svc_error| {
                        if svc_error.kind() == "about:blank" {
                            svc_error.set_kind(reqp.method(), H::ERROR_TITLE);
                        }

                        sentry::send(svc_error.clone())
                            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));

                        // Handler returned an error => 422.
                        error_response(
                            svc_error.status_code(),
                            svc_error.kind(),
                            svc_error.title(),
                            &svc_error.to_string(),
                            &reqp,
                            start_timestamp,
                        )
                    }),
                // Bad envelope or payload format => 400.
                Err(err) => error_response(
                    ResponseStatus::BAD_REQUEST,
                    reqp.method(),
                    H::ERROR_TITLE,
                    &err.to_string(),
                    reqp,
                    start_timestamp,
                ),
            }
        }

        Box::pin(handle_envelope::<H, C>(context, req, start_timestamp))
    }
}

pub(crate) trait ResponseEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        resp: &'async_trait IncomingResponse<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// This is the same as with the above.
impl<'async_trait, H: 'async_trait + endpoint::ResponseHandler>
    ResponseEnvelopeHandler<'async_trait> for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        resp: &'async_trait IncomingResponse<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>> {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::ResponseHandler, C: Context>(
            context: &C,
            resp: &IncomingResponse<String>,
            start_timestamp: DateTime<Utc>,
        ) -> MessageStream {
            // Parse response envelope with the payload from the handler.
            let payload = IncomingResponse::convert_payload::<H::Payload>(resp);
            let resp = resp.properties();
            match payload {
                // Call handler.
                Ok(payload) => H::handle(context, payload, resp, start_timestamp)
                    .await
                    .unwrap_or_else(|mut svc_error| {
                        // Handler returned an error.
                        if svc_error.kind() == "about:blank" {
                            svc_error.set_kind("response", "Failed to handle response");
                        }

                        error!("Failed to handle response: {}", svc_error);

                        sentry::send(svc_error)
                            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));

                        Box::new(stream::empty())
                    }),
                Err(err) => {
                    // Bad envelope or payload format.
                    error!("Failed to parse response: {}", err);
                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, resp, start_timestamp))
    }
}

pub(crate) trait EventEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        event: &'async_trait IncomingEvent<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// This is the same as with the above.
impl<'async_trait, H: 'async_trait + endpoint::EventHandler> EventEnvelopeHandler<'async_trait>
    for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        event: &'async_trait IncomingEvent<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>> {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::EventHandler, C: Context>(
            context: &C,
            event: &IncomingEvent<String>,
            start_timestamp: DateTime<Utc>,
        ) -> MessageStream {
            let payload = IncomingEvent::convert_payload::<H::Payload>(event);
            let evp = event.properties();
            // Parse event envelope with the payload from the handler.
            match payload {
                // Call handler.
                Ok(payload) => H::handle(context, payload, evp, start_timestamp)
                    .await
                    .unwrap_or_else(|mut svc_error| {
                        // Handler returned an error.
                        if let Some(label) = evp.label() {
                            if svc_error.kind() == "about:blank" {
                                let error_title = format!("Failed to handle event '{}'", label);
                                svc_error.set_kind(label, &error_title);
                            }

                            error!(
                                "Failed to handle event with label = '{}': {}",
                                label, svc_error
                            );

                            sentry::send(svc_error).unwrap_or_else(|err| {
                                warn!("Error sending error to Sentry: {}", err)
                            });
                        }

                        Box::new(stream::empty())
                    }),
                Err(err) => {
                    // Bad envelope or payload format.
                    if let Some(label) = evp.label() {
                        error!("Failed to parse event with label = '{}': {}", label, err);
                    }

                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, event, start_timestamp))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) trait SvcErrorSugar<T> {
    fn status(self, status: ResponseStatus) -> Result<T, SvcError>;
}

impl<T, E: AsRef<str>> SvcErrorSugar<T> for Result<T, E> {
    fn status(self, status: ResponseStatus) -> Result<T, SvcError> {
        self.map_err(|err| {
            SvcError::builder()
                .status(status)
                .detail(err.as_ref())
                .build()
        })
    }
}
