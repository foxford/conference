use std::future::Future;
use std::pin::Pin;

use async_std::prelude::*;
use async_std::stream::{self, Stream};
use chrono::{DateTime, Utc};
use futures_util::pin_mut;
use svc_agent::mqtt::{
    Agent, IncomingEvent, IncomingMessage, IncomingRequest, IncomingRequestProperties,
    IncomingResponse, IntoPublishableMessage, OutgoingResponse, ResponseStatus,
    ShortTermTimingProperties,
};
use svc_error::{extension::sentry, Error as SvcError};

use crate::app::context::{AppMessageContext, Context, GlobalContext, MessageContext};
use crate::app::{endpoint, API_VERSION};

pub(crate) type MessageStream =
    Box<dyn Stream<Item = Box<dyn IntoPublishableMessage + Send>> + Send + Unpin>;

pub(crate) struct MessageHandler<C: GlobalContext> {
    agent: Agent,
    global_context: C,
}

impl<C: GlobalContext + Sync> MessageHandler<C> {
    pub(crate) fn new(agent: Agent, global_context: C) -> Self {
        Self {
            agent,
            global_context,
        }
    }

    pub(crate) fn agent(&self) -> &Agent {
        &self.agent
    }

    pub(crate) fn global_context(&self) -> &C {
        &self.global_context
    }

    pub(crate) async fn handle(
        &self,
        message: &Result<IncomingMessage<String>, String>,
        topic: &str,
    ) {
        let mut msg_context = AppMessageContext::new(&self.global_context, Utc::now());

        match message {
            Ok(ref msg) => {
                if let Err(err) = self.handle_message(&mut msg_context, msg, topic).await {
                    Self::report_error(&mut msg_context, message, &err.to_string()).await;
                }
            }
            Err(e) => {
                Self::report_error(&mut msg_context, message, e).await;
            }
        }
    }

    async fn report_error(
        msg_context: &mut AppMessageContext<'_, C>,
        message: &Result<IncomingMessage<String>, String>,
        err: &str,
    ) {
        error!(
            msg_context.logger(),
            "Error processing a message = '{:?}': {}", message, err
        );

        let svc_error = SvcError::builder()
            .status(ResponseStatus::UNPROCESSABLE_ENTITY)
            .kind("message_handler", "Message handling error")
            .detail(&err.to_string())
            .build();

        sentry::send(svc_error).unwrap_or_else(|err| {
            warn!(
                msg_context.logger(),
                "Error sending error to Sentry: {}", err
            )
        });
    }

    async fn handle_message(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        message: &IncomingMessage<String>,
        topic: &str,
    ) -> Result<(), SvcError> {
        match message {
            IncomingMessage::Request(req) => self.handle_request(msg_context, req, topic).await,
            IncomingMessage::Response(resp) => self.handle_response(msg_context, resp, topic).await,
            IncomingMessage::Event(event) => self.handle_event(msg_context, event, topic).await,
        }
    }

    async fn handle_request(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        req: &IncomingRequest<String>,
        topic: &str,
    ) -> Result<(), SvcError> {
        let outgoing_message_stream = endpoint::route_request(msg_context, req, topic)
            .await
            .unwrap_or_else(|| {
                error_response(
                    ResponseStatus::METHOD_NOT_ALLOWED,
                    "about:blank",
                    "Unknown method",
                    &format!("Unknown method '{}'", req.properties().method()),
                    req.properties(),
                    msg_context.start_timestamp(),
                )
            });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn handle_response(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        envelope: &IncomingResponse<String>,
        topic: &str,
    ) -> Result<(), SvcError> {
        let outgoing_message_stream = endpoint::route_response(msg_context, envelope, topic)
            .await
            .unwrap_or_else(|| {
                warn!(msg_context.logger(), "Unhandled response");
                Box::new(stream::empty())
            });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn handle_event(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        event: &IncomingEvent<String>,
        topic: &str,
    ) -> Result<(), SvcError> {
        let outgoing_message_stream = endpoint::route_event(msg_context, event, topic)
            .await
            .unwrap_or_else(|| {
                let label = event.properties().label().unwrap_or("none");
                warn!(msg_context.logger(), "Unexpected event label"; "label" => label);
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
                    H::handle(context, payload, reqp)
                        .await
                        .unwrap_or_else(|mut svc_error| {
                            if svc_error.kind() == "about:blank" {
                                svc_error.set_kind(reqp.method(), H::ERROR_TITLE);
                            }

                            sentry::send(svc_error.clone()).unwrap_or_else(|err| {
                                warn!(context.logger(), "Error sending error to Sentry: {}", err)
                            });

                            // Handler returned an error => 422.
                            error_response(
                                svc_error.status_code(),
                                svc_error.kind(),
                                svc_error.title(),
                                &svc_error.to_string(),
                                &reqp,
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

pub(crate) trait ResponseEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        resp: &'async_trait IncomingResponse<String>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// This is the same as with the above.
impl<'async_trait, H: 'async_trait + endpoint::ResponseHandler>
    ResponseEnvelopeHandler<'async_trait> for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait mut C,
        resp: &'async_trait IncomingResponse<String>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>> {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::ResponseHandler, C: Context>(
            context: &mut C,
            resp: &IncomingResponse<String>,
        ) -> MessageStream {
            // Parse response envelope with the payload from the handler.
            let payload = IncomingResponse::convert_payload::<H::Payload>(resp);
            let resp = resp.properties();
            match payload {
                // Call handler.
                Ok(payload) => {
                    H::handle(context, payload, resp)
                        .await
                        .unwrap_or_else(|mut svc_error| {
                            // Handler returned an error.
                            if svc_error.kind() == "about:blank" {
                                svc_error.set_kind("response", "Failed to handle response");
                            }

                            error!(
                                context.logger(),
                                "Failed to handle response: {}",
                                svc_error.detail().unwrap_or("no detail");
                                "kind" => svc_error.kind(),
                            );

                            sentry::send(svc_error).unwrap_or_else(|err| {
                                warn!(context.logger(), "Error sending error to Sentry: {}", err)
                            });

                            Box::new(stream::empty())
                        })
                }
                Err(err) => {
                    // Bad envelope or payload format.
                    error!(context.logger(), "Failed to parse response: {}", err);
                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, resp))
    }
}

pub(crate) trait EventEnvelopeHandler<'async_trait> {
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
                Ok(payload) => {
                    H::handle(context, payload, evp)
                        .await
                        .unwrap_or_else(|mut svc_error| {
                            // Handler returned an error.
                            if let Some(label) = evp.label() {
                                if svc_error.kind() == "about:blank" {
                                    let error_title = format!("Failed to handle event '{}'", label);
                                    svc_error.set_kind(label, &error_title);
                                }

                                error!(
                                    context.logger(),
                                    "Failed to handle event: {}",
                                    svc_error.detail().unwrap_or("no detail");
                                    "label" => label,
                                    "kind" => svc_error.kind(),
                                );

                                sentry::send(svc_error).unwrap_or_else(|err| {
                                    warn!(
                                        context.logger(),
                                        "Error sending error to Sentry: {}", err
                                    )
                                });
                            }

                            Box::new(stream::empty())
                        })
                }
                Err(err) => {
                    // Bad envelope or payload format.
                    if let Some(label) = evp.label() {
                        error!(
                            context.logger(),
                            "Failed to parse event: {}", err;
                            "label" => label,
                        );
                    }

                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, event))
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
