use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use async_std::prelude::*;
use async_std::stream::{self, Stream};
use chrono::{DateTime, Utc};
use futures_util::pin_mut;
use svc_agent::{
    mqtt::{
        Agent, IncomingEvent, IncomingMessage, IncomingRequest, IncomingRequestProperties,
        IncomingResponse, IntoPublishableMessage, OutgoingResponse, ShortTermTimingProperties,
    },
    Addressable, Authenticable,
};

use crate::app::context::{AppMessageContext, Context, GlobalContext, MessageContext};
use crate::app::error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind};
use crate::app::{endpoint, API_VERSION};

pub(crate) type MessageStream =
    Box<dyn Stream<Item = Box<dyn IntoPublishableMessage + Send>> + Send + Unpin>;

type TimingChannel = crossbeam_channel::Sender<(std::time::Duration, String)>;

pub(crate) struct MessageHandler<C: GlobalContext> {
    agent: Agent,
    global_context: C,
    tx: TimingChannel,
}

impl<C: GlobalContext + Sync> MessageHandler<C> {
    pub(crate) fn new(agent: Agent, global_context: C, tx: TimingChannel) -> Self {
        Self {
            agent,
            global_context,
            tx,
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
            "Error processing a message: {:?}: {}", message, err
        );

        let app_error = AppError::new(
            AppErrorKind::MessageHandlingFailed,
            anyhow!(err.to_string()),
        );

        app_error.notify_sentry(msg_context.logger());
    }

    async fn handle_message(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        message: &IncomingMessage<String>,
        topic: &str,
    ) -> Result<(), AppError> {
        let mut timer = if msg_context.dynamic_stats().is_some() {
            Some(MessageHandlerTiming::new(self.tx.clone()))
        } else {
            None
        };

        match message {
            IncomingMessage::Request(req) => {
                if let Some(ref mut timer) = timer {
                    timer.set_method(req.properties().method().into());
                }
                self.handle_request(msg_context, req, topic).await
            }
            IncomingMessage::Response(resp) => {
                if let Some(ref mut timer) = timer {
                    timer.set_method("response".into());
                }

                self.handle_response(msg_context, resp, topic).await
            }
            IncomingMessage::Event(event) => {
                if let Some(ref mut timer) = timer {
                    let label = match event.properties().label() {
                        Some(label) => format!("event-{}", label),
                        None => "event-none".into(),
                    };

                    timer.set_method(label);
                }

                self.handle_event(msg_context, event, topic).await
            }
        }
    }

    async fn handle_request(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        request: &IncomingRequest<String>,
        topic: &str,
    ) -> Result<(), AppError> {
        let agent_id = request.properties().as_agent_id();

        msg_context.add_logger_tags(o!(
            "agent_label" => agent_id.label().to_owned(),
            "account_id" => agent_id.as_account_id().label().to_owned(),
            "audience" => agent_id.as_account_id().audience().to_owned(),
            "method" => request.properties().method().to_owned()
        ));

        let outgoing_message_stream = endpoint::route_request(msg_context, request, topic)
            .await
            .unwrap_or_else(|| {
                let err = anyhow!("Unknown method '{}'", request.properties().method());
                let app_error = AppError::new(AppErrorKind::UnknownMethod, err);

                error_response(
                    app_error,
                    request.properties(),
                    msg_context.start_timestamp(),
                )
            });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn handle_response(
        &self,
        msg_context: &mut AppMessageContext<'_, C>,
        response: &IncomingResponse<String>,
        topic: &str,
    ) -> Result<(), AppError> {
        let agent_id = response.properties().as_agent_id();

        msg_context.add_logger_tags(o!(
            "agent_label" => agent_id.label().to_owned(),
            "account_id" => agent_id.as_account_id().label().to_owned(),
            "audience" => agent_id.as_account_id().audience().to_owned(),
        ));

        let outgoing_message_stream = endpoint::route_response(msg_context, response, topic)
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
    ) -> Result<(), AppError> {
        let agent_id = event.properties().as_agent_id();

        msg_context.add_logger_tags(o!(
            "agent_label" => agent_id.label().to_owned(),
            "account_id" => agent_id.as_account_id().label().to_owned(),
            "audience" => agent_id.as_account_id().audience().to_owned(),
        ));

        if let Some(label) = event.properties().label() {
            msg_context.add_logger_tags(o!("label" => label.to_owned()));
        }

        let outgoing_message_stream = endpoint::route_event(msg_context, event, topic)
            .await
            .unwrap_or_else(|| {
                warn!(msg_context.logger(), "Unexpected event label");
                Box::new(stream::empty())
            });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn publish_outgoing_messages(
        &self,
        message_stream: MessageStream,
    ) -> Result<(), AppError> {
        let mut agent = self.agent.clone();
        pin_mut!(message_stream);

        while let Some(message) = message_stream.next().await {
            publish_message(&mut agent, message)?;
        }

        Ok(())
    }
}

fn error_response(
    err: AppError,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
) -> MessageStream {
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let props = reqp.to_response(err.status(), timing);
    let e = err.to_svc_error();
    let resp = OutgoingResponse::unicast(e, props, reqp, API_VERSION);

    Box::new(stream::once(
        Box::new(resp) as Box<dyn IntoPublishableMessage + Send>
    ))
}

pub(crate) fn publish_message(
    agent: &mut Agent,
    message: Box<dyn IntoPublishableMessage>,
) -> Result<(), AppError> {
    agent
        .publish_publishable(message)
        .map_err(|err| anyhow!("Failed to publish message: {}", err))
        .error(AppErrorKind::PublishFailed)
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
                        .unwrap_or_else(|app_error| {
                            context.add_logger_tags(o!(
                                "status" => app_error.status().as_u16(),
                                "kind" => app_error.kind().to_owned(),
                            ));

                            error!(
                                context.logger(),
                                "Failed to handle request: {}",
                                app_error.source(),
                            );

                            app_error.notify_sentry(context.logger());

                            // Handler returned an error.
                            error_response(app_error, reqp, context.start_timestamp())
                        })
                }
                // Bad envelope or payload format => 400.
                Err(err) => {
                    let app_error = AppError::new(AppErrorKind::InvalidPayload, anyhow!("{}", err));
                    error_response(app_error, reqp, context.start_timestamp())
                }
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
                        .unwrap_or_else(|app_error| {
                            // Handler returned an error.
                            context.add_logger_tags(o!(
                                "status" => app_error.status().as_u16(),
                                "kind" => app_error.kind().to_owned(),
                            ));

                            error!(
                                context.logger(),
                                "Failed to handle response: {}",
                                app_error.source(),
                            );

                            app_error.notify_sentry(context.logger());
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
                        .unwrap_or_else(|app_error| {
                            // Handler returned an error.
                            context.add_logger_tags(o!(
                                "status" => app_error.status().as_u16(),
                                "kind" => app_error.kind().to_owned(),
                            ));

                            error!(
                                context.logger(),
                                "Failed to handle event: {}",
                                app_error.source(),
                            );

                            app_error.notify_sentry(context.logger());
                            Box::new(stream::empty())
                        })
                }
                Err(err) => {
                    // Bad envelope or payload format.
                    error!(context.logger(), "Failed to parse event: {}", err);
                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, event))
    }
}

struct MessageHandlerTiming {
    start: Instant,
    sender: TimingChannel,
    method: String,
}

impl MessageHandlerTiming {
    fn new(sender: TimingChannel) -> Self {
        Self {
            start: Instant::now(),
            method: "none".into(),
            sender,
        }
    }

    fn set_method(&mut self, method: String) {
        self.method = method;
    }
}

impl Drop for MessageHandlerTiming {
    fn drop(&mut self) {
        if let Err(e) = self
            .sender
            .try_send((self.start.elapsed(), self.method.clone()))
        {
            warn!(
                crate::LOG,
                "Failed to send msg handler future timing, reason = {:?}", e
            );
        }
    }
}
