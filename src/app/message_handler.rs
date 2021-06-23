use anyhow::{anyhow, Context as AnyhowContext};
use async_std::{
    prelude::*,
    stream::{self, Stream},
};
use chrono::{DateTime, Utc};
use slog::{error, o, warn};
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

use crate::{
    app::{
        context::{AppMessageContext, Context, GlobalContext, MessageContext},
        endpoint,
        error::{Error as AppError, ErrorExt, ErrorKind as AppErrorKind},
        API_VERSION,
    },
    backend::{janus, janus::handle_event},
};

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

    pub async fn handle(&self, message: &Result<IncomingMessage<String>, String>, topic: &str) {
        let mut msg_context = AppMessageContext::new(&self.global_context, Utc::now());

        match message {
            Ok(ref msg) => {
                let handle_result = self.handle_message(&mut msg_context, msg, topic).await;
                msg_context.metrics().observe_app_result(&handle_result);
                if let Err(err) = handle_result {
                    Self::report_error(&mut msg_context, message, &err.to_string()).await;
                }
            }
            Err(e) => {
                Self::report_error(&mut msg_context, message, e).await;
            }
        }
    }

    pub async fn handle_events(&self, message: janus::client::IncomingEvent) {
        let mut msg_context = AppMessageContext::new(&self.global_context, Utc::now());

        let messages = handle_event(&mut msg_context, message).await;

        if let Err(err) = self.publish_outgoing_messages(messages).await {
            warn!(msg_context.logger(), "Incoming event error: {:#}", err);
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
        match message {
            IncomingMessage::Request(req) => self.handle_request(msg_context, req, topic).await,
            IncomingMessage::Response(resp) => self.handle_response(msg_context, resp, topic).await,
            IncomingMessage::Event(event) => self.handle_event(msg_context, event, topic).await,
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

        let raw_corr_data = response.properties().correlation_data();

        let outgoing_message_stream =
            endpoint::route_response(msg_context, response, raw_corr_data, topic).await;

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
        mut message_stream: MessageStream,
    ) -> Result<(), AppError> {
        let mut agent = self.agent.clone();

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

pub fn publish_message(
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
                    H::handle(context, payload, reqp)
                        .await
                        .unwrap_or_else(|app_error| {
                            context.add_logger_tags(o!(
                                "status" => app_error.status().as_u16(),
                                "kind" => app_error.kind().to_owned(),
                            ));

                            error!(
                                context.logger(),
                                "Failed to handle request: {:#}",
                                app_error.source(),
                            );

                            app_error.notify_sentry(context.logger());

                            // Handler returned an error.
                            error_response(
                                app_error.status(),
                                app_error.kind(),
                                app_error.title(),
                                &app_error.source().to_string(),
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
                Ok(payload) => {
                    H::handle(context, payload, respp, corr_data)
                        .await
                        .unwrap_or_else(|app_error| {
                            // Handler returned an error.
                            context.add_logger_tags(o!(
                                "status" => app_error.status().as_u16(),
                                "kind" => app_error.kind().to_owned(),
                            ));

                            error!(
                                context.logger(),
                                "Failed to handle response: {:#}",
                                app_error.source(),
                            );

                            app_error.notify_sentry(context.logger());
                            Box::new(stream::empty())
                        })
                }
                Err(err) => {
                    // Bad envelope or payload format.
                    error!(context.logger(), "Failed to parse response: {:#}", err);
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
                                "Failed to handle event: {:#}",
                                app_error.source(),
                            );

                            app_error.notify_sentry(context.logger());
                            Box::new(stream::empty())
                        })
                }
                Err(err) => {
                    // Bad envelope or payload format.
                    error!(context.logger(), "Failed to parse event: {:#}", err);
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
