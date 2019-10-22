use core::future::Future;
use std::ops::Try;

use chrono::{DateTime, Utc};
use failure::Error;
use log::warn;
use serde::{de::DeserializeOwned, Serialize};
use svc_agent::mqtt::{
    compat, IncomingEventProperties, IncomingMessage, IncomingRequestProperties, OutgoingEvent,
    OutgoingRequest, OutgoingResponse, Publishable, ResponseStatus, ShortTermTimingProperties,
};
use svc_error::{extension::sentry, Error as SvcError, ProblemDetails};

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct Result {
    messages: Vec<Box<dyn Publishable>>,
    error: Option<SvcError>,
}

impl std::ops::Try for Result {
    type Ok = Vec<Box<dyn Publishable>>;
    type Error = SvcError;

    fn into_result(self) -> std::result::Result<Self::Ok, Self::Error> {
        match self.error {
            None => Ok(self.messages),
            Some(error) => Err(error),
        }
    }

    fn from_error(error: Self::Error) -> Self {
        Self {
            messages: vec![],
            error: Some(error),
        }
    }

    fn from_ok(messages: Self::Ok) -> Self {
        Self {
            messages,
            error: None,
        }
    }
}

impl<T: Serialize + 'static> From<OutgoingEvent<T>> for Result {
    fn from(event: OutgoingEvent<T>) -> Self {
        Self {
            messages: vec![Box::new(event)],
            error: None,
        }
    }
}

impl<T: Serialize + 'static> From<OutgoingRequest<T>> for Result {
    fn from(request: OutgoingRequest<T>) -> Self {
        Self {
            messages: vec![Box::new(request)],
            error: None,
        }
    }
}

impl<T: Serialize + 'static> From<OutgoingResponse<T>> for Result {
    fn from(response: OutgoingResponse<T>) -> Self {
        Self {
            messages: vec![Box::new(response)],
            error: None,
        }
    }
}

impl From<Vec<Box<dyn Publishable>>> for Result {
    fn from(messages: Vec<Box<dyn Publishable>>) -> Self {
        Self {
            messages,
            error: None,
        }
    }
}

impl From<SvcError> for Result {
    fn from(error: SvcError) -> Self {
        Self {
            messages: vec![],
            error: Some(error),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn handle_request<H, S, P, R>(
    kind: &str,
    title: &str,
    props: &IncomingRequestProperties,
    handler: H,
    state: S,
    envelope: compat::IncomingEnvelope,
    start_timestamp: DateTime<Utc>,
) -> std::result::Result<Vec<Box<dyn Publishable>>, Error>
where
    H: Fn(S, IncomingMessage<P, IncomingRequestProperties>, DateTime<Utc>) -> R,
    P: DeserializeOwned,
    R: Future<Output = Result>,
{
    match compat::into_request::<P>(envelope) {
        Ok(request) => handler(state, request, start_timestamp)
            .await
            .into_result()
            .or_else(|err| handle_error(kind, title, props, err, start_timestamp)),
        Err(err) => {
            let status = ResponseStatus::BAD_REQUEST;

            let err = svc_error::Error::builder()
                .kind(kind, title)
                .status(status)
                .detail(&err.to_string())
                .build();

            let timing = ShortTermTimingProperties::until_now(start_timestamp);
            let resp = OutgoingResponse::unicast(err, props.to_response(status, timing), props);
            Ok(vec![Box::new(resp) as Box<dyn Publishable>])
        }
    }
}

pub(crate) fn handle_error(
    kind: &str,
    title: &str,
    props: &IncomingRequestProperties,
    mut err: impl ProblemDetails + Send + Clone + 'static,
    start_timestamp: DateTime<Utc>,
) -> std::result::Result<Vec<Box<dyn Publishable>>, Error> {
    err.set_kind(kind, title);
    let status = err.status_code();

    if status == ResponseStatus::UNPROCESSABLE_ENTITY
        || status == ResponseStatus::FAILED_DEPENDENCY
        || status >= ResponseStatus::INTERNAL_SERVER_ERROR
    {
        sentry::send(err.clone())
            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
    }

    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let resp = OutgoingResponse::unicast(err, props.to_response(status, timing), props);
    Ok(vec![Box::new(resp) as Box<dyn Publishable>])
}

pub(crate) fn handle_unknown_method(
    method: &str,
    props: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
) -> std::result::Result<Vec<Box<dyn Publishable>>, Error> {
    let status = ResponseStatus::BAD_REQUEST;

    let err = svc_error::Error::builder()
        .kind("general", "General API error")
        .status(status)
        .detail(&format!("invalid request method = '{}'", method))
        .build();

    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let resp = OutgoingResponse::unicast(err, props.to_response(status, timing), props);
    Ok(vec![Box::new(resp) as Box<dyn Publishable>])
}

pub(crate) async fn handle_event<H, S, P, R>(
    kind: &str,
    title: &str,
    handler: H,
    state: S,
    envelope: compat::IncomingEnvelope,
    start_timestamp: DateTime<Utc>,
) -> std::result::Result<Vec<Box<dyn Publishable>>, Error>
where
    H: Fn(S, IncomingMessage<P, IncomingEventProperties>, DateTime<Utc>) -> R,
    P: DeserializeOwned,
    R: Future<Output = Result>,
{
    match compat::into_event::<P>(envelope) {
        Ok(event) => handler(state, event, start_timestamp)
            .await
            .into_result()
            .or_else(|mut err| {
                err.set_kind(kind, title);

                sentry::send(err)
                    .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));

                Ok(vec![])
            }),
        Err(err) => Err(err.into()),
    }
}

pub(crate) mod shared;

pub(crate) mod agent;
pub(crate) mod message;
pub(crate) mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
pub(crate) mod subscription;
pub(crate) mod system;
