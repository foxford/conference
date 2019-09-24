use core::future::Future;
use failure::{format_err, Error};
use svc_agent::mqtt::{
    compat, IncomingEventProperties, IncomingMessage, IncomingRequestProperties, OutgoingResponse,
    Publishable, ResponseStatus,
};
use svc_error::{extension::sentry, Error as SvcError, ProblemDetails};

pub(crate) async fn handle_request<H, S, P, R>(
    kind: &str,
    title: &str,
    props: &IncomingRequestProperties,
    handler: H,
    state: S,
    envelope: compat::IncomingEnvelope,
) -> Result<Vec<Box<dyn Publishable>>, Error>
where
    H: Fn(S, IncomingMessage<P, IncomingRequestProperties>) -> R,
    P: serde::de::DeserializeOwned,
    R: Future<Output = Result<Vec<Box<dyn Publishable>>, SvcError>>,
{
    match compat::into_request::<P>(envelope) {
        Ok(request) => handler(state, request)
            .await
            .or_else(|err| handle_error(kind, title, props, err)),
        Err(err) => {
            let status = ResponseStatus::BAD_REQUEST;

            let err = svc_error::Error::builder()
                .kind(kind, title)
                .status(status)
                .detail(&err.to_string())
                .build();

            let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
            Ok(vec![Box::new(resp) as Box<dyn Publishable>])
        }
    }
}

pub(crate) fn handle_error(
    kind: &str,
    title: &str,
    props: &IncomingRequestProperties,
    mut err: impl ProblemDetails + Send + Clone + 'static,
) -> Result<Vec<Box<dyn Publishable>>, Error> {
    err.set_kind(kind, title);
    let status = err.status_code();

    if status == ResponseStatus::UNPROCESSABLE_ENTITY
        || status == ResponseStatus::FAILED_DEPENDENCY
        || status >= ResponseStatus::INTERNAL_SERVER_ERROR
    {
        sentry::send(err.clone())
            .map_err(|err| format_err!("Error sending error to Sentry: {}", err))?;
    }

    let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
    Ok(vec![Box::new(resp) as Box<dyn Publishable>])
}

pub(crate) fn handle_unknown_method(
    method: &str,
    props: &IncomingRequestProperties,
) -> Result<Vec<Box<dyn Publishable>>, Error> {
    let status = ResponseStatus::BAD_REQUEST;

    let err = svc_error::Error::builder()
        .kind("general", "General API error")
        .status(status)
        .detail(&format!("invalid request method = '{}'", method))
        .build();

    let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
    Ok(vec![Box::new(resp) as Box<dyn Publishable>])
}

pub(crate) async fn handle_event<H, S, P, R>(
    kind: &str,
    title: &str,
    handler: H,
    state: S,
    envelope: compat::IncomingEnvelope,
) -> Result<Vec<Box<dyn Publishable>>, Error>
where
    H: Fn(S, IncomingMessage<P, IncomingEventProperties>) -> R,
    P: serde::de::DeserializeOwned,
    R: Future<Output = Result<Vec<Box<dyn Publishable>>, SvcError>>,
{
    match compat::into_event::<P>(envelope) {
        Ok(event) => handler(state, event).await.or_else(|mut err| {
            err.set_kind(kind, title);

            sentry::send(err)
                .map_err(|err| format_err!("Error sending error to Sentry: {}", err))?;

            Ok(vec![])
        }),
        Err(err) => Err(err.into()),
    }
}

pub(crate) mod agent;
pub(crate) mod message;
pub(crate) mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
pub(crate) mod subscription;
pub(crate) mod system;
