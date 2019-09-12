use failure::{format_err, Error};
use svc_agent::mqtt::{IncomingRequestProperties, OutgoingResponse, Publishable, ResponseStatus};
use svc_error::{extension::sentry, ProblemDetails};

pub(crate) fn handle_response(
    kind: &str,
    title: &str,
    props: &IncomingRequestProperties,
    result: Result<Vec<Box<dyn Publishable>>, impl ProblemDetails + Send + Clone + 'static>,
) -> Result<Vec<Box<dyn Publishable>>, Error> {
    result.or_else(|mut err| {
        // Wrapping the error
        err.set_kind(kind, title);
        let status = err.status_code();

        if status == ResponseStatus::UNPROCESSABLE_ENTITY
            || status == ResponseStatus::FAILED_DEPENDENCY
            || status >= ResponseStatus::INTERNAL_SERVER_ERROR
        {
            sentry::send(err.clone())
                .map_err(|err| format_err!("Error sending error to Sentry: {}", err))?;
        }

        // Publishing error response
        let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
        Ok(vec![Box::new(resp) as Box<dyn Publishable>])
    })
}

pub(crate) fn handle_badrequest(
    kind: &str,
    title: &str,
    props: &IncomingRequestProperties,
    err: &svc_agent::Error,
) -> Result<Vec<Box<dyn Publishable>>, Error> {
    let status = ResponseStatus::BAD_REQUEST;
    let err = svc_error::Error::builder()
        .kind(kind, title)
        .status(status)
        .detail(&err.to_string())
        .build();

    // Publishing error response
    let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
    Ok(vec![Box::new(resp) as Box<dyn Publishable>])
}

pub(crate) fn handle_event(
    kind: &str,
    title: &str,
    result: Result<Vec<Box<dyn Publishable>>, impl ProblemDetails + Send + Clone + 'static>,
) -> Result<Vec<Box<dyn Publishable>>, Error> {
    result.or_else(|mut err| {
        err.set_kind(kind, title);
        sentry::send(err).map_err(|err| format_err!("Error sending error to Sentry: {}", err));
        Ok(vec![])
    })
}

pub(crate) fn handle_badrequest_method(
    method: &str,
    props: &IncomingRequestProperties,
) -> Result<Vec<Box<dyn Publishable>>, Error> {
    let status = ResponseStatus::BAD_REQUEST;
    let err = svc_error::Error::builder()
        .kind("general", "General API error")
        .status(status)
        .detail(&format!("invalid request method = '{}'", method))
        .build();

    // Publishing error response
    let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
    Ok(vec![Box::new(resp) as Box<dyn Publishable>])
}

pub(crate) mod agent;
pub(crate) mod message;
pub(crate) mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
pub(crate) mod subscription;
pub(crate) mod system;
