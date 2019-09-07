use failure::{format_err, Error};
use svc_agent::mqtt::{
    Agent, IncomingRequestProperties, OutgoingResponse, Publishable, ResponseStatus,
};
use svc_error::{extension::sentry, ProblemDetails};

pub(crate) fn handle_response(
    kind: &str,
    title: &str,
    tx: &mut Agent,
    props: &IncomingRequestProperties,
    result: Result<Vec<Box<dyn Publishable>>, impl ProblemDetails + Send + Clone + 'static>,
) -> Result<(), Error> {
    match result {
        Ok(val) => {
            // Publishing success response
            for publishable in val.into_iter() {
                tx.publish(publishable)?;
            }

            Ok(())
        }
        Err(mut err) => {
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
            tx.publish(Box::new(resp) as Box<dyn Publishable>)?;
            Ok(())
        }
    }
}

pub(crate) fn handle_badrequest(
    kind: &str,
    title: &str,
    tx: &mut Agent,
    props: &IncomingRequestProperties,
    err: &svc_agent::Error,
) -> Result<(), Error> {
    let status = ResponseStatus::BAD_REQUEST;
    let err = svc_error::Error::builder()
        .kind(kind, title)
        .status(status)
        .detail(&err.to_string())
        .build();

    // Publishing error response
    let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
    tx.publish(Box::new(resp) as Box<dyn Publishable>)?;
    Ok(())
}

pub(crate) fn handle_badrequest_method(
    method: &str,
    tx: &mut Agent,
    props: &IncomingRequestProperties,
) -> Result<(), Error> {
    let status = ResponseStatus::BAD_REQUEST;
    let err = svc_error::Error::builder()
        .kind("general", "General API error")
        .status(status)
        .detail(&format!("invalid request method = '{}'", method))
        .build();

    // Publishing error response
    let resp = OutgoingResponse::unicast(err, props.to_response(status), props);
    tx.publish(Box::new(resp) as Box<dyn Publishable>)?;
    Ok(())
}

pub(crate) mod message;
pub(crate) mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
pub(crate) mod system;
