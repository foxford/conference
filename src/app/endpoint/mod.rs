use failure::Error;
use svc_agent::mqtt::{
    compat::IntoEnvelope, Agent, IncomingRequestProperties, OutgoingResponse, Publish,
    ResponseStatus,
};
use svc_error::ProblemDetails;

pub(crate) fn handle_response(
    kind: &str,
    title: &str,
    tx: &mut Agent,
    props: &IncomingRequestProperties,
    result: Result<impl Publish, impl ProblemDetails>,
) -> Result<(), Error> {
    match result {
        Ok(val) => {
            // Publishing success response
            val.publish(tx)?;
            Ok(())
        }
        Err(mut err) => {
            // Wrapping the error
            err.set_kind(kind, title);

            let status = err.status_code();
            let resp =
                OutgoingResponse::unicast(err, props.to_response(status), props).into_envelope()?;

            // Publishing error response
            resp.publish(tx)?;
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

    let resp = OutgoingResponse::unicast(err, props.to_response(status), props).into_envelope()?;

    // Publishing error response
    resp.publish(tx)?;
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

    let resp = OutgoingResponse::unicast(err, props.to_response(status), props).into_envelope()?;

    // Publishing error response
    resp.publish(tx)?;
    Ok(())
}

pub(crate) mod message;
pub(crate) mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
pub(crate) mod system;
