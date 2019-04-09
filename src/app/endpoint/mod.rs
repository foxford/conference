use failure::Error;
use svc_agent::mqtt::{
    compat::IntoEnvelope, Agent, IncomingRequestProperties, OutgoingResponse, Publish,
};
use svc_error::ProblemDetails;

pub(crate) fn handle_error(
    kind: &str,
    title: &str,
    tx: &mut Agent,
    props: &IncomingRequestProperties,
    result: Result<impl Publish, impl ProblemDetails>,
) -> Result<(), Error> {
    let next = match result {
        Ok(val) => {
            // Publishing success response
            val.publish(tx)
        }
        Err(mut err) => {
            // Wrapping the error
            err.set_kind(kind, title);

            let status = err.status_code();
            let resp =
                OutgoingResponse::unicast(err, props.to_response(status), props).into_envelope()?;

            // Publishing error response
            resp.publish(tx)
        }
    };
    next.map_err(Into::into)
}

pub(crate) mod room;
pub(crate) mod rtc;
pub(crate) mod rtc_signal;
pub(crate) mod rtc_stream;
pub(crate) mod system;
