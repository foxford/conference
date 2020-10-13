use std::error::Error as StdError;
use std::fmt;

use svc_agent::mqtt::ResponseStatus;
use svc_error::Error as SvcError;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub(crate) enum ErrorKind {
    AccessDenied,
    AgentNotEnteredTheRoom,
    AuthorizationFailed,
    BackendRecordingMissing,
    BackendRequestFailed,
    BackendRequestTimedOut,
    BackendNotFound,
    CapacityExceeded,
    DbConnAcquisitionFailed,
    DbQueryFailed,
    InvalidJsepFormat,
    InvalidRoomTime,
    InvalidSdpType,
    InvalidSubscriptionObject,
    MessageBuildingFailed,
    MessageHandlingFailed,
    MessageParsingFailed,
    NoAvailableBackends,
    NotImplemented,
    PublishFailed,
    ResubscriptionFailed,
    RoomClosed,
    RoomNotFound,
    RtcNotFound,
    StatsCollectionFailed,
}

impl Into<(ResponseStatus, &'static str, &'static str)> for ErrorKind {
    fn into(self) -> (ResponseStatus, &'static str, &'static str) {
        match self {
            Self::AccessDenied => (ResponseStatus::FORBIDDEN, "access_denied", "Access denied"),
            Self::AgentNotEnteredTheRoom => (
                ResponseStatus::NOT_FOUND,
                "agent_not_entered_the_room",
                "Agent not entered the room",
            ),
            Self::AuthorizationFailed => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "authorization_failed",
                "Authorization failed",
            ),
            Self::BackendRecordingMissing => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "backend_recording_missing",
                "Janus recording missing",
            ),
            Self::BackendRequestFailed => (
                ResponseStatus::FAILED_DEPENDENCY,
                "backend_request_failed",
                "Janus request failed",
            ),
            Self::BackendRequestTimedOut => (
                ResponseStatus::FAILED_DEPENDENCY,
                "backend_request_timed_out",
                "Janus request timed out",
            ),
            Self::BackendNotFound => (
                ResponseStatus::NOT_FOUND,
                "backend_not_found",
                "Backend not found",
            ),
            Self::CapacityExceeded => (
                ResponseStatus::SERVICE_UNAVAILABLE,
                "capacity_exceeded",
                "Capacity exceeded",
            ),
            Self::DbConnAcquisitionFailed => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "database_connection_acquisition_failed",
                "Database connection acquisition failed",
            ),
            Self::DbQueryFailed => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "database_query_failed",
                "Database query failed",
            ),
            Self::InvalidJsepFormat => (
                ResponseStatus::BAD_REQUEST,
                "invalid_jsep_format",
                "Invalid JSEP format",
            ),
            Self::InvalidRoomTime => (
                ResponseStatus::BAD_REQUEST,
                "invalid_room_time",
                "Invalid room time",
            ),
            Self::InvalidSdpType => (
                ResponseStatus::BAD_REQUEST,
                "invalid_sdp_type",
                "Invalid SDP type",
            ),
            Self::InvalidSubscriptionObject => (
                ResponseStatus::BAD_REQUEST,
                "invalid_subscription_object",
                "Invalid subscription object",
            ),
            Self::MessageBuildingFailed => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "message_building_failed",
                "Message building failed",
            ),
            Self::MessageHandlingFailed => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "message_handling_failed",
                "Message handling failed",
            ),
            Self::MessageParsingFailed => (
                ResponseStatus::BAD_REQUEST,
                "message_parsing_failed",
                "Message parsing failed",
            ),
            Self::NoAvailableBackends => (
                ResponseStatus::SERVICE_UNAVAILABLE,
                "no_available_backends",
                "No available backends",
            ),
            Self::NotImplemented => (
                ResponseStatus::INTERNAL_SERVER_ERROR,
                "not_implemented",
                "Not implemented",
            ),
            Self::PublishFailed => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "publish_failed",
                "Publish failed",
            ),
            Self::ResubscriptionFailed => (
                ResponseStatus::INTERNAL_SERVER_ERROR,
                "resubscription_failed",
                "Resubscription failed",
            ),
            Self::RoomClosed => (ResponseStatus::NOT_FOUND, "room_closed", "Room closed"),
            Self::RoomNotFound => (
                ResponseStatus::NOT_FOUND,
                "room_not_found",
                "Room not found",
            ),
            Self::RtcNotFound => (ResponseStatus::NOT_FOUND, "rtc_not_found", "RTC not found"),
            Self::StatsCollectionFailed => (
                ResponseStatus::UNPROCESSABLE_ENTITY,
                "stats_collection_failed",
                "Stats collection failed",
            ),
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (_status, _kind, title) = self.to_owned().into();
        write!(f, "{}", title)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct Error {
    kind: ErrorKind,
    source: Box<dyn AsRef<dyn StdError + Send + Sync + 'static>>,
}

impl Error {
    pub(crate) fn new<E>(kind: ErrorKind, source: E) -> Self
    where
        E: AsRef<dyn StdError + Send + Sync + 'static> + 'static,
    {
        Self {
            kind,
            source: Box::new(source),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("source", &self.source.as_ref().as_ref())
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.source.as_ref().as_ref())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref().as_ref())
    }
}

impl Into<SvcError> for Error {
    fn into(self) -> SvcError {
        let (status, kind, title) = self.kind.into();

        SvcError::builder()
            .status(status)
            .kind(kind, title)
            .detail(&self.source.as_ref().as_ref().to_string())
            .build()
    }
}

impl From<svc_authz::Error> for Error {
    fn from(source: svc_authz::Error) -> Self {
        let kind = match source.kind() {
            svc_authz::ErrorKind::Forbidden(_) => ErrorKind::AccessDenied,
            _ => ErrorKind::AuthorizationFailed,
        };

        Self {
            kind,
            source: Box::new(anyhow::Error::from(source)),
        }
    }
}

impl From<diesel::result::Error> for Error {
    fn from(source: diesel::result::Error) -> Self {
        Self {
            kind: ErrorKind::DbQueryFailed,
            source: Box::new(anyhow::Error::from(source)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait ErrorExt<T> {
    fn error(self, kind: ErrorKind) -> Result<T, Error>;
}

impl<T, E: AsRef<dyn StdError + Send + Sync + 'static> + 'static> ErrorExt<T> for Result<T, E> {
    fn error(self, kind: ErrorKind) -> Result<T, Error> {
        self.map_err(|source| Error::new(kind, source))
    }
}
