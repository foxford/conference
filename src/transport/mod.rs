use self::util::{AgentId, Source};

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait Addressable: Authenticable {
    fn agent_label(&self) -> &str;
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct Subscription {}

impl Subscription {
    pub(crate) fn broadcast_events<'a>(
        from: &'a dyn Authenticable,
        uri: &'a str,
    ) -> EventSubscription<'a> {
        EventSubscription::new(Source::Broadcast(from, uri))
    }

    pub(crate) fn multicast_requests<'a>() -> RequestSubscription<'a> {
        RequestSubscription::new(Source::Multicast)
    }

    pub(crate) fn unicast_requests<'a>(
        from: Option<&'a dyn Authenticable>,
    ) -> RequestSubscription<'a> {
        RequestSubscription::new(Source::Unicast(from))
    }

    pub(crate) fn unicast_responses<'a>(
        from: Option<&'a dyn Authenticable>,
    ) -> ResponseSubscription<'a> {
        ResponseSubscription::new(Source::Unicast(from))
    }
}

pub(crate) struct EventSubscription<'a> {
    source: Source<'a>,
}

impl<'a> EventSubscription<'a> {
    pub(crate) fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

pub(crate) struct RequestSubscription<'a> {
    source: Source<'a>,
}

impl<'a> RequestSubscription<'a> {
    pub(crate) fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

pub(crate) struct ResponseSubscription<'a> {
    source: Source<'a>,
}

impl<'a> ResponseSubscription<'a> {
    pub(crate) fn new(source: Source<'a>) -> Self {
        Self { source }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) use authn::Authenticable;

pub(crate) mod util;
