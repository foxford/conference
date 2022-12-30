use std::any::TypeId;
use std::fmt;
use std::marker::PhantomData;
use tracing::{
    field::{Field, Visit},
    span, Dispatch, Span, Subscriber,
};

use serde::{Deserialize, Serialize};
use tracing_subscriber::{
    layer::{self, Layer},
    registry::LookupSpan,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceId(String);

impl TraceId {
    pub fn get() -> Option<Self> {
        let mut trace_id = None;
        Span::current().with_subscriber(|(id, s)| {
            if let Some(getcx) = s.downcast_ref::<WithContext>() {
                getcx.with_context(s, id, |t| trace_id = Some(t.clone()));
            }
        });
        trace_id
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

struct Visitor<'a>(&'a mut Option<String>);

impl<'a> Visit for Visitor<'a> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "request_id" {
            *self.0 = Some(format!("{value:?}"));
        }
    }
}
pub struct TraceIdLayer<S> {
    get_context: WithContext,
    _subscriber: PhantomData<fn(S)>,
}

type TraceIdSetter<'a> = &'a mut dyn FnMut(&TraceId);
// this function "remembers" the types of the subscriber and the formatter,
// so that we can downcast to something aware of them without knowing those
// types at the callsite.
struct WithContext(fn(&Dispatch, &span::Id, f: TraceIdSetter<'_>));

impl<S> Layer<S> for TraceIdLayer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        let span = ctx.span(id).expect("span must already exist!");
        if span.name() != "trace_id" || span.extensions().get::<TraceId>().is_some() {
            return;
        }
        let mut trace_id = None;
        attrs.record(&mut Visitor(&mut trace_id));
        if let Some(trace_id) = trace_id {
            span.extensions_mut().insert(TraceId(trace_id));
        }
    }

    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        match id {
            id if id == TypeId::of::<Self>() => Some(self as *const _ as *const ()),
            id if id == TypeId::of::<WithContext>() => {
                Some(&self.get_context as *const _ as *const ())
            }
            _ => None,
        }
    }
}

impl<S> TraceIdLayer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    pub fn new() -> Self {
        Self {
            get_context: WithContext(Self::get_context),
            _subscriber: PhantomData,
        }
    }

    fn get_context(dispatch: &Dispatch, id: &span::Id, f: &mut dyn FnMut(&TraceId)) {
        let subscriber = dispatch
            .downcast_ref::<S>()
            .expect("subscriber should downcast to expected type; this is a bug!");
        let span = subscriber
            .span(id)
            .expect("registry should have a span for the current ID");
        for span in span.scope() {
            if let Some(ext) = span.extensions().get::<TraceId>() {
                f(ext);
                return;
            }
        }
    }
}

impl WithContext {
    fn with_context(&self, dispatch: &Dispatch, id: &span::Id, mut f: impl FnMut(&TraceId)) {
        (self.0)(dispatch, id, &mut f)
    }
}
