// use std::fmt;
// use std::fmt::Write;
// use std::marker::PhantomData;
// use std::{
//     any::{type_name, TypeId},
//     collections::HashMap,
// };
// use tracing::{
//     field::{display, DisplayValue, Field, FieldSet, Visit},
//     span, Dispatch, Metadata, Subscriber,
// };

// use tracing_subscriber::fmt::format::{DefaultFields, FormatFields};
// use tracing_subscriber::{
//     fmt::FormattedFields,
//     layer::{self, Layer},
//     registry::LookupSpan,
// };

// struct TraceId(String);

// struct Visitor<'a>(&'a mut TraceId);

// impl<'a> Visit for Visitor<'a> {
//     fn record_str(&mut self, field: &Field, value: &str) {
//         self.record_debug(field, &value)
//     }

//     fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {}
// }
// pub struct ErrorLayer<S> {
//     get_context: WithContext,
//     _subscriber: PhantomData<fn(S)>,
// }

// // this function "remembers" the types of the subscriber and the formatter,
// // so that we can downcast to something aware of them without knowing those
// // types at the callsite.
// pub(crate) struct WithContext(
//     fn(&Dispatch, &span::Id, f: &mut dyn FnMut(&str, &HashMap<String, String>) -> bool),
// );

// impl<S> Layer<S> for ErrorLayer<S>
// where
//     S: Subscriber + for<'span> LookupSpan<'span>,
// {
//     /// Notifies this layer that a new span was constructed with the given
//     /// `Attributes` and `Id`.
//     fn new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
//         let mut span = ctx.span(id).expect("span must already exist!");
//         if span.extensions().get::<HashMap<String, String>>().is_some() {
//             return;
//         }
//         let mut h = HashMap::new();
//         attrs.record(&mut Visitor(&mut h));
//         span.extensions_mut().insert(h);
//     }

//     unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
//         match id {
//             id if id == TypeId::of::<Self>() => Some(self as *const _ as *const ()),
//             id if id == TypeId::of::<WithContext>() => {
//                 Some(&self.get_context as *const _ as *const ())
//             }
//             _ => None,
//         }
//     }
// }

// impl<S> ErrorLayer<S>
// where
//     S: Subscriber + for<'span> LookupSpan<'span>,
// {
//     /// Returns a new `ErrorLayer` with the provided [field formatter].
//     ///
//     /// [field formatter]: https://docs.rs/tracing-subscriber/0.2.2/tracing_subscriber/fmt/trait.FormatFields.html
//     pub fn new() -> Self {
//         Self {
//             get_context: WithContext(Self::get_context),
//             _subscriber: PhantomData,
//         }
//     }

//     fn get_context(
//         dispatch: &Dispatch,
//         id: &span::Id,
//         f: &mut dyn FnMut(&str, &HashMap<String, String>) -> bool,
//     ) {
//         let subscriber = dispatch
//             .downcast_ref::<S>()
//             .expect("subscriber should downcast to expected type; this is a bug!");
//         let span = subscriber
//             .span(id)
//             .expect("registry should have a span for the current ID");
//         let parents = span.parents();
//         for span in std::iter::once(span).chain(parents) {
//             if let Some(ext) = span.extensions().get::<HashMap<String, String>>() {
//                 let cont = f(span.name(), &ext);
//             }
//         }
//     }
// }

// impl WithContext {
//     pub(crate) fn with_context<'a>(
//         &self,
//         dispatch: &'a Dispatch,
//         id: &span::Id,
//         mut f: impl FnMut(&str, &HashMap<String, String>) -> bool,
//     ) {
//         (self.0)(dispatch, id, &mut f)
//     }
// }

// impl<S> Default for ErrorLayer<S>
// where
//     S: Subscriber + for<'span> LookupSpan<'span>,
// {
//     fn default() -> Self {
//         Self::new()
//     }
// }
