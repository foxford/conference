use tracing::Value;

// pub struct Optioned<T>(pub Option<T>);

// impl<T: Value> Value for Optioned<T> {
//     fn record(&self, key: &tracing::field::Field, visitor: &mut dyn tracing::field::Visit) {
//         match self {
//             Some(x) => x.record(key, visitor),
//             None => {}
//         }
//     }
// }
