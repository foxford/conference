use crate::outbox::{error::Error, StageHandle};
use serde::de::DeserializeOwned;
use serde::Serialize;
use svc_nats_client::EventId;

pub mod diesel;

#[async_trait::async_trait]
pub trait Pipeline {
    async fn run_single_stage<T, C>(&self, ctx: C, id: EventId) -> Result<(), Error>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static;

    async fn run_multiple_stages<T, C>(&self, ctx: C, records_per_try: i64) -> Result<(), Error>
    where
        T: StageHandle<Context = C, Stage = T>,
        T: Clone + Serialize + DeserializeOwned,
        C: Clone + Send + Sync + 'static;
}
