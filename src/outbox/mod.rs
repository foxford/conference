use crate::outbox::error::StageError;
use svc_nats_client::EventId;

pub mod config;
pub mod db;
pub mod error;
pub mod pipeline;
pub mod util;

#[async_trait::async_trait]
pub trait StageHandle
where
    Self: Sized + Clone + Send + Sync + 'static,
{
    type Context;
    type Stage;

    async fn handle(
        &self,
        ctx: &Self::Context,
        id: &EventId,
    ) -> Result<Option<Self::Stage>, StageError>;
}
