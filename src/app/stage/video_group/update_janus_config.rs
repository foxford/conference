use crate::{
    app::{
        context::GlobalContext,
        error::{Error, ErrorExt, ErrorKind},
    },
    backend::janus::client::update_agent_reader_config::{
        UpdateReaderConfigRequest, UpdateReaderConfigRequestBody,
        UpdateReaderConfigRequestBodyConfigItem,
    },
    db,
};
use anyhow::{anyhow, Context};
use std::sync::Arc;
use svc_agent::AgentId;

pub async fn update_janus_config(
    ctx: Arc<dyn GlobalContext + Send + Sync>,
    backend_id: AgentId,
    configs: Vec<UpdateReaderConfigRequestBodyConfigItem>,
) -> Result<(), Error> {
    let mut conn = ctx.get_conn().await?;

    let janus_backend = db::janus_backend::FindQuery::new(&backend_id)
        .execute(&mut conn)
        .await
        .error(ErrorKind::DbQueryFailed)?
        .ok_or_else(|| anyhow!("Janus backend not found"))
        .error(ErrorKind::BackendNotFound)?;

    let request = UpdateReaderConfigRequest {
        session_id: janus_backend.session_id(),
        handle_id: janus_backend.handle_id(),
        body: UpdateReaderConfigRequestBody::new(configs.clone()),
    };

    ctx.janus_clients()
        .get_or_insert(&janus_backend)
        .error(ErrorKind::BackendClientCreationFailed)?
        .reader_update(request)
        .await
        .context("Reader update")
        .error(ErrorKind::BackendRequestFailed)?;

    Ok(())
}
