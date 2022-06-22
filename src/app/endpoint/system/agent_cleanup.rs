use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::Extension;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use svc_agent::mqtt::ResponseStatus;
use svc_authn::Authenticable;
use svc_utils::extractors::AuthnExtractor;
use tracing_attributes::instrument;

use crate::{
    app::{
        context::{AppContext, Context},
        endpoint::prelude::*,
        service_utils::{RequestParams, Response},
    },
    authz::AuthzObject,
    db,
};

#[derive(Debug, Deserialize)]
pub struct Request {}

pub async fn agent_cleanup(
    Extension(ctx): Extension<Arc<AppContext>>,
    AuthnExtractor(agent_id): AuthnExtractor,
) -> RequestResult {
    let request = Request {};
    Handler::handle(
        &mut ctx.start_message(),
        request,
        RequestParams::Http {
            agent_id: &agent_id,
        },
    )
    .await
}

pub struct Handler;

#[async_trait]
impl RequestHandler for Handler {
    type Payload = Request;
    const ERROR_TITLE: &'static str = "Failed to cleanup agents";

    #[instrument(skip(context, _payload, reqp))]
    async fn handle<C: Context>(
        context: &mut C,
        _payload: Self::Payload,
        reqp: RequestParams<'_>,
    ) -> RequestResult {
        // Authorization: only trusted subjects are allowed to perform operations with the system
        let audience = context.agent_id().as_account_id().audience();

        context
            .authz()
            .authorize(
                audience.into(),
                reqp,
                AuthzObject::new(&["system"]).into(),
                "update".into(),
            )
            .await?;

        let response = Response::new(
            ResponseStatus::NO_CONTENT,
            json!({}),
            context.start_timestamp(),
            None,
        );

        let conn = context.get_conn().await?;
        crate::util::spawn_blocking(move || {
            // TODO: move to constant but chrono doesnt support const fns
            db::agent::CleanupQuery::new(Utc::now() - chrono::Duration::days(1)).execute(&conn)
        })
        .await?;

        Ok(response)
    }
}
