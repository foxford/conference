use std::{
    sync::Arc,
    task::{Context, Poll},
};

use crate::{app::error::ErrorExt, db};
use async_trait::async_trait;
use axum::{
    extract::{
        self,
        rejection::{ExtensionRejection, ExtensionsAlreadyExtracted, MissingExtension},
        Extension, FromRequest, Path, Query, RequestParts, TypedHeader,
    },
    handler::get,
    response::IntoResponse,
    routing::BoxRoute,
    AddExtensionLayer, Router,
};
use chrono::Utc;
use futures::future::BoxFuture;
use http::Request;
use serde::Deserialize;
use svc_agent::{AccountId, AgentId};
use svc_authn::token::jws_compact::extract::decode_jws_compact_with_config;
use tower::{layer::layer_fn, Layer, Service};
use uuid::Uuid;

use super::{
    context::{AppContext, AppMessageContext, GlobalContext},
    error, service_utils,
};

fn build_router(context: Arc<AppContext>) -> Router<BoxRoute> {
    let router = Router::new()
        .route("/rooms/:id/agents", get(super::endpoint::agent::list))
        .layer(AddExtensionLayer::new(context));
    router.boxed()
}

impl IntoResponse for error::Error {
    type Body = axum::body::Body;

    type BodyError = <Self::Body as axum::body::HttpBody>::Error;

    fn into_response(self) -> http::Response<Self::Body> {
        let err = svc_error::Error::builder()
            .status(self.status())
            .kind(self.kind(), self.title())
            .detail(&self.source().to_string())
            .build();
        let error =
            serde_json::to_string(&err).unwrap_or_else(|_| "Failed to serialize error".to_string());
        http::Response::builder()
            .status(self.status())
            .body(axum::body::Body::from(error))
            .expect("This is a valid response")
    }
}

pub struct AuthExtractor(pub AgentId);

#[async_trait]
impl FromRequest for AuthExtractor {
    type Rejection = super::error::Error;

    async fn from_request(req: &mut RequestParts) -> Result<Self, Self::Rejection> {
        let ctx = req
            .extensions()
            .and_then(|x| x.get::<Arc<AppContext>>())
            .ok_or_else(|| anyhow::anyhow!("Missing context"))
            .expect("Context must present");

        let auth_header = req
            .headers()
            .and_then(|x| x.get("Authorization"))
            .and_then(|x| x.to_str().ok())
            .and_then(|x| x.get("Bearer ".len()..))
            .ok_or_else(|| anyhow::anyhow!("Something is wrong with authorization header"))
            .error(super::error::ErrorKind::AuthenticationFailed)?;
        let claims = decode_jws_compact_with_config::<String>(auth_header, &ctx.config().authn)
            .error(super::error::ErrorKind::AuthorizationFailed)?
            .claims;
        let account = AccountId::new(claims.subject(), claims.audience());
        let agent_id = AgentId::new("http", account);
        Ok(AuthExtractor(agent_id))
    }
}
