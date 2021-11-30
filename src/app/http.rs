use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    extract::{FromRequest, RequestParts},
    response::IntoResponse,
    routing::{get, post},
    AddExtensionLayer, Router,
};
use futures::future::BoxFuture;
use http::{Method, Request, Response};
use hyper::{body::HttpBody, Body};
use svc_agent::{
    mqtt::{Agent, IntoPublishableMessage},
    AccountId, AgentId,
};
use svc_authn::token::jws_compact::extract::decode_jws_compact_with_config;
use tower::{layer::layer_fn, Service};
use tower_http::trace::TraceLayer;
use tracing::{
    error,
    field::{self, Empty},
    info, Span,
};

use super::{
    context::{AppContext, GlobalContext},
    endpoint,
};
use crate::app::{error::ErrorExt, message_handler::publish_message};

pub fn build_router(context: Arc<AppContext>, agent: Agent) -> Router {
    let router = Router::new()
        .route("/rooms/:id/agents", get(endpoint::agent::list))
        .route(
            "/rooms/:id/configs/reader",
            get(endpoint::agent_reader_config::read).post(endpoint::agent_reader_config::update),
        )
        .route(
            "/rooms/:id/configs/writer",
            get(endpoint::agent_writer_config::read).post(endpoint::agent_writer_config::update),
        )
        .route("/rooms/:id/close", post(endpoint::room::close))
        .route("/rooms", post(endpoint::room::create))
        .route(
            "/rooms/:id",
            get(endpoint::room::read).patch(endpoint::room::update),
        )
        .route(
            "/rooms/:id/rtcs",
            get(endpoint::rtc::list).post(endpoint::rtc::create),
        )
        .route("/rtcs/:id", get(endpoint::rtc::read))
        .route("/rtcs/:id/streams", post(endpoint::rtc::connect))
        .route("/rooms/:id/streams", get(endpoint::rtc_stream::list))
        .route("/system/vacuum", post(endpoint::system::vacuum))
        .route("/system/vacuum", post(endpoint::system::agent_cleanup))
        .route(
            "/rooms/:id/configs/writer/snapshot",
            get(endpoint::writer_config_snapshot::read),
        )
        .layer(layer_fn(|inner| NotificationsMiddleware { inner }))
        .layer(AddExtensionLayer::new(context))
        .layer(AddExtensionLayer::new(agent));
    let router = Router::new().nest("/api/v1", router);

    let pingz_router = Router::new().route(
        "/healthz",
        get(|| async { Response::builder().body(Body::from("pong")).unwrap() }),
    );

    let router = router.merge(pingz_router);

    router.layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request<Body>| {
                let span = tracing::error_span!(
                    "http-api-request",
                    status_code = Empty,
                    path = request.uri().path(),
                    query = request.uri().query(),
                    method = %request.method(),
                );

                if request.method() != Method::GET && request.method() != Method::OPTIONS {
                    span.record(
                        "body_size",
                        &field::debug(request.body().size_hint().upper()),
                    );
                }

                span
            })
            .on_response(|response: &Response<_>, latency: Duration, span: &Span| {
                span.record("status_code", &field::debug(response.status()));
                if response.status().is_success() {
                    info!("response generated in {:?}", latency)
                } else {
                    error!("response generated in {:?}", latency)
                }
            }),
    )
}

impl IntoResponse for super::error::Error {
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

#[derive(Clone)]
struct NotificationsMiddleware<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for NotificationsMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // best practice is to clone the inner service like this
        // see https://github.com/tower-rs/tower/issues/547 for details
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let mut agent = req.extensions().get::<Agent>().cloned().unwrap();
            let mut res: Response<ResBody> = inner.call(req).await?;
            if let Some(notifications) = res
                .extensions_mut()
                .remove::<Vec<Box<dyn IntoPublishableMessage + Send + Sync + 'static>>>()
            {
                for notification in notifications {
                    publish_message(&mut agent, notification)
                }
            }

            Ok(res)
        })
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
