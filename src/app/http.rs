use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    response::{IntoResponse, Response},
    routing::{get, options},
    Extension, Router,
};
use futures::future::BoxFuture;
use http::{Method, Request};
use hyper::{body::HttpBody, Body};
use svc_agent::mqtt::{Agent, IntoPublishableMessage};
use tower::{layer::layer_fn, Service};
use tower_http::trace::TraceLayer;
use tracing::{
    error,
    field::{self, Empty},
    info, Span,
};

use super::{context::AppContext, endpoint};
use crate::app::message_handler::publish_message;

pub fn build_router(
    context: Arc<AppContext>,
    agent: Agent,
    authn: svc_authn::jose::ConfigMap,
) -> Router {
    let router = Router::new()
        .route(
            "/rooms/:id/agents",
            options(endpoint::options).get(endpoint::agent::list),
        )
        .route(
            "/rooms/:id/configs/reader",
            options(endpoint::options)
                .get(endpoint::agent_reader_config::read)
                .post(endpoint::agent_reader_config::update),
        )
        .route(
            "/rooms/:id/configs/writer",
            options(endpoint::options)
                .get(endpoint::agent_writer_config::read)
                .post(endpoint::agent_writer_config::update),
        )
        .route(
            "/rooms/:id/enter",
            options(endpoint::options).post(endpoint::room::enter),
        )
        .route(
            "/rooms/:id/close",
            options(endpoint::options).post(endpoint::room::close),
        )
        .route(
            "/rooms",
            options(endpoint::options).post(endpoint::room::create),
        )
        .route(
            "/rooms/:id",
            options(endpoint::options)
                .get(endpoint::room::read)
                .patch(endpoint::room::update),
        )
        .route(
            "/rooms/:id/rtcs",
            options(endpoint::options)
                .get(endpoint::rtc::list)
                .post(endpoint::rtc::create),
        )
        .route(
            "/rtcs/:id",
            options(endpoint::options).get(endpoint::rtc::read),
        )
        .route(
            "/rtcs/:id/streams",
            options(endpoint::options).post(endpoint::rtc::connect),
        )
        .route(
            "/rooms/:id/streams",
            options(endpoint::options).get(endpoint::rtc_stream::list),
        )
        .route(
            "/streams/signal",
            options(endpoint::options).post(endpoint::rtc_signal::create),
        )
        .route(
            "/rooms/:id/configs/writer/snapshot",
            options(endpoint::options).get(endpoint::writer_config_snapshot::read),
        )
        .layer(layer_fn(|inner| NotificationsMiddleware { inner }))
        .layer(Extension(context))
        .layer(Extension(agent))
        .layer(Extension(Arc::new(authn)))
        .layer(svc_utils::middleware::CorsLayer);
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
    fn into_response(self) -> Response {
        let err = svc_error::Error::builder()
            .status(self.status())
            .kind(self.kind(), self.title())
            .detail(&self.source().to_string())
            .build();
        let error =
            serde_json::to_string(&err).unwrap_or_else(|_| "Failed to serialize error".to_string());

        (self.status(), error).into_response()
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
