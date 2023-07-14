use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Router,
};
use futures::future::BoxFuture;
use http::{Method, Request};
use hyper::{body::HttpBody, Body};
use svc_agent::mqtt::{Agent, IntoPublishableMessage};
use svc_utils::middleware::MeteredRoute;
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
        .metered_route("/rooms/:id/agents", get(endpoint::agent::list))
        .metered_route(
            "/rooms/:id/configs/reader",
            get(endpoint::agent_reader_config::read).post(endpoint::agent_reader_config::update),
        )
        .metered_route(
            "/rooms/:id/configs/writer",
            get(endpoint::agent_writer_config::read).post(endpoint::agent_writer_config::update),
        )
        .metered_route("/rooms/:id/enter", post(endpoint::room::enter))
        .metered_route("/rooms/:id/close", post(endpoint::room::close))
        .metered_route("/rooms", post(endpoint::room::create))
        .metered_route(
            "/rooms/:id",
            get(endpoint::room::read).patch(endpoint::room::update),
        )
        .metered_route(
            "/rooms/:id/rtcs",
            get(endpoint::rtc::list).post(endpoint::rtc::create),
        )
        .metered_route(
            "/rooms/:id/groups",
            get(endpoint::group::list).post(endpoint::group::update),
        )
        .metered_route("/rtcs/:id", get(endpoint::rtc::read))
        .metered_route("/rtcs/:id/streams", post(endpoint::rtc::connect))
        .metered_route("/rooms/:id/streams", get(endpoint::rtc_stream::list))
        .metered_route("/streams/signal", post(endpoint::rtc_signal::create))
        .metered_route("/rtcs/:id/signal", post(endpoint::rtc::connect_and_signal))
        .metered_route("/streams/trickle", post(endpoint::rtc_signal::trickle))
        .metered_route(
            "/rooms/:id/configs/writer/snapshot",
            get(endpoint::writer_config_snapshot::read),
        )
        .layer(layer_fn(|inner| NotificationsMiddleware { inner }))
        .layer(Extension(context))
        .layer(Extension(agent))
        .layer(Extension(Arc::new(authn)))
        .layer(svc_utils::middleware::CorsLayer);
    let router = Router::new().nest("/api/v1", router);

    let pingz_router = Router::new().metered_route(
        "/healthz",
        get(|| async { Response::builder().body(Body::from("pong")).unwrap() }),
    );

    let router = router.merge(pingz_router);

    router.layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request<Body>| {
                let span = tracing::error_span!(
                    "http-api-request",
                    path = request.uri().path(),
                    query = request.uri().query(),
                    method = %request.method(),
                    status_code = Empty,
                    kind = Empty,
                    detail = Empty,
                    body_size = Empty,
                    room_id = Empty,
                    rtc_id = Empty,
                    classroom_id = Empty,
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
                    info!("response generated in {:?}", latency);
                } else {
                    error!("response generated in {:?}", latency);
                }
            }),
    )
}

impl IntoResponse for super::error::Error {
    fn into_response(self) -> Response {
        let detail = self.detail();
        let err = svc_error::Error::builder()
            .status(self.status())
            .kind(self.kind(), self.title())
            .detail(&detail)
            .build();

        let span = Span::current();
        span.record("kind", self.kind());
        span.record("detail", detail.as_str());

        let error =
            serde_json::to_string(&err).unwrap_or_else(|_| "Failed to serialize error".to_string());

        let mut r = (self.status(), error).into_response();
        r.extensions_mut().insert(self.error_kind());

        r
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
