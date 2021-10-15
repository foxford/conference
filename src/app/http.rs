use std::{
    sync::Arc,
    task::{Context, Poll},
};

use crate::app::message_handler::publish_message;
use axum::{
    handler::{get, post},
    response::IntoResponse,
    routing::BoxRoute,
    AddExtensionLayer, Router,
};

use futures::future::BoxFuture;
use http::{Request, Response};

use svc_agent::mqtt::{Agent, IntoPublishableMessage};
use tower::{layer::layer_fn, Service};

use super::{context::AppContext, endpoint, error};

pub fn build_router(context: Arc<AppContext>, agent: Agent) -> Router<BoxRoute> {
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
        // .route("/rooms/:id/enter", post(endpoint::room::enter))
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
        .route(
            "/rooms/:id/configs/writer/snapshot",
            get(endpoint::writer_config_snapshot::read),
        )
        .layer(layer_fn(|inner| NotificationsMiddleware { inner }))
        .layer(AddExtensionLayer::new(context))
        .layer(AddExtensionLayer::new(agent));
    let router = Router::new().nest("/api/v1", router);
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
