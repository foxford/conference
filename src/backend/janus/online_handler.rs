use std::net::SocketAddr;

use crate::{
    backend::janus::client::{
        create_handle::CreateHandleRequest,
        service_ping::{ServicePingRequest, ServicePingRequestBody},
        JanusClient,
    },
    db::{self, ConnectionPool},
};
use anyhow::{Context, Result};
use http::Response;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Server,
};
use serde::Deserialize;
use svc_agent::AgentId;
use tracing::error;

use super::client_pool::Clients;

#[derive(Debug, Deserialize)]
struct Online {
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
    group: Option<String>,
    janus_url: String,
    agent_id: AgentId,
}

pub async fn start_janus_reg_handler(
    bind_addr: SocketAddr,
    clients: Clients,
    db: ConnectionPool,
) -> anyhow::Result<()> {
    let service = make_service_fn(move |_| {
        let clients = clients.clone();
        let db = db.clone();
        std::future::ready::<Result<_, hyper::Error>>(Ok(service_fn(move |req| {
            let clients = clients.clone();
            let db = db.clone();
            async move {
                let handle = async {
                    let online: Online =
                        serde_json::from_slice(&hyper::body::to_bytes(req.into_body()).await?)?;
                    handle_online(online, clients, db).await?;
                    Ok::<_, anyhow::Error>(Response::builder().body(Body::empty())?)
                };
                Ok::<_, String>(handle.await.unwrap_or_else(|err| {
                    error!(?err, "Register janus failed");
                    Response::builder()
                        .status(500)
                        .body(Body::empty())
                        .expect("Must be ok")
                }))
            }
        })))
    });
    let server = Server::bind(&bind_addr).serve(service);

    server.await?;

    Ok(())
}

async fn handle_online(event: Online, clients: Clients, db: ConnectionPool) -> Result<()> {
    let existing_backend = crate::util::spawn_blocking({
        let backend_id = event.agent_id.clone();
        let db = db.clone();
        move || {
            let conn = db.get()?;
            let janus = db::janus_backend::FindQuery::new()
                .id(&backend_id)
                .execute(&conn)?;
            Ok::<_, anyhow::Error>(janus)
        }
    })
    .await?;
    let janus_client = JanusClient::new(&event.janus_url)?;
    if let Some(backend) = existing_backend {
        let ping_response = janus_client
            .service_ping(ServicePingRequest {
                session_id: backend.session_id(),
                handle_id: backend.handle_id(),
                body: ServicePingRequestBody::new(),
            })
            .await;
        if ping_response.is_ok() {
            clients.get_or_insert(&backend)?;
            return Ok(());
        }
    }

    let session = janus_client
        .create_session()
        .await
        .context("CreateSession")?;
    let handle = janus_client
        .create_handle(CreateHandleRequest {
            session_id: session.id,
            opaque_id: None,
        })
        .await
        .context("Create first handle")?;
    janus_client
        .service_ping(ServicePingRequest {
            session_id: session.id,
            handle_id: handle.id,
            body: ServicePingRequestBody::new(),
        })
        .await?;

    let backend = crate::util::spawn_blocking(move || {
        let conn = db.get()?;
        let mut q = db::janus_backend::UpsertQuery::new(
            &event.agent_id,
            handle.id,
            session.id,
            &event.janus_url,
        );

        if let Some(capacity) = event.capacity {
            q = q.capacity(capacity);
        }

        if let Some(balancer_capacity) = event.balancer_capacity {
            q = q.balancer_capacity(balancer_capacity);
        }

        if let Some(group) = event.group.as_deref() {
            q = q.group(group);
        }

        let janus = q.execute(&conn)?;
        Ok::<_, anyhow::Error>(janus)
    })
    .await?;
    clients.get_or_insert(&backend)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use rand::Rng;

    use crate::{
        backend::janus::{
            client::service_ping::{ServicePingRequest, ServicePingRequestBody},
            online_handler::{handle_online, Online},
        },
        db,
        test_helpers::{
            authz::TestAuthz,
            context::TestContext,
            db::TestDb,
            prelude::{GlobalContext, TestAgent},
            shared_helpers,
            test_deps::LocalDeps,
            SVC_AUDIENCE,
        },
    };

    #[tokio::test]
    async fn test_online_when_backends_absent() -> anyhow::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let janus = local_deps.run_janus();
        let db = TestDb::with_local_postgres(&postgres);
        let mut context = TestContext::new(db, TestAuthz::new());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        context.with_janus(tx);
        let rng = rand::thread_rng();
        let label_suffix: String = rng
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(5)
            .map(char::from)
            .collect();
        let label = format!("janus-gateway-{}", label_suffix);
        let backend_id = TestAgent::new("alpha", &label, SVC_AUDIENCE);
        let event = Online {
            agent_id: backend_id.agent_id().clone(),
            capacity: Some(1),
            balancer_capacity: Some(2),
            group: None,
            janus_url: janus.url.clone(),
        };

        handle_online(event, context.janus_clients(), context.db().clone()).await?;

        let conn = context.get_conn().await?;
        let backend = db::janus_backend::FindQuery::new()
            .id(backend_id.agent_id())
            .execute(&conn)?
            .unwrap();
        // check if handle expired by timeout;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ping_response = context
            .janus_clients()
            .get_or_insert(&backend)?
            .service_ping(ServicePingRequest {
                body: ServicePingRequestBody::new(),
                handle_id: backend.handle_id(),
                session_id: backend.session_id(),
            })
            .await?;
        context.janus_clients().remove_client(&backend);
        Ok(())
    }

    #[tokio::test]
    async fn test_online_when_backends_present() -> anyhow::Result<()> {
        let local_deps = LocalDeps::new();
        let postgres = local_deps.run_postgres();
        let janus = local_deps.run_janus();
        let db = TestDb::with_local_postgres(&postgres);
        let mut context = TestContext::new(db, TestAuthz::new());
        let conn = context.get_conn().await?;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        context.with_janus(tx);
        let (session_id, handle_id) = shared_helpers::init_janus(&janus.url).await;
        let backend =
            shared_helpers::insert_janus_backend(&conn, &janus.url, session_id, handle_id);
        let event = Online {
            agent_id: backend.id().clone(),
            capacity: Some(1),
            balancer_capacity: Some(2),
            group: None,
            janus_url: janus.url.clone(),
        };

        handle_online(event, context.janus_clients(), context.db().clone()).await?;

        let new_backend = db::janus_backend::FindQuery::new()
            .id(backend.id())
            .execute(&conn)?
            .unwrap();
        assert_eq!(backend, new_backend);
        context.janus_clients().remove_client(&backend);
        context.janus_clients().remove_client(&new_backend);
        Ok(())
    }
}
