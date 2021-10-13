use crate::{
    config::JanusRegistry,
    db::{self, ConnectionPool},
};
use anyhow::{Result};
use http::Response;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Server,
};
use serde::Deserialize;
use svc_agent::AgentId;
use tracing::error;

use super::{
    client::{HandleId, SessionId},
    client_pool::Clients,
};

#[derive(Debug, Deserialize)]
struct Session {
    session_id: SessionId,
    handle_id: HandleId,
}

#[derive(Debug, Deserialize)]
pub struct Online {
    session: Session,
    description: JanusDesc,
}

#[derive(Debug, Deserialize)]
struct JanusDesc {
    capacity: Option<i32>,
    balancer_capacity: Option<i32>,
    group: Option<String>,
    janus_url: String,
    agent_id: AgentId,
}

pub async fn start_janus_reg_handler(
    janus_registry: JanusRegistry,
    clients: Clients,
    db: ConnectionPool,
) -> anyhow::Result<()> {
    let token = janus_registry.token.clone();
    let service = make_service_fn(move |_| {
        let clients = clients.clone();
        let db = db.clone();
        let token = token.clone();
        std::future::ready::<Result<_, hyper::Error>>(Ok(service_fn(move |req| {
            let clients = clients.clone();
            let db = db.clone();
            let token = token.clone();
            async move {
                let handle = async {
                    if req
                        .headers()
                        .get("Authorization")
                        .and_then(|x| x.to_str().ok())
                        .map_or(true, |h| h != token)
                    {
                        return Ok::<_, anyhow::Error>(
                            Response::builder().status(401).body(Body::empty())?,
                        );
                    }
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
    let server = Server::bind(&janus_registry.bind_addr).serve(service);

    server.await?;

    Ok(())
}

async fn handle_online(event: Online, clients: Clients, db: ConnectionPool) -> Result<()> {
    let backend = crate::util::spawn_blocking(move || {
        let conn = db.get()?;
        let mut q = db::janus_backend::UpsertQuery::new(
            &event.description.agent_id,
            event.session.handle_id,
            event.session.session_id,
            &event.description.janus_url,
        );

        if let Some(capacity) = event.description.capacity {
            q = q.capacity(capacity);
        }

        if let Some(balancer_capacity) = event.description.balancer_capacity {
            q = q.balancer_capacity(balancer_capacity);
        }

        if let Some(group) = event.description.group.as_deref() {
            q = q.group(group);
        }

        let janus = q.execute(&conn)?;
        Ok::<_, anyhow::Error>(janus)
    })
    .await?;
    clients.get_or_insert(&backend)?;
    Ok(())
}
