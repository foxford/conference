use std::{sync::Arc, time::Duration};

use futures::channel;
use isahc::{http::Uri, AsyncReadResponseExt, HttpClient, Request};
use slog::warn;

use crate::backend::janus::events::IncomingEvent;

pub(crate) struct Poller {
    http_client: HttpClient,
    janus_url: Uri,
    sink: futures_channel::mpsc::UnboundedSender<IncomingEvent>,
}

impl Poller {
    pub fn new(
        sink: futures_channel::mpsc::UnboundedSender<IncomingEvent>,
        janus_url: Uri,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http_client: HttpClient::new()?,
            sink,
            janus_url,
        })
    }

    pub async fn start(self: Arc<Self>, session_id: i64) {
        loop {
            let get_task = async {
                let response = self
                    .http_client
                    .get_async(format!("{}/{}?maxev=5", self.janus_url, session_id))
                    .await?
                    .text()
                    .await?;
                if response.contains("keepalive") {
                    return Ok(());
                }
                let deseriaized: Vec<IncomingEvent> =
                    serde_json::from_str(&response).map_err(|err| {
                        warn!(crate::LOG, "Err: {:?}, raw: {}", err, response);
                        err
                    })?;
                for resp in deseriaized {
                    self.sink.unbounded_send(resp).unwrap();
                }
                Ok::<_, anyhow::Error>(())
            };
            if let Err(err) = get_task.await {
                async_std::task::sleep(Duration::from_millis(100));
                error!(crate::LOG, "Got error: {:#}", err)
            }
        }
    }
}
