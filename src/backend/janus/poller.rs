use slog::{error, warn};

use super::http::{IncomingEvent, JanusClient, PollResult, SessionId};

pub async fn start_polling(
    janus_client: JanusClient,
    session_id: SessionId,
    sink: async_std::channel::Sender<IncomingEvent>,
) {
    loop {
        let poll_result = janus_client.poll(session_id).await;
        match poll_result {
            Ok(PollResult::SessionNotFound) => {
                warn!(crate::LOG, "Session {} not found", session_id);
            }
            Ok(PollResult::Continue) => {
                continue;
            }
            Ok(PollResult::Events(events)) => {
                for event in events {
                    sink.send(event).await.expect("Receiver must exist");
                }
            }
            Err(err) => {
                error!(crate::LOG, "Polling error for {}: {:#}", session_id, err);
            }
        }
    }
}
