use std::sync::{Arc, Mutex};

use svc_agent::mqtt::{agent::Address, IntoPublishableMessage, PublishableMessage};
use svc_agent::Error as SvcAgentError;

use crate::app::context::MessagePublisher;

#[derive(Clone)]
pub(crate) struct TestMessagePublisher {
    address: Address,
    messages: Arc<Mutex<Vec<PublishableMessage>>>,
}

impl TestMessagePublisher {
    pub(crate) fn new(address: Address) -> Self {
        Self {
            address,
            messages: Arc::new(Mutex::new(vec![])),
        }
    }
}

impl MessagePublisher for TestMessagePublisher {
    fn publish(&mut self, message: Box<dyn IntoPublishableMessage>) -> Result<(), SvcAgentError> {
        let dump = message
            .into_dump(&self.address)
            .expect("Failed to dump message");

        let mut messages = self
            .messages
            .lock()
            .expect("Failed to acquire messages mutex");

        messages.push(dump);
        Ok(())
    }
}
