use crate::event::*;
use std::sync::Arc;

pub struct UniformConsensus {
    event_queue: Arc<EventQueue>,
    value: Option<ValueType>,
    proposed: bool,
    decided: bool,
}

impl UniformConsensus {
    pub fn new(event_queue: Arc<EventQueue>) -> Self {
        UniformConsensus {
            event_queue,
            value: None,
            proposed: false,
            decided: false,
        }
    }

    fn uc_propose(&mut self, value: ValueType) {
        self.value.replace(value);
    }
}

impl EventHandler for UniformConsensus {
    fn handle(&mut self, message: &EventData) {
        match message {
            EventData::Internal(msg) => match msg {
                InternalMessage::UcPropose(value) => self.uc_propose(value.clone()),
                _ => (),
            },
            _ => (),
        }
    }
}
