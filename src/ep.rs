use crate::event::*;
use crate::node::NodeInfo;
use crate::protos::message;
use std::sync::{Arc, Mutex};

const MAX_BUFFER: usize = 2048;
type ValueType = i32;

pub struct EpochConsensus {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<Mutex<EventQueue>>,
    current_val: ValueType,
    states: [ValueType; MAX_BUFFER],
    accepted: u32,
}

impl EpochConsensus {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<Mutex<EventQueue>>) -> Self {
        EpochConsensus {
            node_info,
            event_queue,
            current_val: ValueType::default(),
            states: [0; MAX_BUFFER],
            accepted: 0,
        }
    }

    fn ep_propose(&mut self, value: ValueType) {
        self.current_val = value;
        self.beb_read();
    }

    fn beb_read(&self) {
        let read_message = message::EpRead_::new();
        let mut message = message::Message::new();
        message.set_field_type(message::Message_Type::EP_READ);
        message.set_epRead(read_message);
        let internal_message = InternalMessage::BebBroadcast(message);
        let event_data = EventData::Internal(internal_message);
        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }
}
