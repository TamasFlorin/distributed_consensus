use crate::event::*;
use crate::node::*;
use crate::protos::message;
use log::{trace};
use std::sync::Arc;

pub struct BestEffortBroadcast {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
}

impl BestEffortBroadcast {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>) -> Self {
        BestEffortBroadcast {
            node_info,
            event_queue,
        }
    }

    fn broadcast(&self, message: &message::Message) {
        // send the message to all other nodes
        for node in &self.node_info.nodes {
            self.send(node, message);
        }
    }

    fn send(&self, node: &Node, message: &message::Message) {
        let from = self.node_info.current_node.clone();
        let internal_message = InternalMessage::PlSend(from, node.clone(), message.clone());
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    fn deliver(&self, sender: &Node, message: &message::Message) {
        let message = InternalMessage::BebDeliver(sender.clone(), message.clone());
        let event_data = EventData::Internal(message);
        self.event_queue.push(event_data);
    }
}

impl EventHandler for BestEffortBroadcast {
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        match event_data {
            EventData::Internal(data) => match data {
                InternalMessage::BebBroadcast(msg) => self.broadcast(msg),
                InternalMessage::PlDeliver(sender, msg) => {
                    self.deliver(&sender, msg);
                }
                _ => (),
            },
            _ => (),
        };
    }
}
