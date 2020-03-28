use crate::event::*;
use crate::node::{Node, NodeInfo};
use crate::protos::message::Message;
use log::trace;
use std::sync::{Arc, Mutex};

pub struct BestEffortBroadcast {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<Mutex<EventQueue>>,
}

impl BestEffortBroadcast {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<Mutex<EventQueue>>) -> Self {
        BestEffortBroadcast {
            node_info,
            event_queue,
        }
    }

    fn broadcast(&self, message: &Message) {
        // send the message to all other nodes
        for node in &self.node_info.nodes {
            self.send(node, message);
        }

        // send the message to ourselvles
        self.send(&self.node_info.current_node, message);
    }

    fn send(&self, node: &Node, message: &Message) {
        let internal_message = InternalMessage::Send(node.clone(), message.clone());
        let event_data = EventData::Internal(internal_message);
        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }

    fn deliver(&self, node: &Node, message: &Message) {
        unimplemented!();
    }
}

impl EventHandler for BestEffortBroadcast {
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        match event_data {
            EventData::Internal(data) => match data {
                InternalMessage::Broadcast(msg) => self.broadcast(msg),
                _ => (),
            },
            _ => (),
        };
    }
}
