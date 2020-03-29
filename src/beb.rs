use crate::event::*;
use crate::node::{Node, NodeInfo};
use crate::protos::message;
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

    fn broadcast(&self, message: &message::Message) {
        // send the message to all other nodes
        for node in &self.node_info.nodes {
            self.send(node, message);
        }

        // send the message to ourselvles
        self.send(&self.node_info.current_node, message);
    }

    fn send(&self, node: &Node, message: &message::Message) {
        // wrap the received message into a beb wrapper
        let mut broadcast_message = message::BebBroadcast::new();
        broadcast_message.set_message(message.clone());

        // compose the actual beb broadcast message
        let mut message = message::Message::new();
        message.set_field_type(message::Message_Type::BEB_BROADCAST);
        message.set_bebBroadcast(broadcast_message);

        let from = self.node_info.current_node.clone();
        let internal_message = InternalMessage::Send(from, node.clone(), message.clone());
        let event_data = EventData::Internal(internal_message);
        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }

    fn deliver(&self, sender: &Node, receiver: &Node, message: &message::Message) {
        let message =
            InternalMessage::BebDeliver(sender.clone(), receiver.clone(), message.clone());
        let event_data = EventData::Internal(message);
        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }
}

impl EventHandler for BestEffortBroadcast {
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        match event_data {
            EventData::Internal(data) => match data {
                InternalMessage::Broadcast(msg) => self.broadcast(msg),
                InternalMessage::PlDeliver(sender, receiver, msg) => {
                    // we sent a message via the perfect link and now we've received a notification
                    if sender == &self.node_info.current_node {
                        self.deliver(sender, receiver, msg);
                    }
                }
                _ => (),
            },
            _ => (),
        };
    }
}
