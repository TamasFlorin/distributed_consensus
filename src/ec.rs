use crate::event::*;
use crate::node::{Node, NodeInfo};
use crate::protos::message::{EcNewEpoch_, Message, Message_Type};
use log::trace;
use std::sync::{Arc, Mutex};

const N: u32 = 10;

pub struct EpochChange {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<Mutex<EventQueue>>,
    last_ts: u32,
    ts: u32,
    trusted: Option<Node>,
}

impl EpochChange {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<Mutex<EventQueue>>) -> Self {
        let id = node_info.current_node.id as u32;
        EpochChange {
            node_info,
            event_queue,
            last_ts: 0,
            ts: id,
            trusted: None,
        }
    }

    fn eld_trust(&mut self, node: &Node) {
        self.trusted.replace(node.clone());

        if node == &self.node_info.current_node {
            self.ts += N;
            self.new_epoch(self.ts);
        }
    }

    fn beb_deliver(&mut self, node: &Node, new_ts: u32) {
        let trusted = self.trusted.as_ref().unwrap();
        if node == trusted && new_ts > self.last_ts {
            self.last_ts = new_ts;
            self.start_epoch(node, new_ts);
        } else {
            self.pl_send_nack(node);
        }
    }

    fn new_epoch(&self, ts: u32) {
        let mut new_epoch_msg = EcNewEpoch_::new();
        new_epoch_msg.set_timestamp(ts as i32);

        let mut message = Message::new();
        message.set_ecNewEpoch(new_epoch_msg);
        message.set_field_type(Message_Type::EC_NEW_EPOCH);

        let internal_msg = InternalMessage::Broadcast(message);
        let event_data = EventData::Internal(internal_msg);

        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }

    fn start_epoch(&mut self, node: &Node, ts: u32) {
        let message = InternalMessage::EcStartEpoch(node.clone(), ts);
        let event_data = EventData::Internal(message);
        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }

    fn pl_send_nack(&self, node: &Node) {
        let from = self.node_info.current_node.clone();
        let nack_msg = InternalMessage::Nack(from, node.clone());
        let event_data = EventData::Internal(nack_msg);
        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }

    fn pl_deliver(&mut self, _: &Node) {
        let trusted = self.trusted.as_ref().unwrap();
        if trusted == &self.node_info.current_node {
            self.ts += N;
            self.new_epoch(self.ts);
        }
    }
}

impl EventHandler for EpochChange {
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        match event_data {
            EventData::External(_) => (),
            EventData::Internal(internal_data) => {
                match internal_data {
                    InternalMessage::EldTrust(trusted_node) => self.eld_trust(trusted_node),
                    InternalMessage::BebDeliver(from, to, msg) => {
                        match msg {
                            // TODO: change proto to use snake case.
                            #![allow(non_snake_case)]
                            Message{field_type: Message_Type::EC_NEW_EPOCH, ecNewEpoch, ..} => {
                                if from == &self.node_info.current_node {
                                    let new_ts = ecNewEpoch.as_ref().expect("We should have a valid epoch object.").get_timestamp();
                                    self.beb_deliver(to, new_ts as u32);
                                }
                            }
                            _ => (),
                        }
                    }
                    InternalMessage::PlDeliver(from, to, msg) => match msg {
                        Message {
                            field_type: Message_Type::EC_NACK,
                            ..
                        } => {
                            if from == &self.node_info.current_node {
                                self.pl_deliver(to);
                            }
                        }
                        _ => (),
                    },
                    _ => (),
                }
            }
        }
    }
}
