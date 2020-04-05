use crate::event::*;
use crate::node::{Node, NodeInfo};
use crate::protos::message::{EcNack_, EcNewEpoch_, Message, Message_Type, ProcessId};
use log::trace;
use std::sync::Arc;

const N: u32 = 10;

pub struct EpochChange {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    last_ts: u32,
    ts: u32,
    pub trusted: Node, // needs to be accessible by UniformConsensus
}

impl EpochChange {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>) -> Self {
        let id = node_info.current_node.id as u32;
        let initial_trusted = node_info
            .nodes
            .first()
            .cloned()
            .expect("Node information must have at least one node.");

        EpochChange {
            node_info,
            event_queue,
            last_ts: 0,
            ts: id,
            trusted: initial_trusted,
        }
    }

    fn eld_trust(&mut self, node: &Node) {
        self.trusted = node.clone();

        if node == &self.node_info.current_node {
            self.ts += N;
            self.new_epoch(self.ts);
        }
    }

    fn beb_deliver(&mut self, node: &Node, new_ts: u32) {
        if node == &self.trusted && new_ts > self.last_ts {
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
        let current_node = &self.node_info.current_node;
        let sender: ProcessId = current_node.into();

        message.set_sender(sender);
        message.set_ecNewEpoch(new_epoch_msg);
        message.set_field_type(Message_Type::EC_NEW_EPOCH);

        let internal_msg = InternalMessage::BebBroadcast(message);
        let event_data = EventData::Internal(internal_msg);
        self.event_queue.push(event_data);
    }

    fn start_epoch(&mut self, node: &Node, ts: u32) {
        let message = InternalMessage::EcStartEpoch(node.clone(), ts);
        let event_data = EventData::Internal(message);
        self.event_queue.push(event_data);
    }

    fn pl_send_nack(&self, node: &Node) {
        let current_node = &self.node_info.current_node;
        let sender: ProcessId = current_node.into();

        let nack = EcNack_::new();
        let mut msg = Message::new();
        msg.set_sender(sender);
        msg.set_ecNack(nack);
        msg.set_field_type(Message_Type::EC_NACK);

        let internal_message = InternalMessage::PlSend(current_node.clone(), node.clone(), msg);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    fn on_nack(&mut self) {
        if self.trusted == self.node_info.current_node {
            self.ts += N;
            self.new_epoch(self.ts);
        }
    }
}

impl EventHandler for EpochChange {
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        if let EventData::Internal(internal_data) = event_data {
            match internal_data {
                InternalMessage::EldTrust(trusted_node) => self.eld_trust(trusted_node),
                InternalMessage::BebDeliver(from, msg) => {
                    if let Message {
                        field_type: Message_Type::EC_NEW_EPOCH,
                        ..
                    } = msg
                    {
                        let new_ts = msg.get_ecNewEpoch().get_timestamp();
                        self.beb_deliver(from, new_ts as u32);
                    }
                }
                InternalMessage::PlDeliver(_, msg) => {
                    if let Message {
                        field_type: Message_Type::EC_NACK,
                        ..
                    } = msg
                    {
                        self.on_nack();
                    }
                }
                _ => (),
            }
        };
    }
}
