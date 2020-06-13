use crate::event::*;
use crate::node::{Node, NodeInfo};
use crate::protos::message::{EcNack_, EcNewEpoch_, Message, Message_Type};
use log::trace;
use std::sync::Arc;
use uuid::Uuid;

const N: u32 = 10;
const ABSTRACTION_ID: &str = "ec";

/// The epoch-change algorithmis quite simple. Every process p maintains two timestamps:
/// a timestamp lastts of the last epoch that it started (i.e., for which it triggered
/// a ⟨ StartEpoch ⟩ event), and the timestamp ts of the last epoch that it attempted
/// to start with itself as leader (i.e., for which it broadcast a NEWEPOCH message,
/// as described next). Initially, the process sets ts to its rank. Whenever the leader
/// detector subsequently makes p trust itself, p adds N to ts and sends a NEWEPOCH
/// message with ts. When process p receives a NEWEPOCH message with a parameter
/// newts > lastts from some process ℓ and p most recently trusted ℓ, then the
/// process triggers a ⟨ StartEpoch ⟩ event with parameters newts and ℓ. Otherwise, the
/// process informs the aspiring leader ℓ with a NACK message that the new epoch could
/// not be started. When a process receives a NACK message and still trusts itself, it increments
/// ts by N and tries again to start an epoch by sending another NEWEPOCH message.
pub struct EpochChange {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    last_ts: u32,
    ts: u32,
    pub trusted: Node, // needs to be accessible by UniformConsensus
    system_id: String,
}

impl EpochChange {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>, system_id: String) -> Self {
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
            system_id,
        }
    }

    /// upon event ⟨ Ω, Trust | p ⟩ do
    fn eld_trust(&mut self, node: &Node) {
        self.trusted = node.clone();

        if node == &self.node_info.current_node {
            self.ts += N;
            self.new_epoch(self.ts);
        }
    }

    /// upon event ⟨ beb, Deliver | l, [NEWEPOCH, newts] ⟩ do
    fn beb_deliver(&mut self, node: &Node, new_ts: u32) {
        if node == &self.trusted && new_ts > self.last_ts {
            self.last_ts = new_ts;
            self.start_epoch(node, new_ts);
        } else {
            self.pl_send_nack(node);
        }
    }

    /// upon event ⟨ pl, Deliver | p, [NACK] ⟩ do
    fn on_nack(&mut self) {
        if self.trusted == self.node_info.current_node {
            self.ts += N;
            self.new_epoch(self.ts);
        }
    }

    fn new_epoch(&self, ts: u32) {
        let mut new_epoch_msg = EcNewEpoch_::new();
        new_epoch_msg.set_timestamp(ts as i32);

        let uuid = Uuid::new_v4();
        let mut message = Message::new();
        message.set_messageUuid(uuid.to_string());
        message.set_ecNewEpoch_(new_epoch_msg);
        message.set_field_type(Message_Type::EC_NEW_EPOCH_);
        message.set_abstractionId(ABSTRACTION_ID.to_owned());
        message.set_systemId(self.system_id.clone());

        let internal_msg = InternalMessage::BebBroadcast(message);
        let event_data = EventData::Internal(self.system_id.clone(), internal_msg);
        self.event_queue.push(event_data);
    }

    fn start_epoch(&mut self, node: &Node, ts: u32) {
        let message = InternalMessage::EcStartEpoch(node.clone(), ts);
        let event_data = EventData::Internal(self.system_id.clone(), message);
        self.event_queue.push(event_data);
    }

    fn pl_send_nack(&self, node: &Node) {
        let current_node = &self.node_info.current_node;
        let nack = EcNack_::new();
        
        let uuid = Uuid::new_v4();
        let mut msg = Message::new();
        msg.set_messageUuid(uuid.to_string());
        msg.set_ecNack_(nack);
        msg.set_field_type(Message_Type::EC_NACK_);
        msg.set_abstractionId(ABSTRACTION_ID.to_owned());
        msg.set_systemId(self.system_id.clone());

        let internal_message = InternalMessage::PlSend(current_node.clone(), node.clone(), msg);
        let event_data = EventData::Internal(self.system_id.clone(), internal_message);
        self.event_queue.push(event_data);
    }
}

impl EventHandler for EpochChange {
    fn should_handle_event(&self, event_data: &EventData) -> bool {
        if let EventData::Internal(system_id, _) = event_data {
            system_id == &self.system_id   
        } else {
            false
        }
    }
    
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        if let EventData::Internal(_, internal_data) = event_data {
            match internal_data {
                InternalMessage::EldTrust(trusted_node) => self.eld_trust(trusted_node),
                InternalMessage::BebDeliver(from, msg) => {
                    if let Message {
                        field_type: Message_Type::EC_NEW_EPOCH_,
                        ..
                    } = msg
                    {
                        let new_ts = msg.get_ecNewEpoch_().get_timestamp();
                        self.beb_deliver(from, new_ts as u32);
                    }
                }
                InternalMessage::PlDeliver(_, msg) => {
                    if let Message {
                        field_type: Message_Type::EC_NACK_,
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
