use crate::event::*;
use crate::node::{NodeInfo, Node, NodeId};
use crate::protos::message;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;

/// Interface the of epoch consensus
/// Module:
/// Name: EpochConsensus, instance ep, with timestamp ts and leader process ℓ.
/// Events:
/// Request: ⟨ ep, Propose | v ⟩: Proposes value v for epoch consensus. Executed only by the leader ℓ.
/// Request: ⟨ ep, Abort ⟩: Aborts epoch consensus.
/// Indication: ⟨ ep, Decide | v ⟩: Outputs a decided value v of epoch consensus.
/// Indication: ⟨ ep, Aborted | state ⟩: Signals hat epoch consensus has completed the
/// abort and outputs internal state state.

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Copy, Clone)]
pub struct EpochConsensusState {
    pub value_timestamp: u32,
    pub value: ValueType,
}

impl EpochConsensusState {
    fn new(value_timestamp: u32, value: ValueType) -> Self {
        EpochConsensusState{value_timestamp, value}
    }
}

pub struct EpochConsensus {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    temporary_value: ValueType,
    value_timestamp: u32,
    value: ValueType,
    states: BTreeMap<NodeId, EpochConsensusState>,
    accepted: u32,
}

impl EpochConsensus {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>) -> Self {
        EpochConsensus {
            node_info,
            event_queue,
            value_timestamp: 0,
            value: ValueType::default(),
            temporary_value: ValueType::default(),
            states: BTreeMap::new(),
            accepted: 0,
        }
    }

    /// upon event ⟨ ep, Propose | v ⟩ do
    fn ep_propose(&mut self, value: ValueType) {
        self.temporary_value = value;
        self.beb_broadcast_read();
    }

    /// upon event ⟨ beb, Deliver | ℓ, [READ] ⟩ do
    fn beb_deliver_read(&self, from: &Node) {
        self.pl_send_state(from);
    }

    /// upon event ⟨ pl, Deliver | q, [STATE, ts, v] ⟩ do
    fn pl_deliver_state(&mut self, from: &Node, msg: &message::EpState_) {
        let value_timestamp = msg.get_valueTimestamp() as u32;
        let value = msg.get_value();
        let state = EpochConsensusState::new(value_timestamp, value);
        self.states.insert(from.id, state);
    }

    /// upon event ⟨ beb, Deliver | ℓ, [WRITE, v] ⟩ do
    fn beb_deliver_write(&mut self, from: &Node, msg: &message::EpWrite_) {
        // TODO: what is the value of ets????
        let value_from = msg.get_value() as ValueType;
        self.value = value_from;
        self.pl_send_accept(from);
    }

    /// upon event ⟨ pl, Deliver | q, [ACCEPT] ⟩ do
    fn pl_deliver_accept(&mut self) {
        self.accepted = self.accepted + 1;
    }

    /// upon event ⟨ beb, Deliver | ℓ, [DECIDED, v] ⟩ do
    fn beb_deliver_decided(&self, msg: &message::EpDecided_) {
        let value = msg.get_value();
        let internal_message = InternalMessage::EpDecide(value as ValueType);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    /// upon event ⟨ ep, Abort ⟩ do
    fn abort(&self) {
        let internal_message = InternalMessage::EpAborted(self.value_timestamp, self.value);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    fn pl_send_accept(&self, receiver: &Node) {
        let current_node = &self.node_info.current_node;
        let accept_message = message::EpAccept_::new();
        let mut message = message::Message::new();
        message.set_epAccept(accept_message);
        message.set_field_type(message::Message_Type::EP_ACCEPT);
        message.set_sender(current_node.into());
        let internal_message = InternalMessage::PlSend(current_node.clone(), receiver.clone(), message);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    fn pl_send_state(&self, receiver: &Node) {
        let current_node = &self.node_info.current_node;
        let mut state_message = message::EpState_::new();
        state_message.set_value(self.value);
        state_message.set_valueTimestamp(self.value_timestamp as i32);
        let mut message = message::Message::new();
        message.set_epState(state_message);
        message.set_field_type(message::Message_Type::EP_STATE);
        message.set_sender(current_node.into());
        let internal_message = InternalMessage::PlSend(current_node.clone(), receiver.clone(), message);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    fn beb_broadcast_read(&self) {
        let current_node = &self.node_info.current_node;
        let sender: message::ProcessId = current_node.into();

        let read_message = message::EpRead_::new();
        let mut message = message::Message::new();
        message.set_field_type(message::Message_Type::EP_READ);
        message.set_epRead(read_message);
        message.set_sender(sender);

        let internal_message = InternalMessage::BebBroadcast(message);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }
}

impl EventHandler for EpochConsensus {
    fn handle(&mut self, message: &EventData) {
        match message {
            EventData::Internal(internal_msg) => match internal_msg {
                InternalMessage::EpPropose(value) => self.ep_propose(value.clone()),
                InternalMessage::BebDeliver(from ,msg) => match msg {
                    message::Message{field_type: message::Message_Type::EP_READ, ..} => self.beb_deliver_read(from),
                    message::Message{field_type: message::Message_Type::EP_WRITE, ..} => self.beb_deliver_write(from, msg.get_epWrite()),
                    message::Message{field_type: message::Message_Type::EP_DECIDED, ..} => self.beb_deliver_decided(msg.get_epDecided()),
                    _ => (),
                }
                InternalMessage::PlDeliver(from, msg) => match msg {
                    message::Message{field_type: message::Message_Type::EP_STATE, ..} => {
                        self.pl_deliver_state(from, msg.get_epState());
                    },
                    message::Message{field_type: message::Message_Type::EP_ACCEPT, ..} => {
                        self.pl_deliver_accept();
                    }
                    _ => (),
                },
                InternalMessage::EpAbort => self.abort(),
                _ => (),
            },
            EventData::External(_) => (),
        }
    }
}
