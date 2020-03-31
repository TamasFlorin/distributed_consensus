use crate::event::*;
use crate::node::{Node, NodeId, NodeInfo};
use crate::protos::message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

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
        EpochConsensusState {
            value_timestamp,
            value,
        }
    }
}

impl PartialOrd<EpochConsensusState> for EpochConsensusState {
    fn partial_cmp(&self, other: &EpochConsensusState) -> Option<std::cmp::Ordering> {
        self.value_timestamp.partial_cmp(&other.value_timestamp)
    }
}

impl Ord for EpochConsensusState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value_timestamp.cmp(&other.value_timestamp)
    }
}

pub struct EpochConsensus {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    temporary_value: ValueType,
    states: BTreeMap<NodeId, EpochConsensusState>,
    accepted: u32,
    state: EpochConsensusState,
    aborted: bool,
}

impl EpochConsensus {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>) -> Self {
        EpochConsensus {
            node_info,
            event_queue,
            temporary_value: ValueType::default(),
            states: BTreeMap::new(),
            accepted: 0,
            state: EpochConsensusState::new(0, 0), // TODO: this should be impl specific
            aborted: false,
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

        if self.states.len() > self.node_info.nodes.len() / 2 {
            let states_message = InternalMessage::EpStateCountReached;
            let event_data = EventData::Internal(states_message);
            self.event_queue.push(event_data);
        }
    }

    // upon #(states) > N/2 do
    fn ep_state_count_reached(&mut self) {
        let highest_timestamp = self.states.iter().max_by(|(_, x), (_, y)| x.cmp(y));
        match highest_timestamp {
            Some((_, state)) => self.temporary_value = state.value,
            None => (),
        }

        self.states.clear();

        self.beb_broadcast_write(self.temporary_value);
    }

    /// upon event ⟨ beb, Deliver | ℓ, [WRITE, v] ⟩ do
    fn beb_deliver_write(&mut self, from: &Node, msg: &message::EpWrite_) {
        // TODO: what is the value of ets????
        let value_from = msg.get_value() as ValueType;
        self.state.value = value_from;
        self.pl_send_accept(from);
    }

    /// upon event ⟨ pl, Deliver | q, [ACCEPT] ⟩ do
    fn pl_deliver_accept(&mut self) {
        self.accepted = self.accepted + 1;

        if self.accepted as usize > self.node_info.nodes.len() {
            let accepted_message = InternalMessage::EpAcceptedCountReached;
            let event_data = EventData::Internal(accepted_message);
            self.event_queue.push(event_data);
        }
    }

    /// upon accepted > N/2 do
    fn ep_accepted_count_reached(&mut self) {
        self.accepted = 0;

        let current_node = &self.node_info.current_node;

        let mut decided_message = message::EpDecided_::new();
        decided_message.set_value(self.temporary_value as i32);

        let mut msg = message::Message::new();
        msg.set_epDecided(decided_message);
        msg.set_sender(current_node.into());

        let broadcast_message = InternalMessage::BebBroadcast(msg);
        let event_data = EventData::Internal(broadcast_message);
        self.event_queue.push(event_data);
    }

    /// upon event ⟨ beb, Deliver | ℓ, [DECIDED, v] ⟩ do
    fn beb_deliver_decided(&self, msg: &message::EpDecided_) {
        let value = msg.get_value();
        let internal_message = InternalMessage::EpDecide(value as ValueType);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    /// upon event ⟨ ep, Abort ⟩ do
    fn abort(&mut self) {
        self.aborted = true;
        let internal_message =
            InternalMessage::EpAborted(self.state.value_timestamp, self.state.value);
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
        let internal_message =
            InternalMessage::PlSend(current_node.clone(), receiver.clone(), message);
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }

    fn pl_send_state(&self, receiver: &Node) {
        let current_node = &self.node_info.current_node;
        let mut state_message = message::EpState_::new();
        state_message.set_value(self.state.value);
        state_message.set_valueTimestamp(self.state.value_timestamp as i32);
        let mut message = message::Message::new();
        message.set_epState(state_message);
        message.set_field_type(message::Message_Type::EP_STATE);
        message.set_sender(current_node.into());
        let internal_message =
            InternalMessage::PlSend(current_node.clone(), receiver.clone(), message);
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

    fn beb_broadcast_write(&self, value: ValueType) {
        let current_node = &self.node_info.current_node;
        let sender: message::ProcessId = current_node.into();

        let mut write_message = message::EpWrite_::new();
        write_message.set_value(value as i32);

        let mut message = message::Message::new();
        message.set_field_type(message::Message_Type::EP_READ);
        message.set_epWrite(write_message);
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
                InternalMessage::BebDeliver(from, msg) => match msg {
                    message::Message {
                        field_type: message::Message_Type::EP_READ,
                        ..
                    } => {
                        if !self.aborted {
                            self.beb_deliver_read(from)
                        }
                    }
                    message::Message {
                        field_type: message::Message_Type::EP_WRITE,
                        ..
                    } => {
                        if !self.aborted {
                            self.beb_deliver_write(from, msg.get_epWrite())
                        }
                    }
                    message::Message {
                        field_type: message::Message_Type::EP_DECIDED,
                        ..
                    } => {
                        if !self.aborted {
                            self.beb_deliver_decided(msg.get_epDecided())
                        }
                    }
                    _ => (),
                },
                InternalMessage::PlDeliver(from, msg) => match msg {
                    message::Message {
                        field_type: message::Message_Type::EP_STATE,
                        ..
                    } => {
                        if !self.aborted {
                            self.pl_deliver_state(from, msg.get_epState());
                        }
                    }
                    message::Message {
                        field_type: message::Message_Type::EP_ACCEPT,
                        ..
                    } => {
                        if !self.aborted {
                            self.pl_deliver_accept()
                        };
                    }
                    _ => (),
                },
                InternalMessage::EpAbort => {
                    if !self.aborted {
                        self.abort()
                    }
                }
                InternalMessage::EpStateCountReached => {
                    if !self.aborted {
                        self.ep_state_count_reached()
                    }
                }
                InternalMessage::EpAcceptedCountReached => {
                    if !self.aborted {
                        self.ep_accepted_count_reached()
                    }
                }
                _ => (),
            },
            EventData::External(_) => (),
        }
    }
}
