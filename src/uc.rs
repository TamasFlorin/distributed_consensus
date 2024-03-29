use crate::ep;
use crate::ep::EpochConsensusState;
use crate::event::*;
use crate::node::{Node, NodeInfo};
use log::{trace};
use std::sync::Arc;

pub struct UniformConsensusState {
    pub epoch_timestamp: u32,
    pub leader: Option<Node>,
}

impl UniformConsensusState {
    fn new(epoch_timestamp: u32, leader: Option<Node>) -> Self {
        UniformConsensusState {
            epoch_timestamp,
            leader,
        }
    }
}

pub struct UniformConsensus {
    event_queue: Arc<EventQueue>,
    node_info: Arc<NodeInfo>,
    value: Option<ValueType>,
    proposed: bool,
    decided: bool,
    state: UniformConsensusState,
    new_state: UniformConsensusState,
    system_id: String,
    ep_index: usize,
}

impl UniformConsensus {
    pub fn new(
        event_queue: Arc<EventQueue>,
        node_info: Arc<NodeInfo>,
        initial_leader: Node,
        system_id: String,
    ) -> Self {
        UniformConsensus {
            event_queue,
            node_info,
            value: None,
            proposed: false,
            decided: false,
            state: UniformConsensusState::new(0, Some(initial_leader)),
            new_state: UniformConsensusState::new(0, None),
            system_id,
            ep_index: 0,
        }
    }

    /// upon event ⟨ uc, Init ⟩ do
    pub fn init(&self) {}

    /// upon event ⟨ uc, Propose | v ⟩ do
    fn uc_propose(&mut self, value: ValueType) {
        // val := v;
        self.value.replace(value);
    }

    /// upon event ⟨ ec, StartEpoch | newts', newl' ⟩ do
    fn ec_start_epoch(&mut self, leader: &Node, timestamp: u32) {
        // (newts, newl) := (newts', newl');
        self.new_state.epoch_timestamp = timestamp;
        self.new_state.leader.replace(leader.clone());

        // trigger ⟨ ep.ets, Abort ⟩;
        let ets = self.state.epoch_timestamp;
        let abort_mesasge = InternalMessage::EpAbort(ets);
        let event_data = EventData::Internal(self.system_id.clone(), abort_mesasge);
        self.event_queue.push(event_data);
    }

    /// upon event ⟨ ep.ts, Aborted | state ⟩ such that ts = ets do
    fn ep_aborted(&mut self, epoch_ts: u32, ts: u32, value: ValueType) {
        if self.state.epoch_timestamp == epoch_ts {
            // (ets, l) := (newts, newl);
            self.state.epoch_timestamp = self.new_state.epoch_timestamp;
            self.state.leader = self.new_state.leader.clone();

            // proposed := FALSE;
            self.proposed = false;

            // Initialize a new instance ep.ets of epoch consensus with timestamp ets, leader l, and state state;
            let state = EpochConsensusState::new(ts, value);
            let leader = self
                .state
                .leader
                .clone()
                .expect("We should have a leader at this point.");
            
            self.ep_index += 1;
            let ep = ep::EpochConsensus::new(
                self.node_info.clone(),
                self.event_queue.clone(),
                state,
                leader,
                self.state.epoch_timestamp,
                self.system_id.clone(),
                self.ep_index,
            );
            self.event_queue
                .register_handler( Box::new(ep));
        }
    }

    /// upon l = self && val != None && proposed = FALSE do
    fn change_proposed(&mut self) {
        let leader = self
            .state
            .leader
            .as_ref()
            .expect("We should have a leader at this point.");
        if leader == &self.node_info.current_node && self.value.is_some() {
            self.proposed = true;
            let propose_message =
                InternalMessage::EpPropose(self.state.epoch_timestamp, self.value.unwrap());
            let event_data = EventData::Internal(self.system_id.clone(), propose_message);
            self.event_queue.push(event_data);
        }
    }

    /// upon event ⟨ ep.ts, Decide | v ⟩ such that ts = ets do
    fn ep_decide(&mut self, ts: u32, value: ValueType) {
        if !self.decided && self.state.epoch_timestamp == ts {
            self.decided = true;
            let decide_message = InternalMessage::UcDecide(value);
            let event_data = EventData::Internal(self.system_id.clone(), decide_message);
            self.event_queue.push(event_data);
        }
    }
}

impl EventHandler for UniformConsensus {
    fn should_handle_event(&self, event_data: &EventData) -> bool {
        if let EventData::Internal(system_id, _) = event_data {
            system_id == &self.system_id   
        } else {
            false
        }
    }

    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        if let EventData::Internal(_, msg) = event_data {
            match msg {
                InternalMessage::UcPropose(value) => {
                    self.uc_propose(value.clone());
                    // we need to call this here since this is the point where the value changes
                    self.change_proposed();
                }
                InternalMessage::EcStartEpoch(leader, new_timestamp) => {
                    self.ec_start_epoch(leader, *new_timestamp);

                    // we need to call this here since this is the point where the value changes
                    self.change_proposed();
                }
                InternalMessage::EpAborted(e_ts, ts, value) => {
                    self.ep_aborted(*e_ts, *ts, *value);

                    // we need to call this here since this is where the current leader might change.
                    self.change_proposed();
                }
                InternalMessage::EpDecide(ts, value) => self.ep_decide(*ts, *value),
                _ => (),
            }
        }
    }
}
