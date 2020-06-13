use crate::event::*;
use crate::node::*;
use log::{trace, debug};
use std::sync::Arc;

pub struct EventualLeaderDetector {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    suspected: Vec<Node>,
    leader: Option<Node>,
    system_id: String,
}

impl EventualLeaderDetector {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>, system_id: String) -> Self {
        Self {
            node_info,
            event_queue,
            suspected: Vec::new(),
            leader: None,
            system_id,
        }
    }

    /// upon event ⟨ Ω, Init ⟩ do
    pub fn init(&mut self) {}

    fn on_received_suspect(&mut self, suspect: &Node) {
        debug!("EPFD_SUSPECT: {}", suspect);
        self.suspected.push(suspect.clone());
        self.check_leader();
    }

    fn on_removed_suspect(&mut self, node: &Node) {
        let item_index = self.suspected.iter().position(|o| o == node).unwrap();
        self.suspected.remove(item_index);
        self.check_leader();
    }

    fn check_leader(&mut self) {
        let candidates: Vec<Node> = self
            .node_info
            .nodes
            .iter()
            .filter(|n| !self.suspected.contains(n))
            .cloned()
            .collect();

        let max_by_rank = candidates.iter().max_by(|&x, &y| x.rank.cmp(&y.rank)).cloned();
        if max_by_rank.is_some() {
            self.leader = max_by_rank;
            let message = InternalMessage::EldTrust(self.leader.clone().unwrap());
            self.event_queue
                .push(EventData::Internal(self.system_id.clone(), message));
        }
    }
}

impl EventHandler for EventualLeaderDetector {
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
                InternalMessage::EpfdSuspect(node) => self.on_received_suspect(node),
                InternalMessage::EpfdRestore(node) => self.on_removed_suspect(node),
                _ => (),
            }
        }
    }
}
