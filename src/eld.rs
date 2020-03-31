use crate::event::*;
use crate::node::*;
use crate::protos::message::*;
use crate::storage::Storage;
use chrono;
use log::{info, trace};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use timer::Guard;
use timer::Timer;

const DELTA: i64 = 5000;
const INITIAL_EPOCH: u32 = 0;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Candidate {
    node: Node,
    epoch: u32,
}

impl Candidate {
    pub fn new(node: Node, epoch: u32) -> Self {
        Candidate { node, epoch }
    }
}

impl PartialOrd<Candidate> for Candidate {
    fn partial_cmp(&self, other: &Candidate) -> Option<std::cmp::Ordering> {
        self.epoch.partial_cmp(&other.epoch)
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.epoch.cmp(&other.epoch)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EldState /*<S: Storage<Self>>*/ {
    epoch: u32,
}

impl EldState {
    pub fn new(epoch: u32) -> Self {
        EldState { epoch }
    }
}

pub struct EventualLeaderDetector<S: Storage<EldState>> {
    candidates: Arc<RwLock<Vec<Candidate>>>,
    epoch: u32,
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    delay: chrono::Duration,
    timer_guard: Option<Guard>,
    timer: Timer,
    leader: Option<Node>,
    storage: Mutex<S>,
}

impl<S: Storage<EldState>> EventualLeaderDetector<S> {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>, storage: S) -> Self {
        Self {
            candidates: Arc::new(RwLock::new(Vec::new())),
            epoch: 0,
            node_info,
            event_queue,
            delay: chrono::Duration::milliseconds(DELTA),
            timer_guard: None,
            timer: Timer::new(),
            leader: None,
            storage: Mutex::new(storage),
        }
    }

    pub fn init(&mut self) {
        // recovery procedure completes the initialization
        self.recovery();
    }

    pub fn recovery(&mut self) {
        // select the leader based on the maximum rank
        self.leader = Some(self.maxrank().clone());
        self.trust(&self.leader.as_ref().unwrap());

        // Update our epoch number (this represents the number of times that we have crashed).
        // This value will be used when selecting the leader node.
        self.epoch = self.update_epoch();

        // now we need to send a heartbeat to each node from our configuration
        self.node_info
            .nodes
            .iter()
            .for_each(|node| self.send_heartbeat(node));

        self.start_timer();
    }

    fn update_epoch(&self) -> u32 {
        let storage = self.storage.lock().unwrap();
        match storage.read() {
            Ok(state) => {
                let new_epoch = state.epoch + 1;
                let new_state = EldState::new(new_epoch);
                storage
                    .write(&new_state)
                    .expect("Epoch should be updated on storage.");
                new_epoch
            }
            Err(_) => {
                let state = EldState::new(INITIAL_EPOCH);
                storage
                    .write(&state)
                    .expect("Epoch should be updated on storage.");
                INITIAL_EPOCH
            }
        }
    }

    fn start_timer(&mut self) {
        let event_queue = Arc::clone(&self.event_queue);

        // TODO: add support for chaning the delay here...
        self.timer_guard = Some(self.timer.schedule_with_delay(self.delay, move || {
            // we just need to send the timeout message to ourselvles.
            let message = InternalMessage::EldTimeout;
            let event_data = EventData::Internal(message);
            event_queue.push(event_data);
        }));
    }

    fn timeout(&mut self) {
        let new_leader = self.select();
        let current_leader = self
            .leader
            .as_ref()
            .expect("We should already have a leader.");
        if &new_leader.node != current_leader {
            // Changing the delay guarantees that if leaders keep changing because the timeout delay is too short with
            // respect to communication delays, the delay will continue to increase, until it eventually
            // becomes large enough for the leader to stabilize when the system becomes synchronous.
            self.delay = self.delay + chrono::Duration::milliseconds(DELTA);

            self.leader.replace(new_leader.node);

            // let the others know that we have a new leader
            self.trust(&self.leader.as_ref().unwrap());
        }

        // now we need to send a heartbeat to each node from our configuration
        self.node_info
            .nodes
            .iter()
            .for_each(|node| self.send_heartbeat(node));

        {
            let mut candidates = self.candidates.write().unwrap();
            candidates.clear();
        }

        self.start_timer();
    }

    fn send_heartbeat(&self, node: &Node) {
        let current_node = &self.node_info.current_node;
        let process: ProcessId = current_node.into();
        let mut heartbeat = EldHeartbeat_::new();
        heartbeat.set_epoch(self.epoch as i32);
        heartbeat.set_from(process);
        let mut message = Message::new();
        message.set_eldHeartbeat(heartbeat);
        message.set_field_type(Message_Type::ELD_HEARTBEAT);
        let from = self.node_info.current_node.clone();
        let internal_msg = InternalMessage::PlSend(from, node.clone(), message);
        let event_data = EventData::Internal(internal_msg);
        self.event_queue.push(event_data);
    }

    fn recv_heartbeat(&self, heartbeat: &EldHeartbeat_) {
        let process = heartbeat.get_from();
        let index = process.get_index() as u16;
        let epoch = heartbeat.get_epoch() as u32;

        // TODO: Figure out if we should just ignore the message or panic
        let node = self
            .node_info
            .nodes
            .iter()
            .find(|node| node.id == index)
            .expect("Message must be from one of the known nodes.");

        let mut candidates = self.candidates.write().unwrap();
        let mut candidate = candidates
            .iter_mut()
            .find(|c| c.node.id == node.id && c.epoch < epoch);

        match candidate.as_deref_mut() {
            Some(value) => value.epoch = epoch,
            _ => candidates.push(Candidate::new(node.clone(), epoch)),
        }
    }

    fn trust(&self, leader: &Node) {
        // at this point we should already have a leader
        let message = InternalMessage::EldTrust(leader.clone());
        self.event_queue.push(EventData::Internal(message));
    }

    /// This method picks one process from candidates according to the following rule:
    /// it considers the process/epoch pairs in candidates with the lowest epoch number,
    /// selects the corresponding processes, and returns the process with the highest rank
    /// among them. This choice guarantees that when a process p is elected leader, but
    /// keeps on crashing and recovering forever, then p will eventually be replaced by a
    /// correct process. By definition, the epoch number of a correct process will eventually
    /// stop growing.
    fn select(&self) -> Candidate {
        let candidates = self.candidates.read().unwrap();
        let min = candidates
            .iter()
            .min()
            .expect("We should have at least one candidate.");
        let min_by_epoch: Vec<Candidate> = candidates
            .iter()
            .filter(|c| c.epoch == min.epoch)
            .cloned()
            .collect();
        let max_by_rank = min_by_epoch.iter().max().unwrap().clone();
        max_by_rank
    }

    /// the rank of a process is a unique index
    fn maxrank(&self) -> &Node {
        let max_rank_node = self
            .node_info
            .nodes
            .iter()
            .max()
            .expect("We should have at least one node.");
        max_rank_node
    }
}

impl<S: Storage<EldState>> EventHandler for EventualLeaderDetector<S> {
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        match event_data {
            EventData::Internal(msg) => match msg {
                InternalMessage::EldTimeout => {
                    self.timeout();
                }
                InternalMessage::EldTrust(leader) => {
                    info!("A new leader has been set: {:?}", leader)
                }
                InternalMessage::PlDeliver(_, msg) => {
                    match msg {
                        Message {
                            field_type: Message_Type::ELD_HEARTBEAT,
                            ..
                        } => {
                            self.recv_heartbeat(msg.get_eldHeartbeat());
                        },
                        _ => ()
                    }
                }
                _ => (),
            },
            _ => (),
        }
    }
}
