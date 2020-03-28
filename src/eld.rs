use crate::event::*;
use crate::node::*;
use crate::protos::message::*;
use chrono;
use timer::Timer;
use timer::Guard;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::RwLock;

const DELTA: i64 = 5000;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Candidate {
    node: Node,
    epoch: u32,
}

impl Candidate {
    pub fn new(node: Node, epoch: u32) -> Self {
        Candidate {node, epoch}
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

pub struct EventualLeaderDetector {
    epoch: AtomicU32,
    candidates: Arc<RwLock<Vec<Candidate>>>,
    node_info: Arc<NodeInfo>,
    event_queue: Arc<Mutex<EventQueue>>,
    delay: chrono::Duration,
    timer_guard: Option<Guard>,
    leader: Option<Node>,
}

impl EventualLeaderDetector {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<Mutex<EventQueue>>) -> Self {
        Self {
            epoch: AtomicU32::new(0),
            candidates: Arc::new(RwLock::new(Vec::new())),
            node_info: node_info.clone(),
            event_queue,
            delay: chrono::Duration::milliseconds(DELTA),
            timer_guard: None,
            leader: None,
        }
    }

    pub fn init(&mut self, timer: &Timer) {
        // recovery procedure completes the initialization
        self.recovery(timer);
    }

    pub fn recovery(&mut self, timer: &Timer) {
        // select the leader based on the maximum rank
        self.leader = Some(self.maxrank().clone());
        self.trust(&self.leader.as_ref().unwrap());

        self.epoch.store(self.epoch.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        
        // now we need to send a heartbeat to each node from our configuration
        for _ in self.node_info.nodes.iter() {
            // send message to the node.
        }

        self.start_timer(timer);
    }

    fn start_timer(&mut self, timer: &Timer) {
        let queue = Arc::clone(&self.event_queue);
        let from = self.node_info.current_node.id;
        self.timer_guard = Some(timer.schedule_repeating(self.delay, move || {
            // we just need to send the timeout message to ourselvles.
            let message = InternalMessage::Timeout(from);
            let event_data = EventData::Internal(message);
            {
                let queue = queue.lock().unwrap();
                queue.push(event_data);
            }
        }));
    }

    fn timeout(&mut self) {
        let new_leader = self.select();

        // at this point we should already have a leader
        let current_leader = self.leader.as_ref().unwrap();
        if &new_leader.node != current_leader {
            // This guarantees that if leaders keep changing because the timeout delay is too short with
            // respect to communication delays, the delay will continue to increase, until it eventually
            // becomes large enough for the leader to stabilize when the system becomes synchronous.
            self.delay = self.delay + chrono::Duration::milliseconds(DELTA);

            self.leader.replace(new_leader.node);

            // let the others know that we have a new leader
            self.trust(&self.leader.as_ref().unwrap());
        }

        let mut candidates = self.candidates.write().unwrap();
        candidates.clear();
    }

    fn trust(&self, leader: &Node) {
        // at this point we should already have a leader
        let process_id: ProcessId = leader.into();
        let mut trust = EldTrust::new();
        trust.set_processId(process_id);
        let message = InternalMessage::Trust(trust);
        let event_queue = self.event_queue.lock().unwrap();
        event_queue.push(EventData::Internal(message));
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
        let min = candidates.iter().min();
        match min {
            Some(value) => {
                let min_by_epoch: Vec<Candidate> = 
                    candidates.iter().filter(|c| c.epoch == value.epoch).cloned().collect();
                let max_by_rank = min_by_epoch.iter().max().unwrap().clone();
                max_by_rank
            },
            None => Candidate::new(self.node_info.current_node.clone(), self.epoch.load(Ordering::SeqCst)),
        }
    }

    /// the rank of a process is a unique index
    fn maxrank(&self) -> &Node {
        let max_rank_node = self.node_info.nodes.iter().max();
        let current_node = &self.node_info.current_node;
        match max_rank_node {
            Some(node) => if node > current_node {node} else {current_node},
            None => current_node
        }
    }
}

impl EventHandler for EventualLeaderDetector {
    fn handle(&mut self, event_data: &EventData) {
        println!("I am a handler and I have been summoned with event {:?}", event_data);

        match event_data {
            EventData::Internal(msg) => {
                match msg {
                    InternalMessage::Timeout(id) => if id == &self.node_info.current_node.id { self.timeout(); }
                    InternalMessage::Trust(leader) => println!("New leader: {:?}", leader),
                    _ => (),
                }
            },
            EventData::External(_) => (),
        };
    }
}
