use crate::event::*;
use crate::node::*;
use crate::protos::message::*;
use std::rc::Rc;
use std::cell::RefCell;
use chrono;
use timer::Timer;
use timer::Guard;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

const DELTA: i64 = 5000;

pub struct Candidate {
    node_info: NodeInfo,
    epoch: u32,
}

pub struct EventualLeaderDetector {
    epoch: AtomicU32,
    candidates: Arc<Mutex<Vec<Candidate>>>,
    node_info: Arc<NodeInfo>,
    event_queue: Arc<Mutex<EventQueue>>,
    delay: chrono::Duration,
    timer: Timer,
    timer_guard: Option<Guard>,
}

impl EventualLeaderDetector {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<Mutex<EventQueue>>) -> Self {
        Self {
            epoch: AtomicU32::new(0),
            candidates: Arc::new(Mutex::new(Vec::new())),
            node_info: node_info.clone(),
            event_queue,
            delay: chrono::Duration::milliseconds(DELTA),
            timer: Timer::new(),
            timer_guard: None,
        }
    }

    pub fn init(&mut self) {
        // recovery procedure completes the initialization
        self.recovery();
    }

    pub fn recovery(&mut self) {
        // select the leader based on the maximum rank
        let leader = self.maxrank();
        self.trust(leader);

        self.epoch.store(self.epoch.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        
        // now we need to send a heartbeat to each node from our configuration
        for _ in self.node_info.nodes.iter() {
            // send message to the node.
        }

        self.start_timer();
    }

    fn start_timer(&mut self) {
        let queue = Arc::clone(&self.event_queue);
        let from = self.node_info.current_node.clone();
        let to = self.node_info.current_node.clone();
        self.timer_guard = Some(self.timer.schedule_repeating(self.delay, move || {
            // we just need to send the timeout message to ourselvles.
            let internal_message_data = InternalMessageData::new(from.clone(), to.clone());
            let message = InternalMessage::Timeout(internal_message_data);
            let event_data = EventData::Internal(message);
            queue.lock().unwrap().push(event_data);
        }));
    }

    fn trust(&self, leader: &Node) {
        let process_id: ProcessId = leader.into();
        let mut trust = EldTrust::new();
        let message = InternalMessage::Trust(trust);
        self.event_queue.lock().unwrap().push(EventData::Internal(message));
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
    fn handle(&mut self, message: &EventData) {
        println!("I am a handler and I have been summoned with msg {:?}", message);
    }
}
