use crate::event::*;
use crate::node::*;
use std::rc::Rc;

pub trait EventualLeaderDetector {}

pub struct ElectLowerEpoch {
    epoch: u32,
    candidates: Vec<Node>,
    node_info: Rc<NodeInfo>,
    delay: i32,
}

impl ElectLowerEpoch {
    pub fn new(node_info: Rc<NodeInfo>) -> Self {
        Self {
            epoch: 0,
            candidates: Vec::new(),
            node_info: node_info.clone(),
            delay: 0,
        }
    }

    pub fn init(&mut self) {
        // recovery procedure completes the initialization
        self.recovery();
    }

    pub fn recovery(&mut self) {
        let leader = self.maxrank();
        // trigger event trust for leader
        let delay = 0;
        self.epoch += 1;
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

impl EventualLeaderDetector for ElectLowerEpoch {}
