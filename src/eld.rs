use crate::event;
use crate::node::{Node, Nodes};
use std::rc::Rc;

pub trait EventualLeaderDetector {}

pub struct ElectLowerEpoch {
    epoch: u32,
    candidates: Vec<i32>,
    nodes: Rc<Nodes>,
}

impl ElectLowerEpoch {
    pub(crate) fn new(nodes: Rc<Nodes>) -> Self {
        Self {
            epoch: 0,
            candidates: Vec::new(),
            nodes: nodes.clone(),
        }
    }

    pub fn recovery() {}

    /// the rank of a process is a unique index
    fn maxrank() {}
}

impl EventualLeaderDetector for ElectLowerEpoch {}
