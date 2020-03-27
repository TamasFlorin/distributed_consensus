pub trait EventualLeaderDetector {}

pub struct ElectLowerEpoch {
    epoch: u32,
    candidates: Vec<i32>,
}

impl ElectLowerEpoch {
    pub(crate) fn new() -> Self {
        Self {
            epoch: 0,
            candidates: Vec::new(),
        }
    }

    pub fn recovery() {}

    /// the rank of a process is a unique index
    fn maxrank() {}
}

impl EventualLeaderDetector for ElectLowerEpoch {}
