use crate::beb::BestEffortBroadcast;
use crate::ec::EpochChange;
use crate::eld::EventualLeaderDetector;
use crate::ep::{EpochConsensus, EpochConsensusState};
use crate::epfd::EvenutallyPerfectFailureDetector;
use crate::event::EventQueue;
use crate::event::ValueType;
use crate::node::NodeInfo;
use crate::uc::UniformConsensus;
use std::sync::Arc;

pub struct System {
    pub system_id: String,
}

impl System {
    pub fn new(
        system_id: String,
        node_info: Arc<NodeInfo>,
        event_queue: Arc<EventQueue>,
        _: ValueType,
    ) -> Self {
        let mut epfd = EvenutallyPerfectFailureDetector::new(
            node_info.clone(),
            event_queue.clone(),
            system_id.clone(),
        );
        let mut eld =
            EventualLeaderDetector::new(node_info.clone(), event_queue.clone(), system_id.clone());
        let beb =
            BestEffortBroadcast::new(node_info.clone(), event_queue.clone(), system_id.clone());
        let ec = EpochChange::new(node_info.clone(), event_queue.clone(), system_id.clone());
        let ep = EpochConsensus::new(
            node_info.clone(),
            event_queue.clone(),
            EpochConsensusState::new(0, 0),
            ec.trusted.clone(),
            0,
            system_id.clone(),
            0,
        );
        let uc = UniformConsensus::new(
            event_queue.clone(),
            node_info.clone(),
            ec.trusted.clone(),
            system_id.clone(),
        );

        epfd.init();
        eld.init();
        uc.init();

        event_queue.register_handler(Box::new(epfd));
        event_queue.register_handler(Box::new(eld));
        event_queue.register_handler(Box::new(beb));
        event_queue.register_handler(Box::new(ec));
        event_queue.register_handler(Box::new(ep));
        event_queue.register_handler(Box::new(uc));

        System { system_id }
    }
}
