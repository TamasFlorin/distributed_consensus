use crate::event::*;
use crate::node::*;
use crate::protos::message;
use log::trace;
use std::sync::Arc;

const ABSTRACTION_ID: &str = "beb";

/// A broadcast abstraction enables a process to send amessage, in a one-shotoperation,
/// to all processes in a system, including itself. We give here the specification and an
/// algorithm for a broadcast communication primitive with a weak form of reliability,
/// called best-effort broadcast.
/// Specification:
/// With best-effort broadcast, the burden of ensuring reliability is only on the sender.
/// Therefore, the remaining processes do not have to be concerned with enforcing
/// the reliability of received messages. On the other hand, no delivery guarantees are
/// offered in case the sender fails. Best-effort broadcast is characterized by the following
/// three properties: validity is a liveness property, whereas
/// the no duplication property and the no creation property are safety properties. They
/// descend directly from the corresponding properties of perfect point-to-point links.
/// Note that broadcast messages are implicitly addressed to all processes. Remember
/// also that messages are unique, that is, no process ever broadcasts the same message
/// twice and furthermore, no two processes ever broadcast the same message.
pub struct BestEffortBroadcast {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    system_id: String,
}

impl BestEffortBroadcast {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>, system_id: String) -> Self {
        BestEffortBroadcast {
            node_info,
            event_queue,
            system_id,
        }
    }

    fn broadcast(&self, message: &message::Message) {
        // send the message to all other nodes
        for node in &self.node_info.nodes {
            self.send(node, message);
        }
    }

    fn send(&self, node: &Node, message: &message::Message) {
        let mut beb_broadcast_message = message::BebBroadcast::new();
        beb_broadcast_message.set_message(message.clone());

        let mut message_data = message::Message::new();
        message_data.set_bebBroadcast(beb_broadcast_message);
        message_data.set_abstractionId(ABSTRACTION_ID.to_owned());
        message_data.set_systemId(self.system_id.clone());

        let from = self.node_info.current_node.clone();
        let internal_message = InternalMessage::PlSend(from, node.clone(), message_data.clone());
        let event_data = EventData::Internal(self.system_id.clone(), internal_message);
        self.event_queue.push(event_data);
    }

    fn deliver(&self, sender: &Node, msg: &message::Message) {
        let internal_message = InternalMessage::BebDeliver(sender.clone(), msg.clone());
        let event_data = EventData::Internal(self.system_id.clone(), internal_message);
        self.event_queue.push(event_data);
    }
}

impl EventHandler for BestEffortBroadcast {
    fn should_handle_event(&self, event_data: &EventData) -> bool {
        if let EventData::Internal(system_id, _) = event_data {
            system_id == &self.system_id   
        } else {
            false
        }
    }

    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        if let EventData::Internal(_, data) = event_data {
            match data {
                InternalMessage::BebBroadcast(msg) => self.broadcast(msg),
                InternalMessage::PlDeliver(sender, msg) => self.deliver(&sender, msg),
                _ => (),
            }
        }
    }
}
