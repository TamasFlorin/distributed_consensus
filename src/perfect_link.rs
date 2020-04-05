use crate::event::*;
use crate::node::{Node, NodeId, NodeInfo};
use crate::protos::message;
use log::trace;
use protobuf::Message;
use std::error::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

pub struct PerfectLink {
    event_queue: Arc<EventQueue>,
    node_info: Arc<NodeInfo>,
}

impl PerfectLink {
    pub fn new(event_queue: Arc<EventQueue>, node_info: Arc<NodeInfo>) -> Self {
        PerfectLink {
            event_queue,
            node_info,
        }
    }

    fn send(
        &self,
        _: &Node,
        dest: &Node,
        message: &message::Message,
    ) -> Result<(), Box<dyn Error>> {
        let address_to: SocketAddr = dest.into();
        let mut stream = TcpStream::connect(address_to)?;
        message.write_to_writer(&mut stream)?;
        Ok(())
    }

    fn deliver(&self, msg: &message::Message) {
        let sender = msg
            .sender
            .as_ref()
            .clone()
            .expect("Sender should be set on a message.");
        let from = self
            .node_info
            .nodes
            .iter()
            .find(|n| n.id == sender.index as NodeId)
            .expect("Message should be delivered from a known node.");
        let internal_message = InternalMessage::PlDeliver(from.clone(), msg.clone());
        let event_data = EventData::Internal(internal_message);
        self.event_queue.push(event_data);
    }
}

impl EventHandler for PerfectLink {
    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);
        match event_data {
            EventData::External(msg) => {
                self.deliver(msg);
            }
            EventData::Internal(msg) => {
                if let InternalMessage::PlSend(from, dest, data) = msg {
                    let _ = self.send(from, dest, data);
                }
            }
        };
    }
}
