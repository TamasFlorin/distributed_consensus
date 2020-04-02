use crate::event::*;
use crate::node::Node;
use crate::protos::message;
use protobuf::Message;
use std::error::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use log::trace;

pub struct PerfectLink {
    event_queue: Arc<EventQueue>,
}

impl PerfectLink {
    pub fn new(event_queue: Arc<EventQueue>) -> Self {
        PerfectLink { event_queue }
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
        let from: Node = msg.get_sender().into();
        let internal_message = InternalMessage::PlDeliver(from, msg.clone());
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
                match msg {
                    InternalMessage::PlSend(from, dest, data) => {
                        let _ = self.send(from, dest, data);
                    }
                    _ => (),
                };
            }
        };
    }
}
