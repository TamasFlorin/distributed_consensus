use crate::event::*;
use crate::node::Node;
use crate::protos::message;
use protobuf::Message;
use std::error::Error;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};

pub struct PerfectLink {}

impl Default for PerfectLink {
    fn default() -> Self {
        PerfectLink {}
    }
}

impl PerfectLink {
    pub fn new() -> Self {
        PerfectLink {}
    }

    fn send(&self, dest: &Node, message: &message::Message) -> Result<(), Box<dyn Error>> {
        println!("Sending message {:?} to {:?}", message, dest);
        let address_to: SocketAddr = dest.into();
        let mut stream = TcpStream::connect(address_to)?;
        let bytes = message.write_to_bytes()?;
        let _ = stream.write(&bytes)?;
        Ok(())
    }
}

impl EventHandler for PerfectLink {
    fn handle(&mut self, message: &EventData) {
        match message {
            EventData::External(_) => (),
            EventData::Internal(msg) => {
                match msg {
                    InternalMessage::Send(dest, data) => {
                        let _ = self.send(dest, data);
                    }
                    _ => (),
                };
            }
        };
    }
}
