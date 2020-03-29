use crate::event::*;
use crate::node::Node;
use crate::protos::message;
use protobuf::Message;
use std::error::Error;
use std::net::{SocketAddr, TcpStream};
use std::sync;

pub struct PerfectLink {
    event_queue: sync::Arc<sync::Mutex<EventQueue>>,
}

impl PerfectLink {
    pub fn new(event_queue: sync::Arc<sync::Mutex<EventQueue>>) -> Self {
        PerfectLink {
            event_queue,
        }
    }

    fn send(&self, from: &Node,  dest: &Node, message: &message::Message) -> Result<(), Box<dyn Error>> {
        let address_to: SocketAddr = dest.into();
        let mut stream = TcpStream::connect(address_to)?;
        message.write_to_writer(&mut stream)?;

        // let the sender know that the message was delivered successfully
        self.notify_sender(from, dest, message);

        Ok(())
    }

    fn notify_sender(&self, sender: &Node, recv: &Node, message: &message::Message) {
        let sent_message = InternalMessage::Sent(sender.clone(), recv.clone(), message.clone());
        let event_data = EventData::Internal(sent_message);
        let queue = self.event_queue.lock().unwrap();
        queue.push(event_data);
    }
}

impl EventHandler for PerfectLink {
    fn handle(&mut self, message: &EventData) {
        match message {
            EventData::External(_) => (),
            EventData::Internal(msg) => {
                match msg {
                    InternalMessage::Send(from, dest, data) => {
                        let _ = self.send(from, dest, data);
                    }
                    InternalMessage::Nack(from, dest) => {
                        let nack = message::EcNack_::new();
                        let mut msg = message::Message::new();
                        msg.set_ecNack(nack);
                        msg.set_field_type(message::Message_Type::EC_NACK);
                        let _ = self.send(from, dest, &msg);
                    }
                    _ => (),
                };
            }
        };
    }
}
