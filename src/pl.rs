use crate::event::*;
use crate::node::{Node, NodeInfo};
use crate::protos::message;
use log::{trace, error, info};
use protobuf::Message;
use std::error::Error;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use uuid::Uuid;

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
       
        let bytes = message.write_to_bytes().unwrap();
        let length = bytes.len() as i32;
        let mut msg_as_bytes = length.to_be_bytes().to_vec();
        msg_as_bytes.extend(bytes);
        
        let _ = stream
            .write(&msg_as_bytes[..])
            .expect("The message should be sent successsfully.");

        Ok(())
    }

    fn deliver(&self, msg: &message::Message) {
        let network_message = msg.get_networkMessage();
        let sender: Option<&Node> = self.node_info.nodes.iter().find(|&node| {
            node.port as i32 == network_message.get_senderListeningPort()
                && node.host == network_message.get_senderHost()
        });
        if !sender.is_some() {
            error!(
                "PerfectLink received message from unknown node {:?}. Ignoring message.",
                network_message
            );
        } else {
            let sender = sender.unwrap().clone();
            let mut actual_message = network_message.get_message().clone();
            actual_message.set_systemId(msg.get_systemId().to_owned());

            if let message::Message {
                field_type: message::Message_Type::APP_PROPOSE,
                ..
            } = actual_message
            {
                let internal_message = InternalMessage::AppPropose(sender, actual_message);
                let event_data =
                    EventData::Internal(msg.get_systemId().to_owned(), internal_message);
                self.event_queue.push(event_data);
            } else {
                let internal_message = InternalMessage::PlDeliver(sender, actual_message);
                let event_data =
                    EventData::Internal(msg.get_systemId().to_owned(), internal_message);
                self.event_queue.push(event_data);
            }
        }
    }
}

impl EventHandler for PerfectLink {
    fn should_handle_event(&self, _: &EventData) -> bool {
        true
    }

    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);
        match event_data {
            EventData::External(_, msg) => {
                info!("Received msg: {:?}", msg);
                if let message::Message {
                    field_type: message::Message_Type::NETWORK_MESSAGE,
                    ..
                } = msg
                {
                    self.deliver(msg);
                } else {
                    error!("PerectLink received unexpected message type");
                }
            },
            EventData::Internal(_, msg) => {
                if let InternalMessage::PlSend(from, dest, data) = msg {
                    let mut network_message = message::NetworkMessage::new();
                    if let message::Message{field_type: message::Message_Type::BEB_BROADCAST, ..} = data {
                        let beb_message = data.get_bebBroadcast();
                        let actual_message = beb_message.get_message();
                        network_message.set_message(actual_message.clone());
                    } else {
                        network_message.set_message(data.clone());
                    }

                    network_message.set_senderHost(from.host.clone());
                    network_message.set_senderListeningPort(from.port as i32);

                    let mut external_msg = message::Message::new();
                    external_msg.set_field_type(message::Message_Type::NETWORK_MESSAGE);
                    external_msg.set_networkMessage(network_message);

                    let uuid = Uuid::new_v4();
                    external_msg.set_messageUuid(uuid.to_string());
                    external_msg.set_systemId(data.get_systemId().to_owned());
                    external_msg.set_abstractionId(data.get_abstractionId().to_owned());
                    trace!("Sending message {:?}", external_msg.clone());
                    let _ = self.send(from, dest, &external_msg);
                }
            }
        };
    }
}
