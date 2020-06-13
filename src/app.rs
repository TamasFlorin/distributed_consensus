use crate::event::*;
use crate::node::*;
use crate::protos::message::*;
use crate::sys::System;
use log::{info, trace};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const ABSTRACTION_ID: &str = "app";

pub struct App {
    current_node: Node,
    hub: Node,
    event_queue: Arc<EventQueue>,
    systems: HashMap<String, System>,
    current_system_id: String,
}

impl App {
    pub fn new(current_node: Node, hub: Node, event_queue: Arc<EventQueue>) -> App {
        App {
            current_node,
            hub,
            event_queue,
            systems: HashMap::new(),
            current_system_id: "sys-1".to_owned(),
        }
    }

    fn init(&mut self) {
        let mut app_register = AppRegistration::new();
        app_register.set_index(self.current_node.id as i32);
        app_register.set_owner(self.current_node.owner.clone());
        
        let uuid = Uuid::new_v4();
        let mut initial_message = Message::new();
        initial_message.set_messageUuid(uuid.to_string());
        initial_message.set_field_type(Message_Type::APP_REGISTRATION);
        initial_message.set_appRegistration(app_register);
        initial_message.set_systemId(self.current_system_id.clone());
        initial_message.set_abstractionId(ABSTRACTION_ID.to_owned());

        let internal_message =
            InternalMessage::PlSend(self.current_node.clone(), self.hub.clone(), initial_message);

        let event_data = EventData::Internal(self.current_system_id.clone(), internal_message);
        self.event_queue.push(event_data);
    }

    fn on_propose(&mut self, msg: &Message) {
        let app_propose = msg.get_appPropose();
        let involved_processes = app_propose.get_processes();
        let maybe_value = app_propose.get_value();

        if maybe_value.get_defined() {
            let involved_nodes: Vec<Node> = involved_processes.iter().map(|p| p.into()).collect();
            let node_info = Arc::new(NodeInfo {
                current_node: self.current_node.clone(),
                hub: self.hub.clone(),
                nodes: involved_nodes,
            });
            let value = maybe_value.get_v() as ValueType;
            let system = System::new(
                msg.get_systemId().to_owned(),
                node_info.clone(),
                self.event_queue.clone(),
                value,
            );

            self.current_system_id = format!("sys-{}", self.systems.len() + 1);
            self.systems.insert(msg.get_systemId().to_owned(), system);
            let proposal = InternalMessage::UcPropose(value);
            self.event_queue
                .push(EventData::Internal(msg.get_systemId().to_owned(), proposal));
        }
    }

    fn on_decide(&mut self, value: &i32, system_id: &String) {
        info!("Decided value {}", value);

        let mut maybe_value = Value::new();
        maybe_value.set_defined(true);
        maybe_value.set_v(*value);

        let mut app_decide = AppDecide::new();
        app_decide.set_value(maybe_value);

        let uuid = Uuid::new_v4();
        let mut msg = Message::new();
        msg.set_messageUuid(uuid.to_string());
        msg.set_field_type(Message_Type::APP_DECIDE);
        msg.set_appDecide(app_decide);
        msg.set_systemId(system_id.clone());
        msg.set_abstractionId(ABSTRACTION_ID.to_owned());

        self.event_queue.push(EventData::Internal(
            system_id.clone(),
            InternalMessage::PlSend(self.current_node.clone(), self.hub.clone(), msg),
        ));
    }
}

impl EventHandler for App {
    fn should_handle_event(&self, _: &EventData) -> bool {
        true
    }

    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        if let EventData::Internal(system_id, data) = event_data {
            match data {
                InternalMessage::AppPropose(_, msg) => self.on_propose(msg), //self.on_propose(),
                InternalMessage::AppInit => self.init(),
                InternalMessage::UcDecide(value) => self.on_decide(value, system_id),
                _ => (),
            }
        }
    }
}
