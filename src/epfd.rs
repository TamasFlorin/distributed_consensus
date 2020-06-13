use crate::event::*;
use crate::node::{Node, NodeInfo};
use crate::protos::message::*;
use chrono;
use log::trace;
use log::{warn};
use std::sync::Arc;
use std::sync::Mutex;
use timer::Guard;
use timer::Timer;
use uuid::Uuid;

const DELTA: i64 = 100;
const ABSTRACTION_ID: &str = "epfd";

pub struct EvenutallyPerfectFailureDetector {
    node_info: Arc<NodeInfo>,
    event_queue: Arc<EventQueue>,
    alive: Vec<Node>,
    suspected: Vec<Node>,
    delay: chrono::Duration,
    timer_guard: Option<Guard>,
    timer: Mutex<Timer>,
    system_id: String,
}

impl EvenutallyPerfectFailureDetector {
    pub fn new(node_info: Arc<NodeInfo>, event_queue: Arc<EventQueue>, system_id: String) -> Self {
        let alive = node_info.nodes.clone();
        EvenutallyPerfectFailureDetector {
            node_info,
            event_queue,
            alive,
            suspected: Vec::new(),
            delay: chrono::Duration::milliseconds(DELTA),
            timer_guard: None,
            timer: Mutex::new(Timer::new()),
            system_id,
        }
    }

    pub fn init(&mut self) {
        self.start_timer();
    }

    fn on_timeout(&mut self) {
        if self.contains_suspected() {
            self.delay = self.delay + chrono::Duration::milliseconds(DELTA);
            let seconds = self.delay.num_seconds();
            let milliseconds = self.delay.num_milliseconds();
            let seconds = if seconds > 0 {seconds} else {milliseconds / 1000};
            warn!("Increased timeout to {} seconds.", seconds);
        }

        for item in self.node_info.nodes.iter() {
            if item.id == self.node_info.current_node.id {
                continue;
            }
            let alive = self.alive.iter().find(|&o| o == item).is_some();
            let suspected = self.suspected.iter().find(|&o| o == item).is_some();
            if !alive && !suspected {
                self.suspected.push(item.clone());
                let msg = InternalMessage::EpfdSuspect(item.clone());
                self.event_queue
                    .push(EventData::Internal(self.system_id.clone(), msg));
            } else if alive && suspected {
                let item_index = self.suspected.iter().position(|o| o == item).unwrap();
                self.suspected.remove(item_index);
                let msg = InternalMessage::EpfdRestore(item.clone());
                self.event_queue
                    .push(EventData::Internal(self.system_id.clone(), msg));
            }

            let heart_message = EpfdHeartbeatRequest_::new();

            let uuid = Uuid::new_v4();
            let mut msg = Message::new();
            msg.set_messageUuid(uuid.to_string());
            msg.set_epfdHeartbeatRequest_(heart_message);
            msg.set_field_type(Message_Type::EPFD_HEARTBEAT_REQUEST);
            msg.set_abstractionId(ABSTRACTION_ID.to_owned());
            msg.set_systemId(self.system_id.clone());

            let from = self.node_info.current_node.clone();
            let internal_msg = InternalMessage::PlSend(from.clone(), item.clone(), msg);
            let event_data = EventData::Internal(self.system_id.clone(), internal_msg);
            self.event_queue.push(event_data);
        }

        self.alive.clear();
        self.start_timer();
    }

    fn send_reply(&mut self, to: &Node) {
        let heart_message = EpfdHeartbeatReply_::new();

        let uuid = Uuid::new_v4();
        let mut msg = Message::new();
        msg.set_messageUuid(uuid.to_string());
        msg.set_epfdHeartbeatReply_(heart_message);
        msg.set_field_type(Message_Type::EPFD_HEARTBEAT_REPLY);
        msg.set_abstractionId(ABSTRACTION_ID.to_owned());
        msg.set_systemId(self.system_id.clone());

        let from = self.node_info.current_node.clone();
        let internal_msg = InternalMessage::PlSend(from.clone(), to.clone(), msg);
        let event_data = EventData::Internal(self.system_id.clone(), internal_msg);
        self.event_queue.push(event_data);
    }

    fn on_got_reply(&mut self, from: &Node) {
        self.alive.push(from.clone());
    }

    fn contains_suspected(&mut self) -> bool {
        for item in self.alive.iter() {
            let other = self.suspected.iter().find(|&o| o == item);
            if other.is_some() {
                return true;
            }
        }
        false
    }

    fn start_timer(&mut self) {
        let event_queue = Arc::clone(&self.event_queue);
        let system_id = self.system_id.clone();
        self.timer_guard = Some(self.timer.lock().unwrap().schedule_with_delay(
            self.delay,
            move || {
                // we just need to send the timeout message to ourselvles.
                let message = InternalMessage::EpfdTimeout;
                let event_data = EventData::Internal(system_id.clone(), message);
                event_queue.push(event_data);
            },
        ));
    }
}

impl EventHandler for EvenutallyPerfectFailureDetector {
    fn should_handle_event(&self, event_data: &EventData) -> bool {
        if let EventData::Internal(system_id, _) = event_data {
            system_id == &self.system_id   
        } else {
            false
        }
    }

    fn handle(&mut self, event_data: &EventData) {
        trace!("Handler summoned with event {:?}", event_data);

        match event_data {
            EventData::Internal(_, message) => match message {
                InternalMessage::EpfdTimeout => self.on_timeout(),
                InternalMessage::PlDeliver(from, msg) => {
                    if let Message {
                        field_type: Message_Type::EPFD_HEARTBEAT_REQUEST,
                        ..
                    } = msg
                    {
                        self.send_reply(from);
                    }
                    if let Message {
                        field_type: Message_Type::EPFD_HEARTBEAT_REPLY,
                        ..
                    } = msg
                    {
                        self.on_got_reply(from);
                    }
                }
                _ => (),
            },
            _ => (),
        }
    }
}
