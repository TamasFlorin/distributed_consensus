use crate::node::{Node, NodeId};
use crate::protos::message::{EldTrust, Message};
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub trait EventHandler {
    fn handle(&mut self, message: &EventData);
}

#[derive(Debug, Clone)]
pub enum InternalMessage {
    Timeout(NodeId),
    Trust(Node),
    Send(Node, Node, Message), //(from, to, msg)
    Sent(Node, Node, Message), // (from, to, msg)
    Broadcast(Message),
    Deliver(Node, Node, Message),
    Nack(Node, Node), // (from, to)
}

#[derive(Debug, Clone)]
pub enum EventData {
    Internal(InternalMessage),
    External(Message),
}

pub struct EventQueue {
    handlers: Arc<Mutex<Vec<Box<dyn EventHandler + Send>>>>,
    queue: Arc<Mutex<VecDeque<EventData>>>,
    cvar: Arc<Condvar>,
    is_running: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    element_added: Arc<Mutex<bool>>,
}

impl Default for EventQueue {
    fn default() -> Self {
        EventQueue {
            handlers: Arc::new(Mutex::new(Vec::new())),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            cvar: Arc::new(Condvar::default()),
            is_running: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
            element_added: Arc::new(Mutex::new(false)),
        }
    }
}

impl EventQueue {
    pub fn push(&self, event_data: EventData) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(event_data);
        let mut guard = self.element_added.lock().unwrap();
        *guard = true;
        self.cvar.notify_one();
    }

    pub fn run(&mut self) {
        if self.is_running.load(Ordering::SeqCst) {
            panic!("Event queue is already running.");
        }

        let handlers = Arc::clone(&self.handlers);
        let cvar = Arc::clone(&self.cvar);
        let queue = Arc::clone(&self.queue);
        let is_running = Arc::clone(&self.is_running);
        let element_added = Arc::clone(&self.element_added);
        self.handle = Mutex::new(Some(thread::spawn(move || {
            is_running.store(true, Ordering::SeqCst);

            loop {
                let mut q = queue.lock().unwrap();
                let mut queue_items: VecDeque<EventData> = q.iter().cloned().collect();
                q.clear();
                std::mem::drop(q);

                // We need to parse a copy of the original items since our event handlers
                // might in turn use the event queue to send other messages.
                // This means that we cannot hold a lock on the queue here.
                while !queue_items.is_empty() {
                    let first = queue_items.pop_front().unwrap();

                    // we are sending the message to everyone for now...
                    // they will need to filter it themselvles.
                    let mut guard = handlers.lock().unwrap();
                    let handlers = guard.deref_mut();
                    for event_handler in handlers.iter_mut() {
                        event_handler.handle(&first);
                    }
                }

                if !is_running.load(Ordering::SeqCst) {
                    break;
                }

                let guard = element_added.lock().unwrap();
                let mut guard = cvar
                    .wait_while(guard, |added| !*added && is_running.load(Ordering::SeqCst))
                    .unwrap();

                // we woke up because there was some work to do, now we can reset the work variable to false
                // since we are going to do the work that we were woken up about.
                *guard = false;
            }
        })));
    }

    fn close(&mut self) {
        let mut handle = self.handle.lock().unwrap();
        if handle.is_some() {
            self.is_running.store(false, Ordering::SeqCst);
            let _ = self.element_added.lock().unwrap();
            self.cvar.notify_one();
            let _ = handle.take().unwrap().join();
        } else {
            panic!("The queue has been already closed");
        }
    }

    pub fn register_handler(&mut self, event_handler: Box<dyn EventHandler + Send>) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(event_handler);
    }
}

impl Drop for EventQueue {
    fn drop(&mut self) {
        self.close();
    }
}
