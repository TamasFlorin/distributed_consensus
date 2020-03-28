use crate::protos::message::{Message, EldTrust};
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use crate::node::NodeId;
//type EventHandler = dyn Fn(Message);

pub trait EventHandler {
    fn handle(&mut self, message: &EventData);
}

#[derive(Debug, Clone)]
pub enum InternalMessage {
    Timeout(NodeId),
    Recovery(NodeId),
    Trust(EldTrust),
}

#[derive(Debug, Clone)]
pub enum EventData {
    Internal(InternalMessage),
    External(Message)
}

pub struct EventQueue<> {
    handlers: Arc<Mutex<Vec<Box<dyn EventHandler + Send + Sync>>>>,
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
    pub fn push(&self, message: EventData) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message);
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
                // unlock the queue since we are done with it
                q.clear();
                std::mem::drop(q);

                while !queue_items.is_empty() {
                    let first = queue_items.pop_front().unwrap();
                    println!("Processing message {:?}", first);

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

                {
                    // sleep until we get some other work to do
                    let guard = element_added.lock().unwrap();
                    let mut guard = cvar.wait_while(guard, |added| !*added && is_running.load(Ordering::SeqCst)).unwrap();

                    // we woke up because there was some work to do...now we can reset that
                    *guard = false;
                }
            }
        })));
    }

    pub fn close(&mut self) {
        let mut handle = self.handle.lock().unwrap();
        if handle.is_some() {
            self.is_running.store(false, Ordering::SeqCst);
            let _ = self.element_added.lock().unwrap();
            self.cvar.notify_one();
            let _ = handle.take().unwrap().join();
        }
    }

    pub fn register_handler(&mut self, event_handler: Box<dyn EventHandler + Send + Sync>) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(event_handler);
    }
}
