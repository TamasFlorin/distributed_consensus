use crate::node::Node;
use crate::protos::message::Message;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub type ValueType = i32;

pub trait EventHandler {
    fn handle(&mut self, message: &EventData);
}

#[derive(Debug, Clone)]
pub enum InternalMessage {
    EldTimeout,
    EldTrust(Node),
    BebBroadcast(Message),
    BebDeliver(Node, Message),
    EcNack(Node, Node),      // (from, to)
    EcStartEpoch(Node, u32), //(leader, epoch_timestamp)
    EcInitialLeader(Node),
    EpPropose(u32, ValueType), // (timestamp, value)
    EpDecided(ValueType),
    EpDecide(u32, ValueType),
    EpStateCountReached,
    EpAcceptedCountReached,
    EpAbort(u32), // timestamp
    EpAborted(u32, ValueType),
    UcPropose(ValueType),
    UcDecide(ValueType),
    PlSend(Node, Node, Message), //(from, to, msg)
    PlDeliver(Node, Message),    // (from, msg)
}

#[derive(Debug, Clone)]
pub enum EventData {
    Internal(InternalMessage),
    External(Message),
}

type EventHandlerType = Box<dyn EventHandler + Send>;
type EventHandlerCollection = Vec<Mutex<EventHandlerType>>;
type SafeEventHandlerCollection = Mutex<EventHandlerCollection>;

pub struct EventQueue {
    handlers: Arc<SafeEventHandlerCollection>,
    new_handlers: Arc<SafeEventHandlerCollection>,
    queue: Arc<Mutex<VecDeque<EventData>>>,
    cvar: Arc<Condvar>,
    is_running: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    element_added: Arc<Mutex<bool>>,
}

impl EventQueue {
    pub fn create_and_run() -> Self {
        // We need the mutex for the condition variable.
        #[allow(clippy::mutex_atomic)]
        let mut event_queue = EventQueue {
            handlers: Arc::new(Mutex::new(Vec::new())),
            new_handlers: Arc::new(Mutex::new(Vec::new())),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            cvar: Arc::new(Condvar::default()),
            is_running: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
            element_added: Arc::new(Mutex::new(false)),
        };
        event_queue.run();
        event_queue
    }

    pub fn push(&self, event_data: EventData) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(event_data);
        let mut guard = self.element_added.lock().unwrap();
        *guard = true;
        self.cvar.notify_one();
    }

    fn run(&mut self) {
        if self.is_running.load(Ordering::SeqCst) {
            panic!("Event queue is already running.");
        }

        let handlers = Arc::clone(&self.handlers);
        let cvar = Arc::clone(&self.cvar);
        let queue = Arc::clone(&self.queue);
        let is_running = Arc::clone(&self.is_running);
        let element_added = Arc::clone(&self.element_added);
        let new_event_handlers = self.new_handlers.clone();
        self.handle = Mutex::new(Some(thread::spawn(move || {
            is_running.store(true, Ordering::SeqCst);

            loop {
                let mut q = queue.lock().unwrap();
                let mut queue_items: VecDeque<EventData> = q.iter().cloned().collect();
                q.clear();
                std::mem::drop(q);

                // handle the case where a certain event handler's 'handle' method was called
                // and it uses the 'EventQueue' to call 'register_handler'
                let mut current_handlers = handlers.lock().unwrap();
                {
                    let mut event_handlers = new_event_handlers.lock().unwrap();
                    while let Some(handler) = event_handlers.pop() {
                        current_handlers.push(handler);
                    }
                }

                // We need to parse a copy of the original items since our event handlers
                // might in turn use the event queue to send other messages.
                // This means that we cannot hold a lock on the queue here.
                while !queue_items.is_empty() {
                    let first = queue_items.pop_front().unwrap();

                    // we are sending the message to everyone for now...
                    // they will need to filter it themselvles.
                    for event_handler in current_handlers.iter() {
                        let mut event_handler_guard = event_handler.lock().unwrap();
                        event_handler_guard.handle(&first);
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

    pub fn register_handler(&self, event_handler: Box<dyn EventHandler + Send>) {
        let mut handlers = self.new_handlers.lock().unwrap();
        handlers.push(Mutex::new(event_handler));
    }
}

impl Drop for EventQueue {
    fn drop(&mut self) {
        self.close();
    }
}
