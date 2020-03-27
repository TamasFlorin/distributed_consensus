use crate::protos::message::Message;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
//type EventHandler = dyn Fn(Message);

pub trait EventHandler {
    fn handle(&self, message: &Message);
}

pub struct EventQueue<T: EventHandler + Send + Sync> {
    handlers: Arc<Mutex<Vec<Box<T>>>>,
    queue: Arc<Mutex<VecDeque<Message>>>,
    item_added: Arc<Mutex<bool>>,
    cvar: Arc<Condvar>,
    is_running: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl<T: EventHandler + Send + Sync + 'static> Default for EventQueue<T> {
    fn default() -> Self {
        EventQueue {
            handlers: Arc::new(Mutex::new(Vec::new())),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            item_added: Arc::new(Mutex::new(false)),
            cvar: Arc::new(Condvar::default()),
            is_running: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
        }
    }
}

impl<T: EventHandler + Send + Sync + 'static> EventQueue<T> {
    pub fn push(&self, message: Message) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message);
        self.cvar.notify_one();
    }

    pub fn run(&mut self) {
        if self.is_running.load(Ordering::SeqCst) {
            panic!("Event queue is already running.");
        }

        let handlers = Arc::clone(&self.handlers);
        let item_added = Arc::clone(&self.item_added);
        let cvar = Arc::clone(&self.cvar);
        let queue = Arc::clone(&self.queue);
        let is_running = Arc::clone(&self.is_running);

        self.handle = Mutex::new(Some(thread::spawn(move || {
            is_running.store(true, Ordering::SeqCst);

            loop  {
                println!("IN while loop");
                let mut queue = queue.lock().unwrap();
                while !queue.is_empty() {
                    let first = queue.pop_front().unwrap();
                    println!("Processing message {:?}", first);

                    // we are sending the message to everyone for now...
                    // they will need to filter it themselvles.
                    let handlers = handlers.lock().unwrap();
                    for event_handler in handlers.iter() {
                        event_handler.handle(&first);
                    }
                }
                // unlock the queue since we are done with it
                std::mem::drop(queue);

                if !is_running.load(Ordering::SeqCst) {
                    break;
                }

                // sleep until we get some other work to do
                let added = item_added.lock().unwrap();
                let _ = cvar.wait(added);
            }
            println!("Closing queue");
        })));
    }

    pub fn close(&mut self) {
        let mut handle = self.handle.lock().unwrap();
        if handle.is_some() {
            self.is_running.store(false, Ordering::SeqCst);
            let _ = self.item_added.lock().unwrap();
            self.cvar.notify_one();
            let _= handle.take().unwrap().join();
            *handle = None;
        }
    }

    pub fn register_handler(&mut self, event_handler: T) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(Box::new(event_handler));
    }
}
