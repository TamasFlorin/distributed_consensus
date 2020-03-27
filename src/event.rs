use crate::protos::message::Message;
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
//type EventHandler = dyn Fn(Message);

pub trait EventHandler {
    fn handle(&mut self, message: &Message);
}

pub struct EventQueue<T: EventHandler + Send + Sync> {
    handlers: Arc<Mutex<Vec<T>>>,
    queue: Arc<Mutex<VecDeque<Message>>>,
    cvar: Arc<Condvar>,
    is_running: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl<T: EventHandler + Send + Sync + 'static> Default for EventQueue<T> {
    fn default() -> Self {
        EventQueue {
            handlers: Arc::new(Mutex::new(Vec::new())),
            queue: Arc::new(Mutex::new(VecDeque::new())),
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
        let cvar = Arc::clone(&self.cvar);
        let queue = Arc::clone(&self.queue);
        let is_running = Arc::clone(&self.is_running);

        self.handle = Mutex::new(Some(thread::spawn(move || {
            is_running.store(true, Ordering::SeqCst);

            loop {
                let mut q = queue.lock().unwrap();
                while !q.is_empty() {
                    let first = q.pop_front().unwrap();
                    println!("Processing message {:?}", first);

                    // we are sending the message to everyone for now...
                    // they will need to filter it themselvles.
                    let mut guard = handlers.lock().unwrap();
                    let handlers = guard.deref_mut();
                    for event_handler in handlers.iter_mut() {
                        event_handler.handle(&first);
                    }
                }
                // unlock the queue since we are done with it
                std::mem::drop(q);

                if !is_running.load(Ordering::SeqCst) {
                    break;
                }

                // sleep until we get some other work to do
                let guard = queue.lock().unwrap();
                let _ = cvar
                    .wait_while(guard, |q| q.is_empty() && is_running.load(Ordering::SeqCst))
                    .unwrap();
            }
        })));
    }

    pub fn close(&mut self) {
        let mut handle = self.handle.lock().unwrap();
        if handle.is_some() {
            self.is_running.store(false, Ordering::SeqCst);
            let _ = self.queue.lock().unwrap();
            self.cvar.notify_one();
            let _ = handle.take().unwrap().join();
        }
    }

    pub fn register_handler(&mut self, event_handler: T) {
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push(event_handler);
    }
}
