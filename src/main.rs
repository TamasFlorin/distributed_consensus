mod protos;
use protos::message::Message;
mod eld;
use std::error::Error;
use std::io::prelude::*;
use std::net::TcpListener;
use std::net::SocketAddr;
pub mod event;
use event::EventHandler;
use event::EventQueue;

struct MyEventHandler {}

impl EventHandler for MyEventHandler {
    fn handle(&self, msg: &Message) {
        println!("I am a handler and I have been summoned with msg {:?}", msg);
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut event_queue = EventQueue::default();
    event_queue.register_handler(MyEventHandler {});
    event_queue.run();
    let address = SocketAddr::from(([127, 0, 0, 1], 1337));
    let listener = TcpListener::bind(address)?;
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                println!("connected");
                let mut buffer = [0; 2048];
                let num_bytes = stream.read(&mut buffer)?;
                match num_bytes {
                    1 => break,
                    _ => {
                        println!("Buffer={:?}", &buffer[0..num_bytes]);
                        event_queue.push(Message::new());
                    }
                }
            }
            Err(e) => {
                println!("{:?}", e);
                break;
            }
        };
    }
    event_queue.close();
    Ok(())
}
