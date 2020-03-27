mod protos;
use protos::message::Message;
mod eld;
use std::error::Error;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpListener;
pub mod event;
use event::EventHandler;
use event::EventQueue;
use protobuf::parse_from_bytes;
pub mod node;
use clap::{App, Arg, SubCommand};
use node::NodeConfigs;
use serde_json;
use std::env;
use std::fs;
use std::path::Path;

struct MyEventHandler {}

impl EventHandler for MyEventHandler {
    fn handle(&mut self, msg: &Message) {
        println!("I am a handler and I have been summoned with msg {:?}", msg);
    }
}

fn read_config<P: AsRef<Path>>(path: &P) -> Result<NodeConfigs, Box<dyn Error>> {
    let mut file = fs::File::open(path.as_ref())?;
    let mut contents = String::new();
    let _ = file.read_to_string(&mut contents)?;
    let nodes: NodeConfigs = serde_json::from_str(&contents)?;
    Ok(nodes)
}

fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("Distributed Consensus")
        .version("1.0")
        .author("Florin T. <tamasflorin@live.com>")
        .arg(
            Arg::with_name("id")
                .help("Set the id of the current node.")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .help("The node configuration file.")
                .takes_value(true)
                .required(true),
        )
        .get_matches();
    let file_name = matches.value_of("config").unwrap();
    let my_id = matches.value_of("id").unwrap().parse::<u16>()?;
    let nodes = read_config(&file_name)?;
    let current_node = nodes.find(my_id).unwrap();
    println!("{:?}", current_node);
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
                        let message = parse_from_bytes::<Message>(&buffer);
                        match message {
                            Ok(msg) => event_queue.push(msg),
                            _ => {
                                let mut msg = Message::new();
                                msg.set_field_type(protos::message::Message_Type::UC_PROPOSE);
                                event_queue.push(msg);
                                println!("Reveived unknown message: {:?}", &buffer[0..num_bytes]);
                            }
                        };
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
