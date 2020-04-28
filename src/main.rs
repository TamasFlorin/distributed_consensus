mod protos;
use protos::message::Message;
mod eld;
use std::error::Error;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpListener;
pub mod event;
use event::EventData;
use event::EventQueue;
use protobuf::parse_from_reader;
pub mod node;
use clap::{App, Arg};
use node::Node;
use node::NodeInfo;
use serde_json;
use std::fs;
use std::path::Path;
pub mod perfect_link;
use log::{info, trace, warn};
pub mod beb;
pub mod ec;
pub mod ep;
pub mod storage;
pub mod uc;

struct UcHandler;

impl event::EventHandler for UcHandler {
    fn handle(&mut self, event_data: &EventData) {
        if let event::EventData::Internal(msg) = event_data {
            if let event::InternalMessage::UcDecide(value) = msg {
                trace!("Decided value: {}", value);
            }
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

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
    let current_node = nodes.iter().find(|node| node.id == my_id).unwrap().clone();
    let node_info = std::sync::Arc::new(node::NodeInfo {
        current_node,
        nodes,
    });

    run(node_info)
}

fn read_config<P: AsRef<Path>>(path: &P) -> Result<Vec<Node>, Box<dyn Error>> {
    let mut file = fs::File::open(path.as_ref())?;
    let mut contents = String::new();
    let _ = file.read_to_string(&mut contents)?;
    let nodes: Vec<Node> = serde_json::from_str(&contents)?;
    Ok(nodes)
}

fn run(node_info: std::sync::Arc<NodeInfo>) -> Result<(), Box<dyn Error>> {
    info!("Listening on Node: {}", node_info.current_node);
    let node_path = format!("{0}_eld_state.json", node_info.current_node.name);
    let local_storage = storage::LocalStorage::new(node_path);
    let event_queue = std::sync::Arc::new(EventQueue::create_and_run());
    let uc_handler = UcHandler {};
    let pl = perfect_link::PerfectLink::new(event_queue.clone(), node_info.clone());
    let mut eld =
        eld::EventualLeaderDetector::new(node_info.clone(), event_queue.clone(), local_storage);
    let beb = beb::BestEffortBroadcast::new(node_info.clone(), event_queue.clone());
    let ec = ec::EpochChange::new(node_info.clone(), event_queue.clone());
    let ep = ep::EpochConsensus::new(
        node_info.clone(),
        event_queue.clone(),
        ep::EpochConsensusState::new(0, 0),
        ec.trusted.clone(),
        0,
    );
    let uc = uc::UniformConsensus::new(event_queue.clone(), node_info.clone(), ec.trusted.clone());

    eld.init();
    uc.init();

    event_queue.register_handler(Box::new(uc_handler));
    event_queue.register_handler(Box::new(pl));
    event_queue.register_handler(Box::new(eld));
    event_queue.register_handler(Box::new(beb));
    event_queue.register_handler(Box::new(ec));
    event_queue.register_handler(Box::new(ep));
    event_queue.register_handler(Box::new(uc));

    let queue = event_queue.clone();
    let info = node_info.clone();
    let _handle = std::thread::spawn(move || {
        // TODO: handle result
        let _ = listen_for_clients(queue, info);
    });

    listen_for_command(event_queue, node_info);

    Ok(())
}

fn listen_for_command(
    event_queue: std::sync::Arc<EventQueue>,
    _node_info: std::sync::Arc<NodeInfo>,
) {
    loop {
        let mut command = String::new();
        let read_result = std::io::stdin().lock().read_line(&mut command);
        if read_result.is_ok() {
            command.pop();
            let command = command.to_lowercase();
            if command == "exit" || command == "e" {
                break;
            } else if command.starts_with("propose") || command.starts_with('p') {
                let mut tokens = command.split(' ');
                let value = tokens
                    .nth(1)
                    .expect("We should have the value from the command.")
                    .to_owned();
                let value = value
                    .parse::<i32>()
                    .expect("Command parameter should be an int.");
                let propose_message = event::InternalMessage::UcPropose(value);
                let event_data = event::EventData::Internal(propose_message);
                event_queue.push(event_data);
            } else {
                eprintln!("Available commands: exit(e) or propose(p) <value>");
            }
        }
    }
}

fn listen_for_clients(
    event_queue: std::sync::Arc<EventQueue>,
    node_info: std::sync::Arc<NodeInfo>,
) -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = node_info.current_node.clone().into();
    let listener = TcpListener::bind(address)?;
    loop {
        match listener.accept() {
            Ok((mut stream, client)) => {
                trace!("Client connected: {}", client);
                let message = parse_from_reader::<Message>(&mut stream);

                match message {
                    Ok(msg) => {
                        let message = EventData::External(msg);
                        event_queue.push(message);
                    }
                    Err(e) => {
                        warn!("Failed to parse message with error: {}", e);
                    }
                };
            }
            Err(e) => return Err(Box::new(e)),
        }
    }
}
