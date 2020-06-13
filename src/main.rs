mod app;
mod beb;
mod ec;
mod eld;
mod ep;
mod epfd;
mod event;
mod node;
mod pl;
mod protos;
mod sys;
mod uc;
use clap::{App, Arg};
use env_logger::{Builder, Target};
use event::{EventData, EventQueue, InternalMessage};
use log::{error, info, trace};
use node::Node;
use node::NodeInfo;
use protos::message::Message;
use serde_json;
use std::error::Error;
use std::fs;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = Builder::from_default_env();
    builder.target(Target::Stdout);
    builder.init();

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
        .arg(
            Arg::with_name("hub")
                .short("hb")
                .long("hub")
                .help("The hub configuration file.")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let file_name = matches.value_of("config").unwrap();
    let hub_file = matches.value_of("hub").unwrap();
    let my_id = matches.value_of("id").unwrap().parse::<u16>()?;
    let mut nodes = read_config(&file_name)?;
    let hub_nodes = read_config(&hub_file)?;
    let hub = hub_nodes.get(0).unwrap().clone();
    nodes.extend(hub_nodes.clone());

    let current_node = nodes.iter().find(|node| node.id == my_id).unwrap().clone();
    let node_info = std::sync::Arc::new(node::NodeInfo {
        current_node,
        hub,
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

    let event_queue = std::sync::Arc::new(EventQueue::create_and_run());
    let pl = pl::PerfectLink::new(event_queue.clone(), node_info.clone());
    let app = app::App::new(
        node_info.current_node.clone(),
        node_info.hub.clone(),
        event_queue.clone(),
    );
    let app_system_id = "app_system_id";
    event_queue.register_handler(Box::new(app));
    event_queue.register_handler(Box::new(pl));
    event_queue.push(EventData::Internal(
        app_system_id.to_owned(),
        InternalMessage::AppInit,
    ));
    let listen_result = listen_for_clients(event_queue.clone(), node_info.clone());
    if listen_result.is_err() {
        error!("{:?}", listen_result.err());
    }
    Ok(())
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
                let mut recv_bytes = Vec::new();
                let read_result = stream.read_to_end(&mut recv_bytes);
                if let Ok(_) = read_result {
                    let proto_buffer = &recv_bytes[4..];
                    let message: Result<Message, protobuf::ProtobufError> =
                        protobuf::parse_from_bytes(proto_buffer);

                    match message {
                        Ok(recv_msg) => {
                            let system_id: String = recv_msg.get_systemId().into();
                            let message = EventData::External(system_id, recv_msg);
                            event_queue.push(message);
                        }
                        Err(e) => {
                            error!("Failed to parse message with error: {}", e);
                        }
                    };
                } else {
                    error!("Unable to read message bytes.");
                }
            }
            Err(e) => {
                return Err(Box::new(e));
            }
        }
    }
}
