use crate::protos::message;
use message::ProcessId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub type NodeId = u16;

#[derive(Deserialize, Serialize, Debug, Clone, Eq)]
pub struct Node {
    pub owner: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub id: NodeId,
    pub rank: u16,
}

impl Node {
    pub fn new(owner: String, name: String, host: String, port: u16, id: u16, rank: u16) -> Self {
        Node {
            owner,
            name,
            host,
            port,
            id,
            rank,
        }
    }
}

impl From<Node> for SocketAddr {
    fn from(node: Node) -> Self {
        SocketAddr::from(&node)
    }
}

impl From<&Node> for SocketAddr {
    fn from(node: &Node) -> Self {
        let address = format!("{}:{}", node.host, node.port);
        let address: SocketAddr = address.parse().expect("Unable to parse socket address");
        address
    }
}

impl From<Node> for message::ProcessId {
    fn from(node: Node) -> Self {
        message::ProcessId::from(&node)
    }
}

impl From<&Node> for message::ProcessId {
    fn from(node: &Node) -> Self {
        let mut proc_id = message::ProcessId::new();
        proc_id.set_owner(node.owner.clone());
        proc_id.set_host(node.host.clone());
        proc_id.set_index(node.id as i32);
        proc_id.set_port(node.port as i32);
        proc_id
    }
}

impl From<Node> for message::EldTrust {
    fn from(node: Node) -> Self {
        message::EldTrust::from(&node)
    }
}

impl From<&Node> for message::EldTrust {
    fn from(node: &Node) -> Self {
        let proc_id = message::ProcessId::from(node);
        let mut eld_trust = message::EldTrust::new();
        eld_trust.set_process(proc_id);
        eld_trust
    }
}

impl From<&ProcessId> for Node {
    fn from(process_id: &ProcessId) -> Self {
        Node::new(
            process_id.get_owner().to_owned(),
            format!("{}-{}", process_id.get_owner(), process_id.get_index()),
            process_id.get_host().to_owned(),
            process_id.get_port() as u16,
            process_id.get_index() as u16,
            process_id.get_rank() as u16,
        )
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Id: {0}, Name: {1}, Host: {2}, Port: {3}",
            self.id, self.name, self.host, self.port
        ))
    }
}

impl PartialEq<Node> for Node {
    fn eq(&self, other: &Node) -> bool {
        self.id == other.id
    }
}

impl PartialOrd<Node> for Node {
    fn partial_cmp(&self, other: &Node) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NodeInfo {
    pub current_node: Node,
    pub hub: Node,
    pub nodes: Vec<Node>,
}
