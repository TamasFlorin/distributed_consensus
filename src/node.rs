use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use crate::protos::message;
#[derive(Deserialize, Serialize, Debug, Clone, Eq)]
pub struct Node {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub id: u16,
}

impl Node {
    pub fn new(name: String, host: String, port: u16, id: u16) -> Self {
        Node {
            name,
            host,
            port,
            id,
        }
    }
}

impl From<Node> for SocketAddr {
    fn from(node: Node) -> Self { 
        let address = format!("{}:{}", node.host, node.port);
        let address: SocketAddr = address.parse().expect("Unable to parse socket address");
        address
    }
}

impl From<Node> for message::ProcessId {
    fn from(node: Node) -> Self {
        let mut proc_id = message::ProcessId::new();
        proc_id.set_host(node.host);
        proc_id.set_index(node.id as i32);
        proc_id.set_port(node.port as i32);
        proc_id
    }
}

impl From<&Node> for message::ProcessId {
    fn from(node: &Node) -> Self {
        let mut proc_id = message::ProcessId::new();
        proc_id.set_host(node.host.clone());
        proc_id.set_index(node.id as i32);
        proc_id.set_port(node.port as i32);
        proc_id
    }
}

impl From<Node> for message::EldTrust {
    fn from(node: Node) -> Self {
        let proc_id = message::ProcessId::from(node);
        let mut eld_trust = message::EldTrust::new();
        eld_trust.set_processId(proc_id);
        eld_trust
    }
}

impl From<&Node> for message::EldTrust {
    fn from(node: &Node) -> Self {
        let proc_id = message::ProcessId::from(node);
        let mut eld_trust = message::EldTrust::new();
        eld_trust.set_processId(proc_id);
        eld_trust
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { 
        f.write_fmt(format_args!("Id: {0}, Name: {1}, Host: {2}, Port: {3}", self.id, self.name, self.host, self.port))
    }
}

impl PartialEq<Node> for Node {
    fn eq(&self, other: &Node) -> bool {
        self.id == other.id
    }
}

impl PartialOrd<Node> for Node {
    fn partial_cmp(&self, other: &Node) -> Option<std::cmp::Ordering> {
        if self.id == other.id {
            Some(std::cmp::Ordering::Equal)
        } else if self.id > other.id {
            Some(std::cmp::Ordering::Greater)
        } else {
            Some(std::cmp::Ordering::Less)
        }
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { 
        if self.id == other.id {
            std::cmp::Ordering::Equal
        } else if self.id > other.id {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        }
    }
}

pub struct NodeInfo {
    pub current_node: Node,
    pub nodes: Vec<Node>,
}