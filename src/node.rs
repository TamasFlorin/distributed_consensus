use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
#[derive(Deserialize, Serialize, Debug, Clone)]
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

#[derive(Debug)]
pub enum Nodes {
    Current(Node),
    Other(Vec<Node>)
}