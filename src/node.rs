use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NodeConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub id: u16,
}

impl NodeConfig {
    pub fn new(name: String, host: String, port: u16, id: u16) -> Self {
        NodeConfig {
            name,
            host,
            port,
            id,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct NodeConfigs {
    nodes: Vec<NodeConfig>,
}

impl NodeConfigs {
    pub fn find(&self, id: u16) -> Option<NodeConfig> {
        let found: Option<&NodeConfig> = self.nodes.iter().find(|node| node.id == id);
        match found {
            Some(node) => Some(node.clone()),
            _ => None,
        }
    }
}
