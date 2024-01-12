use crate::server::OmniPaxosServer;
use env_logger;
use omnipaxos::{util::NodeId, ClusterConfig, OmniPaxosConfig, ServerConfig};

mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    // Server ID
    let args: Vec<String> = std::env::args().collect();
    let id: NodeId = args
        .get(1)
        .expect("Must pass a node ID.")
        .parse()
        .expect("Unable to parse node ID.");

    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        ..Default::default()
    };
    let server_config = ServerConfig {
        pid: id,
        ..Default::default()
    };
    let omnipaxos_config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    let mut server = OmniPaxosServer::new(omnipaxos_config).await;
    server.run().await;
}
