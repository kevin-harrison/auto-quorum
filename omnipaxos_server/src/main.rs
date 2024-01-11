use crate::{network::Network, server::OmniPaxosServer};
use env_logger;
use omnipaxos::{util::NodeId, ClusterConfig, OmniPaxosConfig, ServerConfig};
use tokio::sync::mpsc;

mod database;
mod network;
mod server;
mod read;

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
    let peers = vec![1, 2, 3].into_iter().filter(|pid| *pid != id).collect();

    // Server and Network channels
    let (server_sender, server_receiver) = mpsc::channel(100);
    let (network_sender, network_receiver) = mpsc::channel(100);

    let mut network = Network::new(id, peers, network_receiver, server_sender)
        .await
        .unwrap();

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
    let mut server = OmniPaxosServer::new(omnipaxos_config, network_sender, server_receiver);

    tokio::join!(network.run(), server.run());
}
