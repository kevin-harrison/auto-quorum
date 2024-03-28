use std::{env, fs};
use crate::server::OmniPaxosServer;
use common::kv::{Command, NodeId};
use env_logger;
use metrics::MetricsHeartbeatServer;
use omnipaxos::{ballot_leader_election::Ballot, storage::Storage, OmniPaxosConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use router::Router;
use serde::{Serialize, Deserialize};
use toml;

mod database;
mod optimizer;
mod metrics;
mod read;
mod router;
mod server;

#[derive(Debug, Serialize, Deserialize)]
struct ServerConfig {
    location: String,
    initial_leader: Option<NodeId>,
    optimize: Option<bool>,
    congestion_control: Option<bool>,
    local_deployment: Option<bool>,
}

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let omnipaxos_config = OmniPaxosConfig::with_toml(&config_file).unwrap();
    let config_string = fs::read_to_string(config_file).unwrap();
    let server_config: ServerConfig = toml::from_str(&config_string).unwrap();
    println!("{}", serde_json::to_string(&server_config).unwrap());

    let server_id = omnipaxos_config.server_config.pid;
    let nodes = omnipaxos_config.cluster_config.nodes.clone();
    let local_deployment = server_config.local_deployment.unwrap_or(false);
    let congestion_control = server_config.congestion_control.unwrap_or(false);
    let optimize = server_config.optimize.unwrap_or(true);
    let router = Router::new(server_id, nodes.clone(), local_deployment, congestion_control).await.unwrap();
    let metrics = MetricsHeartbeatServer::new(server_id, nodes);
    let storage: MemoryStorage<Command> = MemoryStorage::default();
    // match server_config.initial_leader {
    //     Some(init_leader_id) if init_leader_id == server_id => {
    //         omnipaxos_config.server_config.leader_priority = 133;
    //     }
    //     _ => (),
    // }
    // log::debug!("{omnipaxos_config:?}");
    // // Hack to set an initial leader for the cluster
    // let saved_promise = storage.get_promise().unwrap();
    // match (server_config.initial_leader, saved_promise) {
    //     (Some(init_leader_id), None) if init_leader_id == server_id => {
    //         let initial_ballot = Ballot { config_id: 1, n: 1000, priority: 1, pid: init_leader_id };
    //         storage.set_promise(initial_ballot, init_leader_id).unwrap()
    //     }
    //     _ => (),
    // }
    let mut server = OmniPaxosServer::new(omnipaxos_config, storage, router, metrics, optimize).await;
    server.run(server_config.initial_leader).await;
}
