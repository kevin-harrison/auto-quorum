use crate::server::OmniPaxosServer;
use env_logger;
use omnipaxos::{util::NodeId, ClusterConfig, OmniPaxosConfig, ServerConfig};

#[macro_use]
extern crate lazy_static;

mod database;
mod network;
mod optimizer;
mod read;
mod router;
mod server;

lazy_static! {
    pub static ref PID: NodeId = if let Ok(var) = std::env::var("PID") {
        let x = var.parse().expect("PIDs must be u64");
        if x == 0 {
            panic!("PIDs cannot be 0")
        } else {
            x
        }
    } else {
        panic!("missing PID")
    };
    pub static ref LOCAL_CLUSTER_DEPLOYMENT: bool = if let Ok(var) = std::env::var("LOCAL") {
        match var.trim().to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => true,
            "false" | "f" | "no" | "n" | "0" => false,
            _ => panic!("Invalid LOCAL argument"),
        }
    } else {
        false
    };
}

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3, 4, 5],
        ..Default::default()
    };
    let server_config = ServerConfig {
        pid: *PID,
        election_tick_timeout: 1,
        resend_message_tick_timeout: 5,
        flush_batch_tick_timeout: 50,
        ..Default::default()
    };
    let omnipaxos_config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    let mut server = OmniPaxosServer::new(omnipaxos_config, *LOCAL_CLUSTER_DEPLOYMENT).await;
    server.run().await;
}
