use std::env;
use crate::server::OmniPaxosServer;
use env_logger;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};

mod database;
// mod network;
mod optimizer;
mod read;
mod router;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_id  = match env::var("SERVER_ID") {
        Ok(id_str) => id_str.parse().expect("Invalid SERVER ID arg"),
        Err(_) => panic!("Requires SERVER ID argument")
    };
    let optimize  = match env::var("OPTIMIZE") {
        Ok(optimize_str) => match optimize_str.trim().to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => true,
            "false" | "f" | "no" | "n" | "0" => false,
            _ => panic!("Invalid OPTIMIZE argument"),
        }
        Err(_) => true
    };
    let local_deployment = match env::var("LOCAL") {
        Ok(local_str) => match local_str.trim().to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => true,
            "false" | "f" | "no" | "n" | "0" => false,
            _ => panic!("Invalid LOCAL argument"),
        },
        Err(_) => false
    };

    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        ..Default::default()
    };
    let server_config = ServerConfig {
        pid: server_id,
        election_tick_timeout: 1,
        resend_message_tick_timeout: 5,
        flush_batch_tick_timeout: 50,
        ..Default::default()
    };
    let omnipaxos_config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    let mut server = OmniPaxosServer::new(omnipaxos_config, optimize, local_deployment).await;
    server.run().await;
}
