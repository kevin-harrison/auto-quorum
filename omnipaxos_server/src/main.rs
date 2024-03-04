use crate::server::OmniPaxosServer;
use env_logger;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};

mod database;
mod network;
mod optimizer;
mod read;
mod router;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let mut args = std::env::args().skip(1);
    let pid_arg = args.next().expect("First argument must be SERVER ID");
    let pid = pid_arg.parse().expect("Invalid SERVER ID arg");
    let local_deployment = match args.next(){
        Some(local_arg) => {
            match local_arg.trim().to_lowercase().as_str() {
                "true" | "t" | "yes" | "y" | "1" => true,
                "false" | "f" | "no" | "n" | "0" => false,
                _ => panic!("Invalid LOCAL argument"),
            }
        },
        None => false
    };

    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        ..Default::default()
    };
    let server_config = ServerConfig {
        pid,
        election_tick_timeout: 1,
        resend_message_tick_timeout: 5,
        flush_batch_tick_timeout: 50,
        ..Default::default()
    };
    let omnipaxos_config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    let mut server = OmniPaxosServer::new(omnipaxos_config, local_deployment).await;
    server.run().await;
}
