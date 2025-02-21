use crate::{configs::AutoQuorumConfig, server::OmniPaxosServer};
use env_logger;

mod configs;
mod database;
mod metrics;
mod network;
mod optimizer;
mod read;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match AutoQuorumConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut server = OmniPaxosServer::new(server_config).await;
    server.run().await;
}
