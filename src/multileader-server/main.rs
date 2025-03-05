use crate::{configs::MultiLeaderConfig, server::MultiLeaderServer};
use env_logger;

mod configs;
mod database;
mod metrics;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match MultiLeaderConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut server = MultiLeaderServer::new(server_config).await;
    server.run().await;
}
