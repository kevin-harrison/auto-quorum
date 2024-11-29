use crate::{configs::AutoQuorumServerConfig, server::OmniPaxosServer};
use env_logger;
use std::{env, fs};
use toml;

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
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let server_config: AutoQuorumServerConfig = toml::from_str(&config_string).unwrap();
    println!("{}", serde_json::to_string(&server_config).unwrap());
    let mut server = OmniPaxosServer::new(server_config).await;
    server.run().await;
}
