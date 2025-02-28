use client::Client;
use configs::ClientConfig;
use core::panic;
use env_logger;
use std::time::Duration;

mod client;
mod configs;
mod data_collection;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let client_config = match ClientConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut client = match Client::new(client_config).await {
        Ok(client) => client,
        Err(e) => panic!("{e}"),
    };
    client.run().await;
}
