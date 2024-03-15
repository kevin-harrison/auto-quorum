use std::{env, fs};
use toml;
use client::Client;

mod client;

#[tokio::main]
pub async fn main() {
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let client_config = toml::from_str(&config_string).unwrap();
    let mut client = Client::with(client_config).await;
    client.run().await;
}
