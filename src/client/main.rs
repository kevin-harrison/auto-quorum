use client::{Client, ClientConfig};
use env_logger;
use std::{env, fs};
use toml;

mod client;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let client_config: ClientConfig = toml::from_str(&config_string).unwrap();
    println!("{}", serde_json::to_string(&client_config).unwrap());
    Client::run(client_config).await;
}
