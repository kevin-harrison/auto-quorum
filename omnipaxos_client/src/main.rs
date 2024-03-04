use std::{time::Duration, env};

use client::Client;

mod client;

#[tokio::main]
pub async fn main() {
    let server_id  = match env::var("SERVER_ID") {
        Ok(id_str) => id_str.parse().expect("Invalid SERVER ID arg"),
        Err(_) => panic!("Requires SERVER ID argument")
    };
    let duration  = match env::var("DURATION") {
        Ok(duration_str) => duration_str.parse().expect("Invalid DURATION arg"),
        Err(_) => panic!("Requires DURATION argument")
    };
    let start_delay  = match env::var("START_DELAY") {
        Ok(start_str) => start_str.parse().expect("Invalid START_DELAY arg"),
        Err(_) => panic!("Requires START_DELAY argument")
    };
    let end_delay  = match env::var("END_DELAY") {
        Ok(end_str) => end_str.parse().expect("Invalid END_DELAY arg"),
        Err(_) => panic!("Requires END_DELAY argument")
    };
    let local_deployment  = match env::var("local") {
        Ok(local_str) => {
            match local_str.trim().to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => true,
            "false" | "f" | "no" | "n" | "0" => false,
            _ => panic!("Invalid LOCAL argument")
            }
        }
        Err(_) => false 
    };

    // Simple benchmark
    let mut client = Client::new(server_id, local_deployment).await;
    let run_duration = Duration::from_secs(duration);
    let initial_request_delay = Duration::from_millis(start_delay);
    let end_request_delay = Duration::from_millis(end_delay);
    client
        .run_simple(run_duration, initial_request_delay, end_request_delay)
        .await;
}
