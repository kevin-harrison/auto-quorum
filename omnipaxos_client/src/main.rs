use std::time::Duration;

use client::Client;

mod client;

#[tokio::main]
pub async fn main() {
    let mut args = std::env::args().skip(1);
    // let cluster_size_arg = args.next().expect("Requires CLUSTER SIZE argument");
    // let cluster_size = cluster_size_arg.parse().expect("Invalid CLUSTER SIZE arg");
    let server_id_arg = args.next().expect("Requires SERVER ID argument");
    let server_id = server_id_arg.parse().expect("Invalid ACTIVE SERVER ID arg");
    let duration_arg = args.next().expect("Requires DURATION argument");
    let duration = duration_arg.parse().expect("Invalid DURATION arg");
    let start_delay_arg = args.next().expect("Requires START DELAY argument");
    let start_delay = start_delay_arg.parse().expect("Invalid START DELAY arg");
    let end_delay_arg = args.next().expect("Requires END DELAY argument");
    let end_delay = end_delay_arg.parse().expect("Invalid END DELAY arg");
    let local_deployment = match args.next() {
        Some(local_arg) => match local_arg.trim().to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => true,
            "false" | "f" | "no" | "n" | "0" => false,
            _ => panic!("Invalid LOCAL argument"),
        },
        None => false,
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
