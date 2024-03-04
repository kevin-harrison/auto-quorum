use futures::StreamExt;
use network::Network;
use tokio::time::Instant;
use rand::Rng;
use std::time::Duration;

use common::{kv::*, messages::*};

struct MultiClient {
    network: Network,
    cluster_size: u64,
    active_server: NodeId,
    command_id: CommandId,
    sent_data: Vec<(Instant, NodeId)>,
    recieved_data: Vec<(Duration, NodeId, ClientResponse)>,
}

impl MultiClient {
    async fn new(cluster_size: u64, active_server: NodeId, is_local: bool) -> Self {
        let nodes = (1..=cluster_size).collect();
        Self {
            network: Network::new(nodes, is_local).await,
            cluster_size,
            active_server,
            command_id: 0,
            sent_data: Vec::with_capacity(8000),
            recieved_data: Vec::with_capacity(8000),
        }
    }

    async fn run_simple(&mut self) {
        let mut rng = rand::thread_rng();
        let run_duration = Duration::from_secs(10);
        let mut end_run = tokio::time::interval(run_duration);
        let mut halfway = tokio::time::interval(run_duration / 2);
        halfway.tick().await;
        end_run.tick().await;

        let initial_request_delay = Duration::from_millis(1000);
        let next_request_delay = Duration::from_millis(100);
        let mut request_interval = tokio::time::interval(initial_request_delay);

        loop {
            tokio::select! {
                biased;
                Some((from, response)) = self.network.next() => self.handle_response(from, response.unwrap()),
                _ = request_interval.tick() => {
                    let key = self.command_id.to_string();
                    self.active_server = rng.gen_range(3..=self.cluster_size);
                    match rng.gen_range(0..=2) {
                        0 => self.put(key.clone(), key).await,
                        1 => self.delete(key).await,
                        _ => self.get(key).await,
                    }
                },
                _ = halfway.tick() => {
                    request_interval = tokio::time::interval(next_request_delay);
                }
                _ = end_run.tick() => break,
            }
        }
        println!("{:?}", self.recieved_data);
    }

    fn handle_response(&mut self, from: NodeId, response: ClientResponse) {
        let msg_delay = self.sent_data[response.command_id()].0.elapsed();
        self.recieved_data.push((msg_delay, from, response))
    }

    fn get_next_command_id(&mut self) -> CommandId {
        let id = self.command_id;
        self.command_id += 1;
        id
    }

    async fn send_command(&mut self, command: KVCommand) {
        let cmd_id = self.get_next_command_id();
        let request = ClientRequest::Append(cmd_id, command);
        self.sent_data.push((Instant::now(), self.active_server));
        self.network.send(self.active_server, request).await;
    }

    async fn put(&mut self, key: String, value: String) {
        self.send_command(KVCommand::Put(key, value)).await;
    }

    async fn delete(&mut self, key: String) {
        self.send_command(KVCommand::Delete(key)).await;
    }

    async fn get(&mut self, key: String) {
        self.send_command(KVCommand::Get(key)).await;
    }
}
