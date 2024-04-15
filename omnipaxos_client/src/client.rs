use chrono::Utc;
use futures::SinkExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_stream::StreamExt;

use common::util::{get_node_addr, wrap_stream, Connection as ServerConnection};
use common::{kv::*, messages::*};

#[derive(Debug, Serialize)]
struct RequestData {
    time_sent_utc: i64,
    response: Option<Response>,
}

#[derive(Debug, Serialize)]
struct Response {
    time_recieved_utc: i64,
    message: ServerMessage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    location: String,
    server_id: u64,
    read_ratio: f64,
    request_rate_intervals: Vec<RequestInterval>,
    local_deployment: Option<bool>,
    kill_signal_sec: Option<u64>,
    pub scheduled_start_utc_ms: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestInterval {
    duration_sec: u64,
    requests_per_sec: u64,
}

impl RequestInterval {
    fn get_interval_duration(self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    fn get_request_delay(self) -> Duration {
        let delay_ms = 1000 / self.requests_per_sec;
        assert!(delay_ms != 0);
        Duration::from_millis(delay_ms)
    }
}

pub struct Client {
    server: ServerConnection,
    command_id: CommandId,
    request_data: Vec<RequestData>,
    read_ratio: f64,
    request_rate_intervals: Vec<RequestInterval>,
    kill_signal_sec: Option<u64>,
}

impl Client {
    pub async fn with(config: ClientConfig) -> Self {
        let is_local = config.local_deployment.unwrap_or(false);
        let server_address =
            get_node_addr(config.server_id, is_local).expect("Couldn't resolve server IP");
        let server_stream = TcpStream::connect(server_address)
            .await
            .expect("Couldn't connect to server {server_id}");
        server_stream.set_nodelay(true).unwrap();
        let mut client = Self {
            server: wrap_stream(server_stream),
            command_id: 0,
            request_data: Vec::with_capacity(8000),
            read_ratio: config.read_ratio,
            request_rate_intervals: config.request_rate_intervals,
            kill_signal_sec: config.kill_signal_sec,
        };
        client.send_registration().await;
        client
    }

    pub async fn put(&mut self, key: String, value: String) {
        self.send_command(KVCommand::Put(key, value)).await;
    }

    pub async fn delete(&mut self, key: String) {
        self.send_command(KVCommand::Delete(key)).await;
    }

    pub async fn get(&mut self, key: String) {
        self.send_command(KVCommand::Get(key)).await;
    }

    pub async fn run(&mut self) {
        if self.request_rate_intervals.is_empty() {
            return;
        }
        let mut rng = rand::thread_rng();
        let intervals = self.request_rate_intervals.clone();
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let mut kill_interval = match self.kill_signal_sec {
            Some(sec_until_kill) => interval(Duration::from_secs(sec_until_kill)),
            None => interval(Duration::from_secs(999999)),
        };
        next_interval.tick().await;
        kill_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                Some(msg) = self.server.next() => self.handle_response(msg.unwrap()),
                _ = request_interval.tick() => {
                    let key = self.command_id.to_string();
                    if rng.gen::<f64>() < self.read_ratio {
                        self.get(key).await;
                    } else {
                        self.put(key.clone(), key).await;
                    }
                },
                _ = next_interval.tick() => {
                    if let Some(new_interval) = intervals.next() {
                        next_interval = interval(new_interval.get_interval_duration());
                        next_interval.tick().await;
                        request_interval = interval(new_interval.get_request_delay());
                    } else {
                        break;
                    }
                },
                // Hardcoded for a specific benchmark
                _ = kill_interval.tick(), if self.kill_signal_sec.is_some() => {
                    self.send_kill_signal().await;
                    let server_address =
                        get_node_addr(6, false).expect("Couldn't resolve server IP");
                    let server_stream = TcpStream::connect(server_address)
                        .await
                        .expect("Couldn't connect to server {server_id}");
                    server_stream.set_nodelay(true).unwrap();
                    self.server = wrap_stream(server_stream);
                    self.send_registration().await;
                }
            }
        }
        self.print_results();
    }

    async fn send_command(&mut self, command: KVCommand) {
        let request = ClientMessage::Append(self.command_id, command);
        let data = RequestData {
            time_sent_utc: Utc::now().timestamp_millis(),
            response: None,
        };
        self.request_data.push(data);
        self.command_id += 1;
        if let Err(e) = self
            .server
            .send(NetworkMessage::ClientMessage(request))
            .await
        {
            log::error!("Couldn't send command to server: {e}");
        }
    }

    fn handle_response(&mut self, msg: NetworkMessage) {
        match msg {
            NetworkMessage::ServerMessage(response) => {
                let cmd_id = response.command_id();
                let response_time = Utc::now().timestamp_millis();
                self.request_data[cmd_id].response = Some(Response {
                    time_recieved_utc: response_time,
                    message: response,
                });
            }
            _ => panic!("Recieved unexpected message: {msg:?}"),
        }
    }

    async fn send_registration(&mut self) {
        self.server
            .send(NetworkMessage::ClientRegister)
            .await
            .expect("Couldn't send message to server");
    }

    async fn send_kill_signal(&mut self) {
        self.server
            .send(NetworkMessage::KillServer)
            .await
            .expect("Couldn't send message to server")
    }

    fn print_results(&self) {
        for request_data in &self.request_data {
            let request_json = serde_json::to_string(request_data).unwrap();
            println!("{request_json}");
        }
    }
}
