use crate::{configs::ClientConfig, data_collection::ClientData, network::Network};
use auto_quorum::common::{kv::*, messages::*};
use chrono::Utc;
use log::*;
use rand::Rng;
use std::time::Duration;
use tokio::time::interval;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let network = Network::new(
            vec![(config.server_id, config.server_address.clone())],
            NETWORK_BATCH_SIZE,
        )
        .await;
        Client {
            id: config.server_id,
            network,
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            final_request_count: None,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        // Early ends
        let intervals = self.config.requests.clone();
        if intervals.is_empty() {
            self.save_results().expect("Failed to save results");
            return;
        }
        if let Some(0) = self.config.kill_signal_sec {
            info!("{}: Sending kill signal to {}", self.id, self.active_server);
            let kill_msg = ClientMessage::Kill;
            self.network.send(self.active_server, kill_msg).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.network.shutdown().await;
            self.save_results().expect("Failed to save results");
            return;
        }

        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(start_time).await;
            }
            _ => panic!("Error waiting for start signal"),
        }

        // Initialize intervals
        let mut rng = rand::thread_rng();
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let mut read_ratio = first_interval.read_ratio;
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let _ = next_interval.tick().await;
        let mut kill_signal = self
            .config
            .kill_signal_sec
            .map(|delay| Box::pin(tokio::time::sleep(Duration::from_secs(delay))));

        // Main event loop
        info!("{}: Starting requests", self.id);
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                }
                _ = request_interval.tick(), if self.final_request_count.is_none() => {
                    let is_write = rng.gen::<f64>() > read_ratio;
                    self.send_request(is_write).await;
                },
                _ = next_interval.tick() => {
                    match intervals.next() {
                        Some(new_interval) => {
                            read_ratio = new_interval.read_ratio;
                            next_interval = interval(new_interval.get_interval_duration());
                            next_interval.tick().await;
                            request_interval = interval(new_interval.get_request_delay());
                        },
                        None => {
                            self.final_request_count = Some(self.client_data.request_count());
                            if self.run_finished() {
                                break;
                            }
                        },
                    }
                },
                // TODO: could have outgoing requests to server we are killing => number of
                // responses to wait for is undefined.
                _ = async { kill_signal.as_mut().unwrap().await }, if kill_signal.is_some() => {
                    info!("{}: Sending kill signal to {}", self.id, self.active_server);
                    let kill_msg = ClientMessage::Kill;
                    // TODO: should cleanup connection task here
                    self.network.send(self.active_server, kill_msg).await;
                    break;
                }
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        if self.config.kill_signal_sec.is_none() {
            // Still connected to an active server
            info!("{}: Sending done signal to {}", self.id, self.active_server);
            self.network
                .send(self.active_server, ClientMessage::Done)
                .await;
        }
        self.network.shutdown().await;
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) -> bool {
        debug!("Recieved {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let cmd_id = server_response.command_id();
                self.client_data.new_response(cmd_id);
            }
        }
        return false;
    }

    async fn send_request(&mut self, is_write: bool) {
        let key = self.next_request_id.to_string();
        let cmd = match is_write {
            true => KVCommand::Put(key.clone(), key),
            false => KVCommand::Get(key),
        };
        let request = ClientMessage::Append(self.next_request_id, cmd);
        debug!("Sending {request:?}");
        // TODO: handle errors?
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        return false;
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(scheduled_start_utc_ms: i64) {
        // // Desync the clients a bit
        // let mut rng = rand::thread_rng();
        // let scheduled_start_utc_ms = scheduled_start_utc_ms + rng.gen_range(1..100);
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        // let sync_time = Utc::now() + chrono::Duration::milliseconds(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        Ok(())
    }
}
