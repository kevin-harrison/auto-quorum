use crate::{configs::ClientConfig, data_collection::ClientData};
use auto_quorum::common::{kv::*, utils::Timestamp};
use chrono::Utc;
use etcd_client::{Client as EtcdClient, Error as EtcdError, EventType};
use log::*;
use rand::Rng;
use std::time::Duration;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::interval,
};

pub struct Client {
    id: ClientId,
    etcd: EtcdClient,
    response_sender: Sender<CommandId>,
    response_receiver: Receiver<CommandId>,
    client_data: ClientData,
    config: ClientConfig,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Result<Client, EtcdError> {
        let (response_sender, response_receiver) = tokio::sync::mpsc::channel(1000);
        info!("{} : Connecting to server", config.server_id);
        Ok(Client {
            id: config.server_id,
            etcd: EtcdClient::connect([config.server_address.clone()], None).await?,
            response_sender,
            response_receiver,
            client_data: ClientData::new(),
            config,
            final_request_count: None,
            next_request_id: 0,
        })
    }

    pub async fn run(&mut self) {
        // Early ends
        let intervals = self.config.requests.clone();
        if intervals.is_empty() {
            self.save_results().expect("Failed to save results");
            return;
        }
        self.initialize_leader_and_sychronize_cluster_start()
            .await
            .expect("Couldn't initialize cluster");

        match self.config.kill_signal_sec {
            Some(0) => {
                info!("{}: Removing server from cluster", self.id);
                let my_servers_id = self
                    .get_etcd_member_id(self.config.server_name.clone())
                    .await
                    .expect("Must be connected to cluster")
                    .expect("My server should be in cluster");
                self.etcd
                    .member_remove(my_servers_id)
                    .await
                    .expect("Couldn't remove server");
                self.save_results().expect("Failed to save results");
                return;
            }
            Some(_) => unimplemented!(),
            None => (),
        }

        // Initialize intervals
        let mut rng = rand::thread_rng();
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let mut read_ratio = first_interval.read_ratio;
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let _ = next_interval.tick().await;

        // Main event loop
        info!("{}: Starting requests", self.id);
        loop {
            tokio::select! {
                biased;
                Some(cmd_id) = self.response_receiver.recv() => {
                    self.handle_server_message(cmd_id);
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
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.save_results().expect("Failed to save results");
    }

    // Sets up cluster's initial leader and waits for a synchronized cluster start time
    async fn initialize_leader_and_sychronize_cluster_start(&mut self) -> Result<(), EtcdError> {
        // Wait for etcd cluster to stabilize
        tokio::time::sleep(Duration::from_secs(5)).await;
        let my_servers_id = self
            .get_etcd_member_id(self.config.server_name.clone())
            .await?
            .expect("Server name not found");
        let init_leader_id = self
            .get_etcd_member_id(self.config.initial_leader.clone())
            .await?
            .expect("Server name not found");
        let current_leader_id = self.etcd.status().await?.leader();

        let cluster_start_time = if current_leader_id == my_servers_id {
            if current_leader_id != init_leader_id {
                // Only the current leader can move leader
                self.etcd
                    .move_leader(init_leader_id)
                    .await
                    .expect("Couldn't move leader");
            }
            info!("{}: Initial leader fully initialized", self.id);
            self.set_cluster_start().await?
        } else {
            self.get_cluster_start().await?
        };
        Self::wait_until_sync_time(cluster_start_time).await;
        Ok(())
    }

    async fn get_etcd_member_id(&mut self, member_name: String) -> Result<Option<u64>, EtcdError> {
        Ok(self
            .etcd
            .member_list()
            .await?
            .members()
            .iter()
            .find(|m| m.name() == member_name)
            .map(|m| m.id()))
    }

    async fn set_cluster_start(&mut self) -> Result<Timestamp, EtcdError> {
        info!("{}: Synchronizing cluster start", self.id);
        let cluster_sync_start = (Utc::now() + Duration::from_secs(4)).timestamp_millis();
        let start_str = cluster_sync_start.to_string();
        self.etcd.put("start", start_str, None).await?;
        Ok(cluster_sync_start)
    }

    async fn get_cluster_start(&mut self) -> Result<Timestamp, EtcdError> {
        info!("{}: Waiting for cluster start", self.id);
        // Set up watcher for start key
        let (mut watcher, mut watcher_stream) = self.etcd.watch("start", None).await?;
        // Check if start key is already set
        match self.etcd.get("start", None).await {
            Ok(resp) => {
                debug!("Received: {resp:?}");
                if let Some(keyvalue) = resp.kvs().get(0) {
                    let start_str = keyvalue.value_str()?;
                    match start_str.parse::<i64>() {
                        Ok(cluster_sync_start) => return Ok(cluster_sync_start),
                        Err(e) => panic!("Failed to parse cluster start time: {e}"),
                    };
                }
            }
            Err(e) => panic!("{e}"),
        }
        // Otherwise wait for update on start key
        while let Some(resp) = watcher_stream.message().await? {
            debug!("[{}] received watch response", resp.watch_id());
            if resp.canceled() {
                debug!("watch canceled: {}", resp.watch_id());
                break;
            }
            for event in resp.events() {
                debug!("watch event: {:?}", event);
                if let Some(kv) = event.kv() {
                    let start_str = kv.value_str()?;
                    match start_str.parse::<i64>() {
                        Ok(cluster_sync_start) => return Ok(cluster_sync_start),
                        Err(e) => panic!("Failed to parse cluster start time: {e}"),
                    };
                }
                if EventType::Delete == event.event_type() {
                    watcher.cancel_by_id(resp.watch_id()).await?;
                }
            }
        }
        panic!("Watch on cluster start key failed");
    }

    fn handle_server_message(&mut self, command_id: CommandId) {
        debug!("Recieved {command_id:?}");
        self.client_data.new_response(command_id);
    }

    async fn send_request(&mut self, is_write: bool) {
        // let key = self.next_request_id.to_string();
        let key = "Some key".to_string();
        let val = self.next_request_id.to_string();
        debug!("Sending {key:?}");
        let mut client = self.etcd.clone();
        let response_sender = self.response_sender.clone();
        let command_id = self.next_request_id;
        tokio::spawn(async move {
            if is_write {
                match client.put(key, val, None).await {
                    Ok(resp) => debug!("Received: {resp:?}"),
                    Err(e) => panic!("{e}"),
                }
            } else {
                match client.get(key, None).await {
                    Ok(resp) => debug!("Received: {resp:?}"),
                    Err(e) => panic!("{e}"),
                }
            };
            response_sender.send(command_id).await.unwrap();
        });
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
