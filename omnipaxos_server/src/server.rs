use chrono::Utc;
use futures::StreamExt;
use log::*;
use std::time::Duration;

use omnipaxos::{
    util::{FlexibleQuorum, LogEntry, NodeId},
    ClusterConfig, OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;

use crate::{
    database::Database, metrics::MetricsHeartbeatServer, optimizer::{self, ClusterStrategy}, read::QuorumReader, router::Router
};
use common::{kv::*, messages::*};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    nodes: Vec<NodeId>,
    database: Database,
    network: Router,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    current_leader: Option<NodeId>,
    quorum_reader: QuorumReader,
    metrics_server: MetricsHeartbeatServer,
    strategy: ClusterStrategy,
    optimize: bool,
}

impl OmniPaxosServer {
    pub async fn new(
        omnipaxos_config: OmniPaxosConfig,
        storage: MemoryStorage<Command>,
        router: Router,
        metrics_server: MetricsHeartbeatServer,
        optimize: bool,
    ) -> Self {
        let id = omnipaxos_config.server_config.pid;
        let nodes = omnipaxos_config.cluster_config.nodes.clone();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let initial_strategy = ClusterStrategy {
            leader: id,
            read_quorum_size: omnipaxos.get_read_config().read_quorum_size,
            read_strat: vec![ReadStrategy::default(); nodes.len()],
            average_latency_estimate: 0.,
        };
        OmniPaxosServer {
            id,
            nodes,
            database: Database::new(),
            network: router,
            omnipaxos,
            current_decided_idx: 0,
            current_leader: None,
            quorum_reader: QuorumReader::new(id),
            metrics_server, 
            strategy: initial_strategy,
            optimize,
        }
    }

    pub async fn run(&mut self) {
        let mut election_interval = tokio::time::interval(Duration::from_millis(1000));
        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => {
                    // TODO: revert BLE to stop tracking latency
                    if let Some(latencies) = self.omnipaxos.tick() {
                        self.current_leader = self.omnipaxos.get_current_leader();
                        // Update metrics
                        let requests = self.metrics_server.tick();
                        for (to, request) in requests {
                            let cluster_msg = ClusterMessage::MetricSync(request);
                            self.network
                                .send(Outgoing::ClusterMessage(to, cluster_msg))
                                .await;
                            }
                        // warn!("Metrics = \n{:?}", self.metrics_server.metrics);
                        // let timestamp = Utc::now().timestamp_millis();
                        // let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
                        // println!("{{ \"timestamp\": {timestamp}, \"cluster_metrics\": {metrics_json}}}");
                        // // Debug leader
                        // warn!("Leader = {:?} with ballot {:?}\n", self.current_leader, self.omnipaxos.get_promise());
                        // warn!("Readstrat = {:?}", self.strategy.read_strat);
                        // Check for strategy update
                        self.handle_optimize_timeout().await;
                        self.send_outgoing_msgs().await;
                    }
                },
                Some(msg) = self.network.next() => {
                    self.handle_incoming_msg(msg.unwrap()).await;
                },
            }
        }
    }

    async fn handle_optimize_timeout(&mut self) {
        if let Some(leader) = self.current_leader {
            // TODO: strategy updates should be atomic (possible if we put them in config log)
            self.strategy.leader = leader;
            self.strategy.read_quorum_size = self.omnipaxos.get_read_config().read_quorum_size;
            let new_strategy = optimizer::find_better_strategy(&self.metrics_server.metrics, &self.strategy);
            match (self.optimize, leader == self.id, new_strategy) {
                (true, true, Some(strategy)) => {
                    // Log server state
                    let timestamp = Utc::now().timestamp_millis();
                    let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
                    let strategy_json = serde_json::to_string(&strategy).unwrap();
                    println!("{{ \"timestamp\": {timestamp}, \"cluster_metrics\": {metrics_json:<300}, \"cluster_strategy\": {strategy_json:<150}, \"leader\": {leader}, \"new_strat\": {}}}", true);
                    if strategy.leader != self.id {
                        self.omnipaxos.relinquish_leadership(strategy.leader);
                        self.send_outgoing_msgs().await;
                    }
                    // Adopt new strategy
                    if strategy.read_quorum_size != self.strategy.read_quorum_size {
                        let write_quorum_size =
                            (self.nodes.len() - strategy.read_quorum_size) + 1;
                        let new_config = ClusterConfig {
                            configuration_id: 1,
                            nodes: self.nodes.clone(),
                            flexible_quorum: Some(FlexibleQuorum {
                                read_quorum_size: strategy.read_quorum_size,
                                write_quorum_size,
                            }),
                        };
                        self.omnipaxos
                            .reconfigure_joint_consensus(new_config)
                            .unwrap();
                        self.send_outgoing_msgs().await;
                    }
                    if strategy.read_strat != self.strategy.read_strat {
                        self.strategy.read_strat = strategy.read_strat;
                        // TODO: what if this gets dropped?
                        self.send_strat().await;
                    }
                },
                _ => {
                    // Log server state
                    let timestamp = Utc::now().timestamp_millis();
                    let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
                    let strategy_json = serde_json::to_string(&self.strategy).unwrap();
                    println!("{{ \"timestamp\": {timestamp}, \"cluster_metrics\": {metrics_json:<300}, \"cluster_strategy\": {strategy_json:<150}, \"leader\": {leader}, \"new_strat\": {}}}", false);
                },
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            let decided_commands = decided_entries.into_iter().filter_map(|e| match e {
                LogEntry::Decided(cmd) => Some(cmd),
                // TODO: handle snapshotted entries
                _ => None,
            });
            let ready_reads = self.quorum_reader.rinse(new_decided_idx);
            self.update_database_and_respond(decided_commands.chain(ready_reads).collect())
                .await;
            // TODO: Fix shapshotting
            // // snapshotting
            // if new_decided_idx % 5 == 0 {
            //     debug!(
            //         "Log before: {:?}",
            //         self.omnipaxos.read_decided_suffix(0).unwrap()
            //     );
            //     self.omnipaxos
            //         .snapshot(Some(new_decided_idx), true)
            //         .expect("Failed to snapshot");
            //     debug!(
            //         "Log after: {:?}\n",
            //         self.omnipaxos.read_decided_suffix(0).unwrap()
            //     );
            // }
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here.
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            // TODO: should leader respond here since it has less latency for decided
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network
                    .send(Outgoing::ServerMessage(command.client_id, response))
                    .await;
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network
                .send(Outgoing::ClusterMessage(to, cluster_msg))
                .await;
        }
    }

    async fn handle_incoming_msg(&mut self, msg: Incoming) {
        match msg {
            Incoming::ClientMessage(from, request) => {
                self.handle_client_request(from, request).await;
            }
            Incoming::ClusterMessage(_from, ClusterMessage::OmniPaxosMessage(m)) => {
                self.omnipaxos.handle_incoming(m);
                self.send_outgoing_msgs().await;
                self.handle_decided_entries().await;
            }
            Incoming::ClusterMessage(from, ClusterMessage::QuorumReadRequest(req)) => {
                self.handle_quorum_read_request(from, req).await;
            }
            Incoming::ClusterMessage(_from, ClusterMessage::QuorumReadResponse(resp)) => {
                self.handle_quorum_read_response(resp).await;
            }
            Incoming::ClusterMessage(from, ClusterMessage::MetricSync(sync)) => {
                if let Some((to, reply)) = self.metrics_server.handle_metric_sync(from, sync) {
                    self.network.send(Outgoing::ClusterMessage(to, ClusterMessage::MetricSync(reply))).await;
                }
            }
            Incoming::ClusterMessage(from, ClusterMessage::ReadStrategyUpdate(strat)) => {
                self.handle_read_strategy_update(from, strat);
            }
        }
    }

    async fn handle_client_request(&mut self, from: ClientId, request: ClientMessage) {
        match request {
            ClientMessage::Append(command_id, kv_command) => {
                let read_strat = self.strategy.read_strat[self.id as usize - 1];
                match (&kv_command, read_strat) {
                    (KVCommand::Get(_), ReadStrategy::QuorumRead) => {
                        self.metrics_server.local_read();
                        self.start_quorum_read(from, command_id, kv_command).await;
                    }
                    _ => {
                        self.metrics_server.local_write();
                        let command = Command {
                            client_id: from,
                            coordinator_id: self.id,
                            id: command_id,
                            kv_cmd: kv_command,
                        };
                        self.omnipaxos
                            .append(command)
                            .expect("Append to Omnipaxos log failed");
                        self.send_outgoing_msgs().await;
                    }
                }
            }
        };
    }

    async fn handle_quorum_read_request(&mut self, from: NodeId, request: QuorumReadRequest) {
        let read_response = QuorumReadResponse {
            client_id: request.client_id,
            command_id: request.command_id,
            read_quorum_config: self.omnipaxos.get_read_config(),
            accepted_idx: self.omnipaxos.get_accepted_idx(),
        };
        let response =
            Outgoing::ClusterMessage(from, ClusterMessage::QuorumReadResponse(read_response));
        self.network.send(response).await;
    }

    // TODO: if reads show us that something is chosen but not decided we can decide it and
    // apply chosen writes and rinse reads immediately.
    async fn handle_quorum_read_response(&mut self, response: QuorumReadResponse) {
        debug!("Got q response: {response:#?}");
        if let Some(ready_read) = self
            .quorum_reader
            .handle_response(response, self.current_decided_idx)
        {
            self.update_database_and_respond(vec![ready_read]).await;
        }
    }

    fn handle_read_strategy_update(&mut self, from: NodeId, read_strat: Vec<ReadStrategy>) {
        if let Some(leader) = self.current_leader {
            if from == leader {
                self.strategy.read_strat = read_strat
            }
        }
    }

    // TODO: client sends to closest replica which might be the leader
    async fn start_quorum_read(
        &mut self,
        client_id: ClientId,
        command_id: CommandId,
        read_command: KVCommand,
    ) {
        warn!("Starting q read: {read_command:?}");
        // Get local info
        let accepted_idx = self.omnipaxos.get_accepted_idx();
        let read_quorum_config = self.omnipaxos.get_read_config();
        self.quorum_reader.new_read(
            client_id,
            command_id,
            read_command,
            read_quorum_config,
            accepted_idx,
        );

        // Send quorum read to followers
        // TODO: thrifty messaging
        let leader = self.current_leader;
        let followers = self.omnipaxos.get_peers().iter().filter(|id| match leader {
            Some(leader_id) => **id != leader_id,
            None => true,
        });
        debug!("Sending q request ({client_id}, {command_id}) to {followers:?}");
        for peer in followers {
            let read_request = ClusterMessage::QuorumReadRequest(QuorumReadRequest {
                client_id,
                command_id,
            });
            let msg = Outgoing::ClusterMessage(*peer, read_request);
            self.network.send(msg).await;
        }
    }

    async fn send_strat(&mut self) {
        let msg = ClusterMessage::ReadStrategyUpdate(self.strategy.read_strat.clone());
        for peer in self.omnipaxos.get_peers() {
            self.network
                .send(Outgoing::ClusterMessage(*peer, msg.clone()))
                .await;
        }
    }
}
