use chrono::Utc;
use futures::StreamExt;
use log::*;
use std::time::Duration;
use serde::{Serialize, Deserialize};

use omnipaxos::{
    util::{FlexibleQuorum, LogEntry, NodeId},
    ClusterConfig, OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;

use crate::{
    database::Database, metrics::MetricsHeartbeatServer, optimizer::{ClusterOptimizer, ClusterStrategy}, read::QuorumReader, network::Network
};
use common::{kv::*, messages::*};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosServerConfig {
    pub location: String,
    pub initial_leader: Option<NodeId>,
    pub optimize: Option<bool>,
    pub optimize_threshold: Option<f64>,
    pub congestion_control: Option<bool>,
    pub local_deployment: Option<bool>,
    pub initial_read_strat: Option<ReadStrategy>,
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    nodes: Vec<NodeId>,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    quorum_reader: QuorumReader,
    metrics_server: MetricsHeartbeatServer,
    optimizer: ClusterOptimizer,
    strategy: ClusterStrategy,
    optimize: bool,
    optimize_threshold: f64,
}

impl OmniPaxosServer {
    pub async fn new(
        server_config: OmniPaxosServerConfig,
        omnipaxos_config: OmniPaxosConfig,
    ) -> Self {
        let server_id = omnipaxos_config.server_config.pid;
        let nodes = omnipaxos_config.cluster_config.nodes.clone();
        let local_deployment = server_config.local_deployment.unwrap_or(false);
        let optimize = server_config.optimize.unwrap_or(true);
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let network = Network::new(server_id, nodes.clone(), local_deployment).await.unwrap();
        let metrics_server = MetricsHeartbeatServer::new(server_id, nodes.clone());
        let init_read_quorum = omnipaxos.get_read_config().read_quorum_size;
        let init_read_strat = server_config.initial_read_strat.unwrap_or_default();
        let init_strat = ClusterStrategy {
            leader: server_id,
            read_quorum_size: init_read_quorum,
            write_quorum_size: nodes.len() - init_read_quorum + 1,
            read_strategies: vec![init_read_strat; nodes.len()],
        };
        let optimizer = ClusterOptimizer::new(nodes.clone());
        let mut server = OmniPaxosServer {
            id: server_id,
            nodes,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            quorum_reader: QuorumReader::new(server_id),
            metrics_server, 
            optimizer,
            strategy: init_strat,
            optimize,
            optimize_threshold: server_config.optimize_threshold.unwrap_or(0.8),
        };
        server.send_outgoing_msgs().await;
        server
    }

    pub async fn run(&mut self, initial_leader: Option<NodeId>) {
        let mut election_interval = tokio::time::interval(Duration::from_millis(1000));
        let mut optimize_interval = tokio::time::interval(Duration::from_millis(1000));
        let mut init_leader_interval = tokio::time::interval(Duration::from_secs(10));
        let mut initialized_leader = match initial_leader {
            Some(_) => false,
            None => true,
        };
        election_interval.tick().await;
        optimize_interval.tick().await;
        init_leader_interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => {
                    // TODO: revert BLE to stop tracking latency
                    if let Some(latencies) = self.omnipaxos.tick() {
                        self.send_outgoing_msgs().await;
                    }
                },
                _ = optimize_interval.tick() => {
                    let requests = self.metrics_server.tick();
                    for (to, request) in requests {
                        let cluster_msg = ClusterMessage::MetricSync(request);
                        self.network
                            .send(Outgoing::ClusterMessage(to, cluster_msg))
                            .await;
                        }
                    self.handle_optimize_timeout().await;
                },
                _ = init_leader_interval.tick(), if !initialized_leader => {
                    initialized_leader = true;
                    self.force_initial_leader_switch(initial_leader.unwrap()).await;
                }
                Some(msg) = self.network.next() => {
                    self.handle_incoming_msg(msg.unwrap()).await;
                },
            }
        }
    }

    async fn handle_optimize_timeout(&mut self) {
        if !self.optimize {
            self.log(None, false, false);
            return;
        }
        self.strategy.read_quorum_size = self.omnipaxos.get_read_config().read_quorum_size;
        self.strategy.write_quorum_size = self.nodes.len() - self.strategy.read_quorum_size + 1;
        if let Some(leader) = self.omnipaxos.get_current_leader() {
            self.strategy.leader = leader;
            // Only leader needs to optimize but kept for logging purposes
            let cache_update = self.optimizer.update_latencies(&self.metrics_server.metrics.latencies);
            let current_workload = &self.metrics_server.metrics.workload;
            let current_strategy_latency = self.optimizer.score_strategy(current_workload, &self.strategy);
            let (optimal_strategy, optimal_strategy_latency) = self.optimizer.calculate_optimal_strategy(current_workload);
            if leader == self.id {
                let absolute_latency_improvement = optimal_strategy_latency - current_strategy_latency;
                let relative_latency_improvement = optimal_strategy_latency / current_strategy_latency;
                if absolute_latency_improvement < -2. && relative_latency_improvement < self.optimize_threshold {
                    self.log(Some((&optimal_strategy, optimal_strategy_latency, current_strategy_latency)), true, cache_update);
                    self.reconfigure_strategy(optimal_strategy).await;
                } else {
                    self.log(Some((&self.strategy, optimal_strategy_latency, current_strategy_latency)), false, cache_update);
                }
            } else {
                self.log(None, false, cache_update)
            }
        }
    }

    async fn reconfigure_strategy(&mut self, optimal_strategy: ClusterStrategy) {
        if optimal_strategy.leader != self.omnipaxos.get_current_leader().unwrap() {
            self.omnipaxos.relinquish_leadership(optimal_strategy.leader);
            self.send_outgoing_msgs().await;
        }
        if optimal_strategy.read_quorum_size != self.strategy.read_quorum_size {
            let write_quorum_size =
                self.nodes.len() - optimal_strategy.read_quorum_size + 1;
            let new_config = ClusterConfig {
                configuration_id: 1,
                nodes: self.nodes.clone(),
                flexible_quorum: Some(FlexibleQuorum {
                    read_quorum_size: optimal_strategy.read_quorum_size,
                    write_quorum_size,
                }),
            };
            self.omnipaxos
                .reconfigure_joint_consensus(new_config)
                .unwrap();
            self.send_outgoing_msgs().await;
        }
        if optimal_strategy.read_strategies != self.strategy.read_strategies {
            self.strategy.read_strategies = optimal_strategy.read_strategies.clone();
            self.send_strat().await;
        }
    }

    async fn force_initial_leader_switch(&mut self, initial_leader: NodeId) {
        if let Some(current_leader) = self.omnipaxos.get_current_leader() {
            if current_leader == self.id {
                self.omnipaxos.relinquish_leadership(initial_leader);
                self.send_outgoing_msgs().await;
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
            log::debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries.into_iter().filter_map(|e| match e {
                LogEntry::Decided(cmd) => Some(cmd),
                // TODO: handle snapshotted entries
                _ => None,
            });
            let ready_reads = self.quorum_reader.rinse(new_decided_idx);
            self.update_database_and_respond(decided_commands.chain(ready_reads).collect())
                .await;
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here.
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
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
            Incoming::ClusterMessage(from, ClusterMessage::QuorumReadResponse(resp)) => {
                self.handle_quorum_read_response(from, resp).await;
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
                match &kv_command {
                    KVCommand::Put(..) => {
                        self.metrics_server.local_write();
                        self.commit_command_to_log(from, command_id, kv_command).await;
                    }
                    KVCommand::Delete(_) => {
                        self.metrics_server.local_write();
                        self.commit_command_to_log(from, command_id, kv_command).await;
                    }
                    KVCommand::Get(_) => {
                        self.metrics_server.local_read();
                        self.handle_read_request(from, command_id, kv_command).await;
                    }
                }
            }
        };
    }

    async fn handle_read_request(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let read_strat = self.strategy.read_strategies[self.id as usize - 1];
        match read_strat {
            ReadStrategy::ReadAsWrite => self.commit_command_to_log(from, command_id, kv_command).await,
            ReadStrategy::QuorumRead => self.start_quorum_read(from, command_id, kv_command, false).await,
            ReadStrategy::BallotRead => self.start_quorum_read(from, command_id, kv_command, true).await,
        }
    }

    async fn commit_command_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
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

    async fn handle_quorum_read_request(&mut self, from: NodeId, request: QuorumReadRequest) {
        let read_response = QuorumReadResponse::new(
            self.id,
            request.client_id,
            request.command_id,
            self.omnipaxos.get_read_config(),
            self.omnipaxos.get_accepted_idx(),
            self.omnipaxos.get_promise(),
            self.omnipaxos.get_current_leader().unwrap_or(0),
            self.omnipaxos.get_decided_idx(),
            self.omnipaxos.get_max_prom_acc_idx(),
        );
        let response =
            Outgoing::ClusterMessage(from, ClusterMessage::QuorumReadResponse(read_response));
        self.network.send(response).await;
    }

    // TODO: if reads show us that something is chosen but not decided we can decide it and
    // apply chosen writes and rinse reads immediately.
    async fn handle_quorum_read_response(&mut self, from: NodeId, response: QuorumReadResponse) {
        debug!("Got q response from {from}: {response:#?}");
        if let Some(ready_read) = self
            .quorum_reader
            .handle_response(response, self.current_decided_idx)
        {
            self.update_database_and_respond(vec![ready_read]).await;
        }
    }

    async fn start_quorum_read(&mut self, client_id: ClientId, command_id: CommandId, read_command: KVCommand, enable_ballot_read: bool) {
        debug!("Starting q read: {read_command:?}");
        self.quorum_reader.new_read(
            client_id,
            command_id,
            read_command,
            self.omnipaxos.get_read_config(),
            self.omnipaxos.get_accepted_idx(),
            self.omnipaxos.get_promise(),
            self.omnipaxos.get_current_leader().unwrap_or(0),
            self.omnipaxos.get_decided_idx(),
            self.omnipaxos.get_max_prom_acc_idx(),
            enable_ballot_read,
        );
        // TODO: thrifty messaging
        for peer in self.omnipaxos.get_peers() {
            let read_request = ClusterMessage::QuorumReadRequest(QuorumReadRequest {
                client_id,
                command_id,
            });
            let msg = Outgoing::ClusterMessage(*peer, read_request);
            self.network.send(msg).await;
        }
    }

    async fn send_strat(&mut self) {
        let msg = ClusterMessage::ReadStrategyUpdate(self.strategy.read_strategies.clone());
        for peer in self.omnipaxos.get_peers() {
            self.network.send(Outgoing::ClusterMessage(*peer, msg.clone())).await;
        }
    }

    // TODO: its possible for strategy updates to come in out of sync or get dropped
    fn handle_read_strategy_update(&mut self, _from: NodeId, read_strat: Vec<ReadStrategy>) {
        self.strategy.read_strategies = read_strat
    }
 
    fn log(&self, strategy: Option<(&ClusterStrategy, f64, f64)>, new_strat: bool, cache_update: bool) {
        let timestamp = Utc::now().timestamp_millis();
        let leader = self.omnipaxos.get_current_leader();
        let read_quorum = self.omnipaxos.get_read_config().read_quorum_size;
        let node_strat = leader.map(|l| self.optimizer.get_optimal_node_strat(l, read_quorum, self.id));
        let leader_json = serde_json::to_string(&leader).unwrap();
        let operation_latency_json = serde_json::to_string(&node_strat).unwrap();
        let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
        let opt_strategy_latency = strategy.map(|s| s.1 / self.metrics_server.metrics.get_total_load());
        let curr_strategy_latency = strategy.map(|s| s.2 / self.metrics_server.metrics.get_total_load());
        let strategy_json = match strategy {
            Some((strat, _, _)) => serde_json::to_string(strat).unwrap(),
            None => serde_json::to_string(&None::<ClusterStrategy>).unwrap(),
        };
        let opt_strategy_latency_json = serde_json::to_string(&opt_strategy_latency).unwrap();
        let curr_strategy_latency_json = serde_json::to_string(&curr_strategy_latency).unwrap();
        println!("{{ \"timestamp\": {timestamp}, \"new_strat\": {new_strat}, \"opt_strat_latency\": {opt_strategy_latency_json:<20}, \"curr_strat_latency\": {curr_strategy_latency_json:<20}, \"cluster_strategy\": {strategy_json:<200}, \"operation_latency\": {operation_latency_json:<100}, \"leader\": {leader_json}, \"metrics_update\": {cache_update}, \"cluster_metrics\": {metrics_json} }}");
    }
}
