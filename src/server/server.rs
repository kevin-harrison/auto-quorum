use crate::{
    database::Database,
    metrics::MetricsHeartbeatServer,
    network::Network,
    optimizer::{ClusterOptimizer, ClusterStrategy},
    read::QuorumReader,
};
use auto_quorum::common::{kv::*, messages::*};
use chrono::Utc;
use log::*;
use omnipaxos::{
    util::{FlexibleQuorum, LogEntry, NodeId},
    ClusterConfig, OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{sync::mpsc::Receiver, time::interval};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosServerConfig {
    pub cluster_name: String,
    pub location: String,
    pub initial_leader: Option<NodeId>,
    pub optimize: Option<bool>,
    pub optimize_threshold: Option<f64>,
    pub congestion_control: Option<bool>,
    pub local_deployment: Option<bool>,
    pub initial_read_strat: Option<Vec<ReadStrategy>>,
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    nodes: Vec<NodeId>,
    database: Database,
    network: Network,
    cluster_messages: Receiver<(NodeId, ClusterMessage)>,
    client_messages: Receiver<(ClientId, ClientMessage)>,
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
        let peers: Vec<u64> = nodes
            .iter()
            .cloned()
            .filter(|node| *node != server_id)
            .collect();
        let local_deployment = server_config.local_deployment.unwrap_or(false);
        let optimize = server_config.optimize.unwrap_or(true);
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let metrics_server = MetricsHeartbeatServer::new(server_id, nodes.clone());
        let init_read_quorum = omnipaxos.get_read_config().read_quorum_size;
        let init_read_strat = match server_config.initial_read_strat {
            Some(strats) => {
                assert_eq!(strats.len(), nodes.len());
                strats
            }
            None => vec![ReadStrategy::default(); nodes.len()],
        };
        let init_strat = ClusterStrategy {
            leader: server_id,
            read_quorum_size: init_read_quorum,
            write_quorum_size: nodes.len() - init_read_quorum + 1,
            read_strategies: init_read_strat,
        };
        let optimizer = ClusterOptimizer::new(nodes.clone());
        // TODO: make this configurable
        let initial_clients = if server_id == 1 || server_id == 2 {
            1
        } else {
            0
        };
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(1000);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(1000);
        let network = Network::new(
            server_config.cluster_name,
            server_id,
            peers,
            initial_clients,
            local_deployment,
            client_message_sender,
            cluster_message_sender,
        )
        .await;
        let mut server = OmniPaxosServer {
            id: server_id,
            nodes,
            database: Database::new(),
            network,
            cluster_messages,
            client_messages,
            omnipaxos,
            current_decided_idx: 0,
            quorum_reader: QuorumReader::new(server_id),
            metrics_server,
            optimizer,
            strategy: init_strat,
            optimize,
            optimize_threshold: server_config.optimize_threshold.unwrap_or(0.8),
        };
        server.send_outgoing_msgs();
        server
    }

    pub async fn run(&mut self, initial_leader: Option<NodeId>) {
        let buffer_size = 100;
        let mut client_message_buffer = Vec::with_capacity(buffer_size);
        let mut cluster_message_buffer = Vec::with_capacity(buffer_size);
        let mut election_interval = interval(Duration::from_millis(1000));
        let mut optimize_interval = interval(Duration::from_millis(1000));
        let mut init_leader_interval = interval(Duration::from_secs(20));
        let mut initialized_leader = match initial_leader {
            Some(_) => false,
            None => true,
        };
        election_interval.tick().await;
        optimize_interval.tick().await;
        init_leader_interval.tick().await;
        if self.id == 1 || self.id == 2 {
            let msg = ServerMessage::Ready;
            self.network.send_to_client(1, msg);
        }
        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => {
                    // TODO: revert BLE to stop tracking latency
                    if let Some(_latencies) = self.omnipaxos.tick() {
                        self.send_outgoing_msgs();
                    }
                },
                _ = optimize_interval.tick() => {
                    let requests = self.metrics_server.tick();
                    for (to, request) in requests {
                        let cluster_msg = ClusterMessage::MetricSync(request);
                        self.network.send_to_cluster(to, cluster_msg);
                        }
                    self.handle_optimize_timeout();
                },
                _ = init_leader_interval.tick(), if !initialized_leader => {
                    initialized_leader = true;
                    self.force_initial_leader_switch(initial_leader.unwrap());
                },
                _ = self.cluster_messages.recv_many(&mut cluster_message_buffer, buffer_size) => {
                        self.handle_cluster_messages(&mut cluster_message_buffer);
                    },
                _ = self.client_messages.recv_many(&mut client_message_buffer, buffer_size) => {
                    self.handle_client_messages(&mut client_message_buffer);
                },
            }
        }
    }

    fn handle_optimize_timeout(&mut self) {
        if !self.optimize {
            self.log(None, false, false);
            return;
        }
        self.strategy.read_quorum_size = self.omnipaxos.get_read_config().read_quorum_size;
        self.strategy.write_quorum_size = self.nodes.len() - self.strategy.read_quorum_size + 1;
        if let Some(leader) = self.omnipaxos.get_current_leader() {
            self.strategy.leader = leader;
            // Only leader needs to optimize but kept for logging purposes
            let cache_update = self
                .optimizer
                .update_latencies(&self.metrics_server.metrics.latencies);
            let current_workload = &self.metrics_server.metrics.workload;
            let current_strategy_latency = self
                .optimizer
                .score_strategy(current_workload, &self.strategy);
            let (optimal_strategy, optimal_strategy_latency) =
                self.optimizer.calculate_optimal_strategy(current_workload);
            if leader == self.id {
                let absolute_latency_improvement =
                    optimal_strategy_latency - current_strategy_latency;
                let relative_latency_improvement =
                    optimal_strategy_latency / current_strategy_latency;
                if absolute_latency_improvement < -2.
                    && relative_latency_improvement < self.optimize_threshold
                {
                    self.log(
                        Some((
                            &optimal_strategy,
                            optimal_strategy_latency,
                            current_strategy_latency,
                        )),
                        true,
                        cache_update,
                    );
                    self.reconfigure_strategy(optimal_strategy);
                } else {
                    self.log(
                        Some((
                            &self.strategy,
                            optimal_strategy_latency,
                            current_strategy_latency,
                        )),
                        false,
                        cache_update,
                    );
                }
            } else {
                self.log(None, false, cache_update)
            }
        }
    }

    fn reconfigure_strategy(&mut self, optimal_strategy: ClusterStrategy) {
        if optimal_strategy.leader != self.omnipaxos.get_current_leader().unwrap() {
            self.omnipaxos
                .relinquish_leadership(optimal_strategy.leader);
            self.send_outgoing_msgs();
        }
        if optimal_strategy.read_quorum_size != self.strategy.read_quorum_size {
            let write_quorum_size = self.nodes.len() - optimal_strategy.read_quorum_size + 1;
            let new_config = ClusterConfig {
                configuration_id: 1,
                nodes: self.nodes.clone(),
                flexible_quorum: Some(FlexibleQuorum {
                    read_quorum_size: optimal_strategy.read_quorum_size,
                    write_quorum_size,
                }),
                initial_leader: None,
            };
            self.omnipaxos
                .reconfigure_joint_consensus(new_config)
                .unwrap();
            self.send_outgoing_msgs();
        }
        if optimal_strategy.read_strategies != self.strategy.read_strategies {
            self.strategy.read_strategies = optimal_strategy.read_strategies.clone();
            self.send_strat();
        }
    }

    fn force_initial_leader_switch(&mut self, initial_leader: NodeId) {
        if let Some(current_leader) = self.omnipaxos.get_current_leader() {
            if current_leader == self.id {
                self.omnipaxos.relinquish_leadership(initial_leader);
                self.send_outgoing_msgs();
            }
        }
    }

    fn handle_decided_entries(&mut self) {
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
                LogEntry::Snapshotted(_) => unimplemented!(),
                _ => None,
            });
            let ready_reads = self.quorum_reader.rinse(new_decided_idx);
            self.update_database_and_respond(decided_commands.chain(ready_reads).collect());
        }
    }

    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here.
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network.send_to_client(command.client_id, response);
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) -> bool {
        let mut client_done = false;
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => match &kv_command {
                    KVCommand::Put(..) => {
                        self.metrics_server.local_write();
                        self.commit_command_to_log(from, command_id, kv_command);
                    }
                    KVCommand::Delete(_) => {
                        self.metrics_server.local_write();
                        self.commit_command_to_log(from, command_id, kv_command);
                    }
                    KVCommand::Get(_) => {
                        self.metrics_server.local_read();
                        self.handle_read_request(from, command_id, kv_command);
                    }
                },
                ClientMessage::Done => client_done = true,
            }
        }
        return client_done;
    }

    fn handle_cluster_messages(&mut self, messages: &mut Vec<(NodeId, ClusterMessage)>) -> bool {
        let mut server_done = false;
        for (from, message) in messages.drain(..) {
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.send_outgoing_msgs();
                    self.handle_decided_entries();
                }
                ClusterMessage::QuorumReadRequest(req) => {
                    self.handle_quorum_read_request(from, req)
                }
                ClusterMessage::QuorumReadResponse(resp) => {
                    self.handle_quorum_read_response(from, resp);
                }
                ClusterMessage::MetricSync(sync) => {
                    if let Some((to, reply)) = self.metrics_server.handle_metric_sync(from, sync) {
                        let cluster_msg = ClusterMessage::MetricSync(reply);
                        self.network.send_to_cluster(to, cluster_msg);
                    }
                }
                ClusterMessage::ReadStrategyUpdate(strat) => {
                    self.handle_read_strategy_update(from, strat);
                }
                ClusterMessage::Done => server_done = true,
            }
        }
        // TODO: check if its go to delay the outgoing + decided
        // if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
        //     self.handle_decided_entries();
        // }
        // self.send_outgoing_msgs();
        server_done
    }

    fn handle_read_request(
        &mut self,
        from: ClientId,
        command_id: CommandId,
        kv_command: KVCommand,
    ) {
        let read_strat = self.strategy.read_strategies[self.id as usize - 1];
        match read_strat {
            ReadStrategy::ReadAsWrite => self.commit_command_to_log(from, command_id, kv_command),
            ReadStrategy::QuorumRead => self.start_quorum_read(from, command_id, kv_command, false),
            ReadStrategy::BallotRead => self.start_quorum_read(from, command_id, kv_command, true),
        }
    }

    fn commit_command_to_log(
        &mut self,
        from: ClientId,
        command_id: CommandId,
        kv_command: KVCommand,
    ) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
        self.send_outgoing_msgs();
    }

    fn handle_quorum_read_request(&mut self, from: NodeId, request: QuorumReadRequest) {
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
        let cluster_msg = ClusterMessage::QuorumReadResponse(read_response);
        self.network.send_to_cluster(from, cluster_msg);
    }

    // TODO: if reads show us that something is chosen but not decided we can decide it and
    // apply chosen writes and rinse reads immediately.
    fn handle_quorum_read_response(&mut self, from: NodeId, response: QuorumReadResponse) {
        debug!("Got q response from {from}: {response:#?}");
        if let Some(ready_read) = self
            .quorum_reader
            .handle_response(response, self.current_decided_idx)
        {
            // TODO: remove allocation
            self.update_database_and_respond(vec![ready_read]);
        }
    }

    fn start_quorum_read(
        &mut self,
        client_id: ClientId,
        command_id: CommandId,
        read_command: KVCommand,
        enable_ballot_read: bool,
    ) {
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
            self.network.send_to_cluster(*peer, read_request);
        }
    }

    fn send_strat(&mut self) {
        let msg = ClusterMessage::ReadStrategyUpdate(self.strategy.read_strategies.clone());
        for peer in self.omnipaxos.get_peers() {
            self.network.send_to_cluster(*peer, msg.clone());
        }
    }

    // TODO: its possible for strategy updates to come in out of sync or get dropped
    fn handle_read_strategy_update(&mut self, _from: NodeId, read_strat: Vec<ReadStrategy>) {
        self.strategy.read_strategies = read_strat
    }

    fn log(
        &self,
        strategy: Option<(&ClusterStrategy, f64, f64)>,
        new_strat: bool,
        cache_update: bool,
    ) {
        let timestamp = Utc::now().timestamp_millis();
        let leader = self.omnipaxos.get_current_leader();
        let read_quorum = self.omnipaxos.get_read_config().read_quorum_size;
        let node_strat = leader.map(|l| {
            self.optimizer
                .get_optimal_node_strat(l, read_quorum, self.id)
        });
        let leader_json = serde_json::to_string(&leader).unwrap();
        let operation_latency_json = serde_json::to_string(&node_strat).unwrap();
        let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
        let opt_strategy_latency =
            strategy.map(|s| s.1 / self.metrics_server.metrics.get_total_load());
        let curr_strategy_latency =
            strategy.map(|s| s.2 / self.metrics_server.metrics.get_total_load());
        let strategy_json = match strategy {
            Some((strat, _, _)) => serde_json::to_string(strat).unwrap(),
            None => serde_json::to_string(&None::<ClusterStrategy>).unwrap(),
        };
        let opt_strategy_latency_json = serde_json::to_string(&opt_strategy_latency).unwrap();
        let curr_strategy_latency_json = serde_json::to_string(&curr_strategy_latency).unwrap();
        println!("{{ \"timestamp\": {timestamp}, \"new_strat\": {new_strat}, \"opt_strat_latency\": {opt_strategy_latency_json:<20}, \"curr_strat_latency\": {curr_strategy_latency_json:<20}, \"cluster_strategy\": {strategy_json:<200}, \"operation_latency\": {operation_latency_json:<100}, \"leader\": {leader_json}, \"metrics_update\": {cache_update}, \"cluster_metrics\": {metrics_json} }}");
    }
}
