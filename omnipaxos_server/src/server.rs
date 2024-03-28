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
    database::Database, metrics::MetricsHeartbeatServer, optimizer::{self, ClusterOptimizer, ClusterStrategy, ReadStrategy}, read::QuorumReader, router::Router
};
use common::{kv::*, messages::*};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosServerConfig {
    pub location: String,
    pub initial_leader: Option<NodeId>,
    pub optimize: Option<bool>,
    pub congestion_control: Option<bool>,
    pub local_deployment: Option<bool>,
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    nodes: Vec<NodeId>,
    database: Database,
    network: Router,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    quorum_reader: QuorumReader,
    metrics_server: MetricsHeartbeatServer,
    optimizer: ClusterOptimizer,
    strategy: ClusterStrategy,
    optimize: bool,
}

impl OmniPaxosServer {
    pub async fn new(
        server_config: OmniPaxosServerConfig,
        omnipaxos_config: OmniPaxosConfig,
    ) -> Self {
        let server_id = omnipaxos_config.server_config.pid;
        let nodes = omnipaxos_config.cluster_config.nodes.clone();
        let local_deployment = server_config.local_deployment.unwrap_or(false);
        let congestion_control = server_config.congestion_control.unwrap_or(false);
        let optimize = server_config.optimize.unwrap_or(true);
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let router = Router::new(server_id, nodes.clone(), local_deployment, congestion_control).await.unwrap();
        let metrics_server = MetricsHeartbeatServer::new(server_id, nodes.clone());
        let initial_strategy = ClusterStrategy::new(server_id, omnipaxos.get_read_config().read_quorum_size);
        let optimizer = ClusterOptimizer::new(nodes.clone(), initial_strategy.clone());
        let mut server = OmniPaxosServer {
            id: server_id,
            nodes,
            database: Database::new(),
            network: router,
            omnipaxos,
            current_decided_idx: 0,
            quorum_reader: QuorumReader::new(server_id),
            metrics_server, 
            strategy: initial_strategy,
            optimizer,
            optimize,
        };
        server.send_outgoing_msgs().await;
        server
    }

    pub async fn run(&mut self, initial_leader: Option<NodeId>) {
        let mut election_interval = tokio::time::interval(Duration::from_millis(1000));
        let mut optimize_interval = tokio::time::interval(Duration::from_millis(1000));
        let mut init_leader_interval = tokio::time::interval(Duration::from_secs(5));
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
            self.print_metrics(false, false, false);
            return;
        }
        if let Some(leader) = self.omnipaxos.get_current_leader() {
            self.strategy.leader = leader;
            self.strategy.read_quorum_size = self.omnipaxos.get_read_config().read_quorum_size;
            let optimal_strategy = self.optimizer.update_optimal_strategy(&self.metrics_server.metrics);
            let new_node_strategies = self.strategy.node_strategies != optimal_strategy.node_strategies;
            let new_optimal_leader = self.strategy.leader != optimal_strategy.leader;
            let new_optimal_quorum = self.strategy.read_quorum_size != optimal_strategy.read_quorum_size;
            let optimal_read_quorum_size = optimal_strategy.read_quorum_size;
            if new_node_strategies {
                // Update node strategies locally since it doesn't matter for safety
                self.strategy.node_strategies = optimal_strategy.node_strategies.clone();
            }
            // Only the leader should try to reconfigure the log
            if leader != self.id {
                self.print_metrics(false, false, new_node_strategies);
                return;
            }
            if new_optimal_leader { 
                self.omnipaxos.relinquish_leadership(optimal_strategy.leader);
                self.send_outgoing_msgs().await;
            }
            if new_optimal_quorum {
                let write_quorum_size =
                    (self.nodes.len() - optimal_read_quorum_size) + 1;
                let new_config = ClusterConfig {
                    configuration_id: 1,
                    nodes: self.nodes.clone(),
                    flexible_quorum: Some(FlexibleQuorum {
                        read_quorum_size: optimal_read_quorum_size,
                        write_quorum_size,
                    }),
                };
                self.omnipaxos
                    .reconfigure_joint_consensus(new_config)
                    .unwrap();
                self.send_outgoing_msgs().await;
            }
            self.print_metrics(new_optimal_leader, new_optimal_quorum, new_node_strategies);

            // let new_strategy = optimizer::find_better_strategy(&self.metrics_server.metrics, &mut self.strategy);
            // match (self.optimize, leader == self.id, new_strategy) {
            //     (true, true, Some(strategy)) => {
            //         // Log server state
            //         let timestamp = Utc::now().timestamp_millis();
            //         let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
            //         let strategy_json = serde_json::to_string(&strategy).unwrap();
            //         println!("{{ \"timestamp\": {timestamp}, \"cluster_metrics\": {metrics_json:<500}, \"cluster_strategy\": {strategy_json:<250}, \"leader\": {leader}, \"new_strat\": {}}}", true);
            //         if strategy.leader != self.id {
            //             self.omnipaxos.relinquish_leadership(strategy.leader);
            //             self.send_outgoing_msgs().await;
            //         }
            //         // Adopt new strategy
            //         if strategy.read_quorum_size != self.strategy.read_quorum_size {
            //             let write_quorum_size =
            //                 (self.nodes.len() - strategy.read_quorum_size) + 1;
            //             let new_config = ClusterConfig {
            //                 configuration_id: 1,
            //                 nodes: self.nodes.clone(),
            //                 flexible_quorum: Some(FlexibleQuorum {
            //                     read_quorum_size: strategy.read_quorum_size,
            //                     write_quorum_size,
            //                 }),
            //             };
            //             self.omnipaxos
            //                 .reconfigure_joint_consensus(new_config)
            //                 .unwrap();
            //             self.send_outgoing_msgs().await;
            //         }
            //         if strategy.read_strat != self.strategy.read_strat {
            //             self.strategy.read_strat = strategy.read_strat;
            //             // TODO: what if this gets dropped?
            //             self.send_strat().await;
            //         }
            //     },
            //     _ => {
            //         let timestamp = Utc::now().timestamp_millis();
            //         let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
            //         let strategy_json = serde_json::to_string(&self.strategy).unwrap();
            //         println!("{{ \"timestamp\": {timestamp}, \"cluster_metrics\": {metrics_json:<300}, \"cluster_strategy\": {strategy_json:<150}, \"leader\": {leader}, \"new_strat\": {}}}", false);
            //     },
            // }
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
        let read_strat = self.strategy.node_strategies[self.id as usize - 1].read_strategy;
        let i_am_leading = match self.omnipaxos.get_current_leader() {
            Some(leader) => leader == self.id,
            None => false,
        };
        match read_strat {
            ReadStrategy::ReadAsWrite => self.commit_command_to_log(from, command_id, kv_command).await,
            ReadStrategy::QuorumRead => self.start_quorum_read(from, command_id, kv_command).await,
            // TODO: Add functinality to reader.rs before we can do this
            ReadStrategy::LeaderRead if i_am_leading => unimplemented!(),
            ReadStrategy::ProxiedLeaderRead if !i_am_leading => unimplemented!(),
            _ => unimplemented!(),
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

    async fn start_quorum_read(&mut self, client_id: ClientId, command_id: CommandId, read_command: KVCommand) {
        debug!("Starting q read: {read_command:?}");
        let accepted_idx = self.omnipaxos.get_accepted_idx();
        let read_quorum_config = self.omnipaxos.get_read_config();
        self.quorum_reader.new_read(
            client_id,
            command_id,
            read_command,
            read_quorum_config,
            accepted_idx,
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

    fn print_metrics(&self, new_leader: bool , new_quorum: bool, new_strat: bool) {
        let leader = self.omnipaxos.get_current_leader();
        let timestamp = Utc::now().timestamp_millis();
        let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
        let strategy_json = serde_json::to_string(&self.strategy).unwrap();
        println!("{{ \"timestamp\": {timestamp}, \"cluster_metrics\": {metrics_json:<500}, \"cluster_strategy\": {strategy_json:<500}, \"leader\": {leader:?}, \"new_leader\": {new_leader}, \"new_quorum\": {new_quorum}, \"new_strat\":{new_strat} }}");
    }
}
