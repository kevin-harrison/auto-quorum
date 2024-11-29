use crate::{
    configs::AutoQuorumServerConfig,
    database::Database,
    metrics::MetricsHeartbeatServer,
    network::Network,
    optimizer::{ClusterOptimizer, ClusterStrategy},
    read::QuorumReader,
};
use auto_quorum::common::{kv::*, messages::*, utils::Timestamp};
use chrono::Utc;
use log::*;
use omnipaxos::{
    util::{FlexibleQuorum, LogEntry, NodeId},
    ClusterConfig, OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::time::Duration;
use tokio::time::interval;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const OPTIMIZE_TIMEOUT: Duration = Duration::from_secs(1);

pub struct OmniPaxosServer {
    id: NodeId,
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
    experiment_state: ExperimentState,
    config: AutoQuorumServerConfig,
}

impl OmniPaxosServer {
    pub async fn new(config: AutoQuorumServerConfig) -> Self {
        let init_strat = ClusterStrategy::initial_strategy(config.clone());
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let metrics_server = MetricsHeartbeatServer::new(config.server_id, config.nodes.clone());
        let optimizer = ClusterOptimizer::new(config.nodes.clone());
        let network = Network::new(
            config.cluster_name.clone(),
            config.server_id,
            config.nodes.clone(),
            config.num_clients,
            config.local_deployment.unwrap_or(false),
            NETWORK_BATCH_SIZE,
        )
        .await;
        let mut server = OmniPaxosServer {
            id: config.server_id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            quorum_reader: QuorumReader::new(config.server_id),
            metrics_server,
            optimizer,
            strategy: init_strat,
            optimize: config.optimize.unwrap_or(true),
            optimize_threshold: config.optimize_threshold.unwrap_or(0.8),
            experiment_state: ExperimentState::initial_state(config.clone()),
            config,
        };
        // Clears outgoing_messages of initial BLE messages
        let _ = server.omnipaxos.outgoing_messages();
        server
    }

    pub async fn run(&mut self) {
        if self.config.num_clients == 0 {
            self.experiment_state.node_finished(self.id);
            for peer in self.omnipaxos.get_peers() {
                let done_msg = ClusterMessage::Done;
                self.network.send_to_cluster(*peer, done_msg);
            }
        }
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election and instead force an initial leader
        // Once the leader is established it chooses a synchronization point which the
        // followers relay to their clients to begin the experiment.
        if self.config.initial_leader == self.id {
            self.become_initial_leader(&mut cluster_msg_buf).await;
            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
            self.send_cluster_start_signals(experiment_sync_start);
            self.send_client_start_signals(experiment_sync_start);
        }
        let mut optimize_interval = interval(OPTIMIZE_TIMEOUT);
        optimize_interval.tick().await;
        // Main event loop
        loop {
            tokio::select! {
                _ = optimize_interval.tick() => {
                    let requests = self.metrics_server.tick();
                    for (to, request) in requests {
                        let cluster_msg = ClusterMessage::MetricSync(request);
                        self.network.send_to_cluster(to, cluster_msg);
                        }
                    self.handle_optimize_timeout();
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                        self.handle_cluster_messages(&mut cluster_msg_buf).await;
                    },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
            }
            if self.experiment_state.is_finished() {
                self.network.shutdown().await;
                break;
            }
        }
    }

    // Ensures cluster is connected and leader is promoted before returning.
    async fn become_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
    ) {
        let mut leader_attempt = 0;
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick() => {
                    self.take_leadership(&mut leader_attempt).await;
                    if self.omnipaxos.is_accept_phase_leader() {
                        info!("{}: Leader fully initialized", self.id);
                        break;
                    }
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(cluster_msg_buffer).await;
                },
            }
        }
    }

    async fn take_leadership(&mut self, leader_attempt: &mut u32) {
        if self.omnipaxos.is_accept_phase_leader() {
            return;
        }
        let mut ballot = omnipaxos::ballot_leader_election::Ballot::default();
        *leader_attempt += 1;
        ballot.n = *leader_attempt;
        ballot.pid = self.id;
        ballot.config_id = 1;
        info!(
            "Node: {:?}, Initializing prepare phase with ballot: {:?}",
            self.id, ballot
        );
        self.omnipaxos.initialize_prepare_phase(ballot);
        self.send_outgoing_msgs();
    }

    fn handle_optimize_timeout(&mut self) {
        if !self.optimize {
            self.log(None, false, false);
            return;
        }
        self.update_current_strategy();
        let leader = self
            .omnipaxos
            .get_current_leader()
            .expect("Leader should be inited");
        // Only leader needs to optimize but kept for logging purposes
        let cache_update = self
            .optimizer
            .update_latencies(&self.metrics_server.metrics.latencies);
        let curr_workload = &self.metrics_server.metrics.workload;
        let curr_strat_latency = self.optimizer.score_strategy(curr_workload, &self.strategy);
        let (optimal_strategy, optimal_strat_latency) =
            self.optimizer.calculate_optimal_strategy(curr_workload);
        if leader == self.id {
            if self.reconfigure_threshold(optimal_strat_latency, curr_strat_latency) {
                self.log(
                    Some((&optimal_strategy, optimal_strat_latency, curr_strat_latency)),
                    true,
                    cache_update,
                );
                self.reconfigure_strategy(optimal_strategy);
            } else {
                self.log(
                    Some((&self.strategy, optimal_strat_latency, curr_strat_latency)),
                    false,
                    cache_update,
                );
            }
        } else {
            self.log(None, false, cache_update)
        }
    }

    fn update_current_strategy(&mut self) {
        let read_quorum = self.omnipaxos.get_read_config().read_quorum_size;
        let write_quorum = self.config.nodes.len() - read_quorum + 1;
        let leader = self
            .omnipaxos
            .get_current_leader()
            .expect("Leader should be inited");
        self.strategy.read_quorum_size = read_quorum;
        self.strategy.write_quorum_size = write_quorum;
        self.strategy.leader = leader;
    }

    fn reconfigure_threshold(&self, optimal_strat_latency: f64, curr_strat_latency: f64) -> bool {
        let absolute_latency_improvement = optimal_strat_latency - curr_strat_latency;
        let relative_latency_improvement = optimal_strat_latency / curr_strat_latency;
        absolute_latency_improvement < -2. && relative_latency_improvement < self.optimize_threshold
    }

    fn reconfigure_strategy(&mut self, optimal_strategy: ClusterStrategy) {
        if optimal_strategy.leader != self.omnipaxos.get_current_leader().unwrap() {
            self.omnipaxos
                .relinquish_leadership(optimal_strategy.leader);
            self.send_outgoing_msgs();
        }
        if optimal_strategy.read_quorum_size != self.strategy.read_quorum_size {
            let write_quorum_size = self.config.nodes.len() - optimal_strategy.read_quorum_size + 1;
            let new_config = ClusterConfig {
                configuration_id: 1,
                nodes: self.config.nodes.clone(),
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

    fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries.into_iter().filter_map(|e| match e {
                LogEntry::Decided(cmd) => Some(cmd),
                _ => unreachable!(),
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

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => match &kv_command {
                    KVCommand::Get(_) => {
                        self.metrics_server.local_read();
                        self.handle_read_request(from, command_id, kv_command);
                    }
                    _ => {
                        self.metrics_server.local_write();
                        self.append_to_log(from, command_id, kv_command);
                    }
                },
                ClientMessage::Done => self.handle_client_done(from).await,
            }
        }
    }

    async fn handle_cluster_messages(&mut self, messages: &mut Vec<(NodeId, ClusterMessage)>) {
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
                ClusterMessage::LeaderStartSignal(start_time) => {
                    self.send_client_start_signals(start_time)
                }
                ClusterMessage::Done => self.handle_peer_done(from).await,
            }
        }
        // TODO: check if its ok to delay the outgoing + decided
        // if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
        //     self.handle_decided_entries();
        // }
        // self.send_outgoing_msgs();
    }

    fn handle_read_request(
        &mut self,
        from: ClientId,
        command_id: CommandId,
        kv_command: KVCommand,
    ) {
        let read_strat = self.strategy.read_strategies[self.id as usize - 1];
        match read_strat {
            ReadStrategy::ReadAsWrite => self.append_to_log(from, command_id, kv_command),
            ReadStrategy::QuorumRead => self.start_quorum_read(from, command_id, kv_command, false),
            ReadStrategy::BallotRead => self.start_quorum_read(from, command_id, kv_command, true),
        }
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
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

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in self.omnipaxos.get_peers() {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    // TODO: its possible for strategy updates to come in out of sync or get dropped
    // Should either self-derive or send through consensus layer
    fn handle_read_strategy_update(&mut self, _from: NodeId, read_strat: Vec<ReadStrategy>) {
        self.strategy.read_strategies = read_strat
    }

    async fn handle_peer_done(&mut self, peer: NodeId) {
        info!("{}: Received Done signal from peer {peer}", self.id);
        self.experiment_state.node_finished(peer);
    }

    async fn handle_client_done(&mut self, client_id: ClientId) {
        info!("{}: Received Done signal from client {client_id}", self.id);
        self.experiment_state.client_finished(client_id);
        if self.experiment_state.my_clients_are_finished() {
            self.experiment_state.node_finished(self.id);
            for peer in self.omnipaxos.get_peers() {
                let done_msg = ClusterMessage::Done;
                self.network.send_to_cluster(*peer, done_msg);
            }
        }
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

// Keeps track of which nodes/clients have finished the experiment.
#[derive(Debug, Clone)]
struct ExperimentState {
    node_states: Vec<State>,
    client_states: Vec<State>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Running,
    Done,
}

impl ExperimentState {
    fn initial_state(config: AutoQuorumServerConfig) -> Self {
        let node_states = vec![State::Running; config.nodes.len()];
        let client_states = vec![State::Running; config.num_clients];
        ExperimentState {
            node_states,
            client_states,
        }
    }

    fn node_finished(&mut self, node: NodeId) {
        self.node_states[node as usize - 1] = State::Done;
    }

    fn client_finished(&mut self, client: ClientId) {
        self.client_states[client as usize - 1] = State::Done;
    }

    fn my_clients_are_finished(&self) -> bool {
        self.client_states.iter().all(|s| *s == State::Done)
    }

    fn is_finished(&self) -> bool {
        self.node_states.iter().all(|s| *s == State::Done)
    }
}
