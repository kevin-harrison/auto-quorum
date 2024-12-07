use crate::{
    configs::AutoQuorumServerConfig,
    database::Database,
    metrics::{ClusterMetrics, MetricsHeartbeatServer},
    network::Network,
};
use auto_quorum::common::{kv::*, messages::*, utils::Timestamp};
use chrono::Utc;
use log::*;
use omnipaxos::{
    util::{FlexibleQuorum, LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::Serialize;
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    time::Duration,
};
use tokio::time::interval;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const OPTIMIZE_TIMEOUT: Duration = Duration::from_secs(1);

pub struct MultiLeaderServer {
    id: NodeId,
    peers: Vec<NodeId>,
    database: Database,
    network: Network,
    omnipaxos_instances: Vec<OmniPaxosInstance>,
    applied_indices: Vec<usize>,
    metrics_server: MetricsHeartbeatServer,
    strategy: ClusterStrategy,
    experiment_state: ExperimentState,
    output_file: File,
    config: AutoQuorumServerConfig,
}

impl MultiLeaderServer {
    pub async fn new(config: AutoQuorumServerConfig) -> Self {
        let mut omnipaxos_instances = vec![];
        for _ in config.nodes.iter() {
            let storage: MemoryStorage<Command> = MemoryStorage::default();
            let omnipaxos_config: OmniPaxosConfig = config.clone().into();
            let omnipaxos = omnipaxos_config.build(storage).unwrap();
            omnipaxos_instances.push(omnipaxos);
        }
        let metrics_server = MetricsHeartbeatServer::new(config.server_id, config.nodes.clone());
        let init_strat = ClusterStrategy::initial_strategy(config.clone());
        let network = Network::new(
            config.cluster_name.clone(),
            config.server_id,
            config.nodes.clone(),
            config.num_clients,
            config.local_deployment.unwrap_or(false),
            NETWORK_BATCH_SIZE,
        )
        .await;
        let output_file = File::create(config.output_filepath.clone()).unwrap();
        let mut server = MultiLeaderServer {
            id: config.server_id,
            peers: config
                .nodes
                .iter()
                .copied()
                .filter(|n| *n != config.server_id)
                .collect(),
            database: Database::new(),
            network,
            omnipaxos_instances,
            applied_indices: vec![0; config.nodes.len()],
            metrics_server,
            strategy: init_strat,
            experiment_state: ExperimentState::initial_state(config.clone()),
            output_file,
            config,
        };
        // Clears outgoing_messages of initial BLE messages
        for instance in &mut server.omnipaxos_instances {
            let _ = instance.outgoing_messages();
        }
        server
    }

    pub async fn run(&mut self) {
        self.start_log();
        if self.config.num_clients == 0 {
            self.experiment_state.node_finished(self.id);
            for peer in &self.peers {
                let done_msg = MultiLeaderClusterMessage::Done;
                self.network.send_to_cluster(*peer, done_msg);
            }
        }
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election and instead force an initial leader
        // Once the leader is established it chooses a synchronization point which the
        // followers relay to their clients to begin the experiment.
        // TODO: syncs for different instances could start a different times
        self.become_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
        self.send_client_start_signals(experiment_sync_start);

        let mut optimize_interval = interval(OPTIMIZE_TIMEOUT);
        optimize_interval.tick().await;
        // Main event loop
        loop {
            tokio::select! {
                _ = optimize_interval.tick() => {
                    // Still metric sync with optimize = False to mimic leader election heartbeats
                    let requests = self.metrics_server.tick();
                    for (to, request) in requests {
                        let cluster_msg = MultiLeaderClusterMessage::MetricSync(request);
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
            if self.experiment_state.is_finished() || self.experiment_state.is_killed(self.id) {
                self.network.shutdown().await;
                break;
            }
        }
        self.end_log();
    }

    // Ensures cluster is connected and leader is promoted before returning.
    async fn become_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, MultiLeaderClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_attempt = 0;
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick() => {
                    self.take_leadership(&mut leader_attempt).await;
                    let instance = &mut self.omnipaxos_instances[self.id as usize - 1];
                    if instance.is_accept_phase_leader() {
                        info!("{}: Leader fully initialized", self.id);
                        break;
                    }
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(cluster_msg_buffer).await;
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    async fn take_leadership(&mut self, leader_attempt: &mut u32) {
        let my_instance_id = self.id;
        let my_instance = &mut self.omnipaxos_instances[my_instance_id as usize - 1];
        if my_instance.is_accept_phase_leader() {
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
        my_instance.initialize_prepare_phase(ballot);
        self.send_outgoing_msgs(my_instance_id);
    }

    fn handle_optimize_timeout(&mut self) {
        self.log();
        return;
        // self.update_current_strategy();
        // let leader = self
        //     .omnipaxos_instances
        //     .get_current_leader()
        //     .expect("Leader should be inited");
        // // Only leader needs to optimize but kept for logging purposes
        // let cache_update = self
        //     .optimizer
        //     .update_latencies(&self.metrics_server.metrics.latencies);
        // let curr_workload = &self.metrics_server.metrics.workload;
        // let curr_strat_latency = self.optimizer.score_strategy(curr_workload, &self.strategy);
        // let (optimal_strategy, optimal_strat_latency) =
        //     self.optimizer.calculate_optimal_strategy(curr_workload);
        // let do_reconfigure = leader == self.id
        //     && self.reconfigure_threshold(optimal_strat_latency, curr_strat_latency);
        // self.log(
        //     do_reconfigure,
        //     curr_strat_latency,
        //     &optimal_strategy,
        //     optimal_strat_latency,
        //     cache_update,
        // );
        // if do_reconfigure {
        //     self.reconfigure_strategy(optimal_strategy);
        // }
    }

    fn handle_decided_entries(&mut self, instance_id: InstanceId) {
        let instance_idx = instance_id as usize - 1;
        let instance = &mut self.omnipaxos_instances[instance_idx];
        let applied_idx = self.applied_indices[instance_idx];
        let new_decided_idx = instance.get_decided_idx();
        if applied_idx < new_decided_idx {
            let decided_entries = instance.read_decided_suffix(applied_idx).unwrap();
            self.applied_indices[instance_idx] = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands);
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

    fn send_outgoing_msgs(&mut self, instance_id: InstanceId) {
        let instance = &mut self.omnipaxos_instances[instance_id as usize - 1];
        let messages = instance.outgoing_messages();
        for msg in messages {
            let to = msg.get_receiver();
            let cluster_msg = MultiLeaderClusterMessage::OmniPaxosMessage(instance_id, msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => match &kv_command {
                    KVCommand::Get(_) => {
                        self.metrics_server.local_read();
                        self.append_to_log(from, command_id, kv_command);
                    }
                    _ => {
                        self.metrics_server.local_write();
                        self.append_to_log(from, command_id, kv_command);
                    }
                },
                ClientMessage::Done => self.handle_client_done(from).await,
                ClientMessage::Kill => self.handle_client_kill(from).await,
            }
        }
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, MultiLeaderClusterMessage)>,
    ) {
        for (from, message) in messages.drain(..) {
            match message {
                MultiLeaderClusterMessage::OmniPaxosMessage(instance_id, m) => {
                    let instance = &mut self.omnipaxos_instances[instance_id as usize - 1];
                    instance.handle_incoming(m);
                    self.send_outgoing_msgs(instance_id);
                    self.handle_decided_entries(instance_id);
                }
                MultiLeaderClusterMessage::MetricSync(sync) => {
                    if let Some((to, reply)) = self.metrics_server.handle_metric_sync(from, sync) {
                        let cluster_msg = MultiLeaderClusterMessage::MetricSync(reply);
                        self.network.send_to_cluster(to, cluster_msg);
                    }
                }
                MultiLeaderClusterMessage::Done => self.handle_peer_done(from).await,
            }
        }
        // TODO: check if its ok to delay the outgoing + decided
        // if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
        //     self.handle_decided_entries();
        // }
        // self.send_outgoing_msgs();
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        let my_instance_id = self.id;
        let my_instance = &mut self.omnipaxos_instances[my_instance_id as usize - 1];
        my_instance
            .append(command)
            .expect("Append to Omnipaxos log failed");
        self.send_outgoing_msgs(my_instance_id);
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
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
            for peer in &self.peers {
                let done_msg = MultiLeaderClusterMessage::Done;
                self.network.send_to_cluster(*peer, done_msg);
            }
        }
    }

    async fn handle_client_kill(&mut self, client_id: ClientId) {
        info!("{}: Received kill signal from {client_id}", self.id);
        for peer in &self.peers {
            let done_msg = MultiLeaderClusterMessage::Done;
            self.network.send_to_cluster(*peer, done_msg);
        }
        self.experiment_state.node_killed(self.id);
    }

    // Fake latency and optimal strategy data to make pandas columns consistent
    fn log(&mut self) {
        let leader = self.omnipaxos_instances[self.id as usize - 1].get_current_leader();
        let node_strat = NodeStrategy {
            read_latency: 0.,
            write_latency: 0.,
            read_strategy: self.strategy.read_strategies[self.id as usize - 1],
        };
        // let current_load = self.metrics_server.metrics.get_total_load();
        let instrumentation = StrategyInstrumentation {
            timestamp: Utc::now().timestamp_millis(),
            reconfigure: false,
            curr_strat_latency: 0.0,
            opt_strat_latency: 0.0,
            curr_strat: &self.strategy,
            opt_strat: &self.strategy,
            operation_latency: node_strat,
            leader,
            metrics_update: false,
            cluster_metrics: &self.metrics_server.metrics,
        };
        serde_json::to_writer(&mut self.output_file, &instrumentation).unwrap();
        self.output_file.write_all(b",\n").unwrap();
    }

    fn start_log(&mut self) {
        let config_json = serde_json::to_string_pretty(&self.config).unwrap();
        let log_start = format!("{{\n \"server_config\": {config_json},\n \"log\": [\n");
        self.output_file.write_all(log_start.as_bytes()).unwrap();
        // Dummy log entry to ensure column names in pandas
        self.log()
    }

    fn end_log(&mut self) {
        // Move back 2 bytes to overwrite the trailing ",\n"
        self.output_file.seek(SeekFrom::End(-2)).unwrap();
        let end_array_json = b"\n]\n}";
        self.output_file.write_all(end_array_json).unwrap();
        self.output_file.flush().unwrap();
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
    Killed,
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

    fn node_killed(&mut self, node: NodeId) {
        self.node_states[node as usize - 1] = State::Killed;
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

    fn is_killed(&self, node: NodeId) -> bool {
        self.node_states[node as usize - 1] == State::Killed
    }
}

#[derive(Serialize)]
struct StrategyInstrumentation<'a> {
    timestamp: Timestamp,
    reconfigure: bool,
    curr_strat_latency: f64,
    opt_strat_latency: f64,
    curr_strat: &'a ClusterStrategy,
    opt_strat: &'a ClusterStrategy,
    operation_latency: NodeStrategy,
    leader: Option<NodeId>,
    metrics_update: bool,
    cluster_metrics: &'a ClusterMetrics,
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterStrategy {
    pub leader: NodeId,
    pub read_quorum_size: usize,
    pub write_quorum_size: usize,
    pub read_strategies: Vec<ReadStrategy>,
}

impl ClusterStrategy {
    pub fn initial_strategy(config: AutoQuorumServerConfig) -> Self {
        let num_nodes = config.nodes.len();
        let init_read_quorum = match config.initial_flexible_quorum {
            Some(flex_quroum) => flex_quroum.read_quorum_size,
            None => num_nodes / 2 + 1,
        };
        let init_read_strat = vec![ReadStrategy::ReadAsWrite; num_nodes];
        Self {
            leader: config.server_id,
            read_quorum_size: init_read_quorum,
            write_quorum_size: num_nodes - init_read_quorum + 1,
            read_strategies: init_read_strat,
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy, Default)]
pub struct NodeStrategy {
    pub write_latency: f64,
    pub read_latency: f64,
    pub read_strategy: ReadStrategy,
}
