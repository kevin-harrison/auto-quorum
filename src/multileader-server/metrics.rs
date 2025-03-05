use auto_quorum::common::{
    kv::NodeId,
    messages::{MetricSync, MetricUpdate},
};
use serde::Serialize;
use tokio::time::Instant;

use crate::configs::MultiLeaderConfig;

const LATENCY_CAP: f64 = 9999.;

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Load {
    pub reads: f64,
    pub writes: f64,
}

#[derive(Debug, Clone, Copy, Default)]
enum ReplyStatus {
    #[default]
    Pending,
    Replied,
}

pub struct MetricsHeartbeatServer {
    pub metrics: ClusterMetrics,
    latency_smoothing_factor: f64,
    workload_smoothing_factor: f64,
    pid: NodeId,
    peers: Vec<NodeId>,
    local_reads: f64,
    local_writes: f64,
    round: u64,
    round_start: Instant,
    replies_status: Vec<ReplyStatus>,
}

#[derive(Serialize)]
pub struct ClusterMetrics {
    // TODO: assumes continuous server ids
    pub num_nodes: usize,
    pub latencies: Vec<Vec<f64>>,
    pub workload: Vec<Load>,
}

impl std::fmt::Debug for ClusterMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node_metrics: Vec<String> = self
            .workload
            .iter()
            .enumerate()
            .map(|(i, node)| format!("node: {}, workload: {node:?}", i + 1))
            .collect();
        let latency_string = self
            .latencies
            .iter()
            .map(|nodes_latencies| format!("{nodes_latencies:>w$?}", w = 2))
            .collect::<Vec<String>>()
            .join("\n");
        write!(
            f,
            "ClusterMetrics {{\n{node_metrics:#?}\n{latency_string}}}",
        )
    }
}

impl ClusterMetrics {
    pub fn get_total_load(&self) -> f64 {
        self.workload
            .iter()
            .fold(0., |acc, x| acc + x.writes + x.reads)
    }
}

impl MetricsHeartbeatServer {
    pub fn new(config: MultiLeaderConfig) -> Self {
        let num_nodes = config.cluster.nodes.len();
        let peers = config
            .cluster
            .nodes
            .into_iter()
            .filter(|id| *id != config.server.server_id)
            .collect();
        let mut initial_latencies = vec![vec![50.; num_nodes]; num_nodes];
        for i in 0..num_nodes {
            initial_latencies[i][i] = 0.;
        }
        let initial_metrics = ClusterMetrics {
            num_nodes,
            latencies: initial_latencies,
            workload: vec![Load::default(); num_nodes],
        };
        Self {
            metrics: initial_metrics,
            pid: config.server.server_id,
            peers,
            latency_smoothing_factor: 0.9,
            workload_smoothing_factor: 0.9,
            local_reads: 0.,
            local_writes: 0.,
            round: 0,
            round_start: Instant::now(),
            replies_status: vec![ReplyStatus::Pending; num_nodes],
        }
    }

    pub fn local_read(&mut self) {
        self.local_reads += 1.;
    }

    pub fn local_write(&mut self) {
        self.local_writes += 1.;
    }

    pub fn handle_metric_sync(
        &mut self,
        from: NodeId,
        metric_sync: MetricSync,
    ) -> Option<(NodeId, MetricSync)> {
        let from_idx = from as usize - 1;
        match metric_sync {
            MetricSync::MetricRequest(round, metric_update) => {
                // Also collect workload metric from request to reduce metric gathering latency
                // Don't have to worry about stale metric since TCP gives ordering
                self.metrics.workload[from_idx].reads = metric_update.load.0;
                self.metrics.workload[from_idx].writes = metric_update.load.1;
                let current_metrics = MetricUpdate {
                    latency: self.my_latency().clone(),
                    load: self.my_load_tuple(),
                };
                let reply = MetricSync::MetricReply(round, current_metrics);
                Some((from, reply))
            }
            MetricSync::MetricReply(round, metric_update) => {
                if round == self.round {
                    let reply_latency = self.get_round_delay();
                    let my_latency = &mut self.metrics.latencies[self.pid as usize - 1][from_idx];
                    *my_latency += self.latency_smoothing_factor * (reply_latency - *my_latency);
                    self.metrics.latencies[from_idx] = metric_update.latency;
                    self.metrics.workload[from_idx].reads = metric_update.load.0;
                    self.metrics.workload[from_idx].writes = metric_update.load.1;
                    self.replies_status[from_idx] = ReplyStatus::Replied;
                }
                None
            }
        }
    }

    pub fn tick(&mut self) -> Vec<(NodeId, MetricSync)> {
        if self.round == 0 {
            return self.next_round();
        }

        // Update local workload measurement
        let load = &mut self.metrics.workload[self.pid as usize - 1];
        let local_reads = std::mem::take(&mut self.local_reads);
        let local_writes = std::mem::take(&mut self.local_writes);
        load.reads += self.workload_smoothing_factor * (local_reads - load.reads);
        load.writes += self.workload_smoothing_factor * (local_writes - load.writes);

        // Upadate missing measurements
        let round_delay = self.get_round_delay();
        for (from, reply_status) in self.replies_status.iter_mut().enumerate() {
            if from == self.pid as usize - 1 {
                continue;
            }
            match reply_status {
                ReplyStatus::Pending => {
                    let outgoing_latency = &mut self.metrics.latencies[self.pid as usize - 1][from];
                    if *outgoing_latency < LATENCY_CAP {
                        *outgoing_latency += round_delay;
                    }
                    for (to, latency) in self.metrics.latencies[from].iter_mut().enumerate() {
                        if to != from && *latency < LATENCY_CAP {
                            *latency += round_delay;
                        }
                    }
                    self.metrics.workload[from].reads +=
                        self.workload_smoothing_factor * (0. - self.metrics.workload[from].reads);
                    self.metrics.workload[from].writes +=
                        self.workload_smoothing_factor * (0. - self.metrics.workload[from].writes);
                }
                ReplyStatus::Replied => *reply_status = ReplyStatus::Pending,
            }
        }

        self.next_round()
    }

    fn next_round(&mut self) -> Vec<(NodeId, MetricSync)> {
        self.round += 1;
        self.round_start = Instant::now();
        let request_metric = MetricUpdate {
            latency: vec![],
            load: self.my_load_tuple(),
        };
        let round_requests = self
            .peers
            .iter()
            .map(|peer| {
                (
                    *peer,
                    MetricSync::MetricRequest(self.round, request_metric.clone()),
                )
            })
            .collect();
        round_requests
    }

    fn my_latency(&self) -> &Vec<f64> {
        &self.metrics.latencies[self.pid as usize - 1]
    }

    fn my_load_tuple(&self) -> (f64, f64) {
        let my_idx = self.pid as usize - 1;
        let my_reads = self.metrics.workload[my_idx].reads;
        let my_writes = self.metrics.workload[my_idx].writes;
        (my_reads, my_writes)
    }

    fn get_round_delay(&self) -> f64 {
        self.round_start.elapsed().as_millis() as f64
    }
}
