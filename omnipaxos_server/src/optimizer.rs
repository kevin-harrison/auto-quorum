use common::messages::ReadStrategy;
use omnipaxos::util::NodeId;

#[derive(Debug, Clone, Copy, Default)]
pub struct Load {
    pub reads: u64,
    pub writes: u64,
}

pub struct ClusterMetrics {
    pub num_nodes: usize,
    pub failure_tolerance: usize,
    pub latencies: Vec<Vec<u64>>,
    pub workload: Vec<Load>,
    // pub capacities: Vec<u64>,
}

impl std::fmt::Debug for ClusterMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node_metrics: Vec<String> = (0..self.num_nodes)
            .map(|node| format!("node: {node}, workload: {:?}", self.workload[node]))
            .collect();
        let latency_string = self
            .latencies
            .iter()
            .map(|nodes_latencies| format!("{nodes_latencies:>w$?}", w = 2))
            .collect::<Vec<String>>()
            .join("\n");
        write!(
            f,
            "ClusterMetrics {{\nnumber of nodes: {}\n{node_metrics:#?}\n{latency_string}}}",
            self.num_nodes,
        )
    }
}

type NodeLatencies = Vec<Option<u64>>;
const LATENCY_CAP: u64 = 9999;

impl ClusterMetrics {
    pub fn new(num_nodes: usize, failure_tolerance: usize) -> Self {
        let starting_load = Load {
            reads: 0,
            writes: 0,
        };
        Self {
            num_nodes,
            failure_tolerance,
            latencies: vec![vec![50; num_nodes]; num_nodes],
            workload: vec![starting_load; num_nodes],
            // TODO: capacities
            // capacities: vec![]
        }
    }

    pub fn update_latencies(&mut self, new_latencies: Vec<Option<NodeLatencies>>) {
        // TODO: handle missing data points
        for from in 0..self.latencies.len() {
            match &new_latencies[from] {
                Some(latencies) => {
                    for to in 0..self.latencies.len() {
                        match latencies[to] {
                            Some(measurement) => self.latencies[from][to] = measurement,
                            None => {
                                let doubled_latency = self.latencies[from][to] * 2;
                                if doubled_latency > LATENCY_CAP {
                                    self.latencies[from][to] = LATENCY_CAP;
                                } else {
                                    self.latencies[from][to] = doubled_latency;
                                }
                            }
                        }
                    }
                }
                None => continue,
            }
        }
    }

    pub fn update_workload(&mut self, from: NodeId, reads: u64, writes: u64) {
        self.workload[from as usize - 1] = Load { reads, writes };
    }

    pub fn inc_workload_reads(&mut self, node: NodeId) {
        self.workload[node as usize - 1].reads += 1;
    }

    pub fn inc_workload_writes(&mut self, node: NodeId) {
        self.workload[node as usize - 1].writes += 1;
    }

    pub fn take_workload(&mut self, node: NodeId) -> Load {
        std::mem::take(&mut self.workload[node as usize - 1])
    }

    fn quorum_latencies(&self, node: usize) -> Vec<u64> {
        let mut latencies = self.latencies[node].clone();
        latencies.sort();
        latencies
    }

    fn get_total_opertations(&self) -> u64 {
        self.workload
            .iter()
            .fold(0, |acc, x| acc + x.writes + x.reads)
    }
}

#[derive(Debug)]
pub struct ClusterStrategy {
    // pub estimated_latency: u64,
    pub leader: NodeId,
    pub read_quorum_size: usize,
    pub read_strat: Vec<ReadStrategy>,
}

pub fn find_better_strategy(
    metrics: &ClusterMetrics,
    current_strategy: &ClusterStrategy,
) -> Option<ClusterStrategy> {
    let mut quorum_latencies = vec![];
    for node in 0..metrics.num_nodes {
        quorum_latencies.push(metrics.quorum_latencies(node));
    }

    let current_leader_idx = current_strategy.leader as usize - 1;
    let (current_strategy_latency, _) = get_latency(
        metrics,
        &quorum_latencies,
        current_leader_idx,
        current_strategy.read_quorum_size,
    );
    let mut min_latency = current_strategy_latency;
    let mut best_parameters = None;
    for leader_node in 0..metrics.num_nodes {
        for read_quorum_size in
            metrics.failure_tolerance + 1..=metrics.num_nodes - metrics.failure_tolerance
        {
            let (latency, read_strats) =
                get_latency(metrics, &quorum_latencies, leader_node, read_quorum_size);
            if latency < min_latency {
                min_latency = latency;
                best_parameters = Some((leader_node, read_quorum_size, read_strats));
            }
            // println!("Total latency: {total_latency}\n");
        }
    }
    match best_parameters {
        Some((leader, read_quorum_size, read_strat)) => {
            let relative_latency_reduction = min_latency as f64 / current_strategy_latency as f64;
            if relative_latency_reduction < 0.8 {
                Some(ClusterStrategy {
                    // estimated_latency: min_latency,
                    leader: leader as NodeId + 1,
                    read_quorum_size,
                    read_strat,
                })
            } else {
                None
            }
        },
        None => None,
    }
}

fn get_latency(
    metrics: &ClusterMetrics,
    quorum_latencies: &Vec<Vec<u64>>,
    leader: usize,
    read_quorum_size: usize,
) -> (u64, Vec<ReadStrategy>) {
    let write_quorum_size = metrics.num_nodes - read_quorum_size + 1;
    // println!("Testing leader={leader_node}, read_quorum={read_quorum_size}");
    let mut total_latency = 0;
    let mut read_strats = vec![ReadStrategy::QuorumRead; metrics.num_nodes];
    for node in 0..metrics.num_nodes {
        if metrics.workload[node].reads == 0 && metrics.workload[node].writes == 0 {
            continue;
        }
        let travel_to_leader_latency = metrics.latencies[node][leader];
        let leader_write_latency = quorum_latencies[leader][write_quorum_size - 1];
        let write_latency = travel_to_leader_latency + leader_write_latency;
        let quorum_read_latency = 2 * quorum_latencies[node][read_quorum_size - 1];
        if write_latency < quorum_read_latency {
            read_strats[node] = ReadStrategy::ReadAsWrite;
            total_latency +=
                (metrics.workload[node].reads + metrics.workload[node].writes) * write_latency;
            // println!("Node {node}: write_lat={write_latency}");
        } else {
            // println!("Node {node}: write_lat={write_latency} read_lat={quorum_read_latency}");
            total_latency += (metrics.workload[node].reads * quorum_read_latency)
                + (metrics.workload[node].writes * write_latency);
        }
    }
    (total_latency, read_strats)
}
