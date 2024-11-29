use auto_quorum::common::{kv::NodeId, messages::ReadStrategy};
use serde::Serialize;

use crate::{configs::AutoQuorumServerConfig, metrics::Load};

// TODO: make this a config setting
const FAILURE_TOLERANCE: usize = 1;

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
        let init_read_strat = match config.initial_read_strat {
            Some(strats) => strats,
            None => vec![ReadStrategy::default(); num_nodes],
        };
        Self {
            leader: config.initial_leader,
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

pub struct ClusterOptimizer {
    num_nodes: usize,
    cached_latencies: Vec<Vec<f64>>,
    cached_quorum_latencies: Vec<Vec<(usize, f64)>>,
    // For cached_strats: rows = leader, cols = read quorum_size, depth = node
    cached_strats: Vec<Vec<Vec<NodeStrategy>>>,
    quorum_indices: Vec<(usize, usize)>,
    invalidate_cache_threshold: f64,
}

impl ClusterOptimizer {
    pub fn new(nodes: Vec<NodeId>) -> ClusterOptimizer {
        let num_nodes = nodes.len();
        let quorum_indices = Self::create_quorum_indices(num_nodes);
        let num_quorum_options = quorum_indices.len();
        ClusterOptimizer {
            num_nodes,
            cached_latencies: vec![vec![0.; num_nodes]; num_nodes],
            cached_quorum_latencies: vec![vec![(0, 0.); num_nodes]; num_nodes],
            cached_strats: vec![
                vec![vec![NodeStrategy::default(); num_nodes]; num_quorum_options];
                num_nodes
            ],
            quorum_indices,
            invalidate_cache_threshold: 3.,
        }
    }

    pub fn update_latencies(&mut self, new_latencies: &Vec<Vec<f64>>) -> bool {
        if self.validate_cached_latencies(new_latencies) {
            return false;
        } else {
            self.cached_latencies = new_latencies.clone();
            self.cached_quorum_latencies = self.calculate_quorum_latencies();
        }
        for leader_idx in 0..self.cached_strats.len() {
            for quorum_idx in 0..self.quorum_indices.len() {
                let read_quorum_idx = self.quorum_indices[quorum_idx].0;
                let write_quorum_idx = self.quorum_indices[quorum_idx].1;
                for node_idx in 0..self.num_nodes {
                    let read_as_write_latency = self.calculate_read_as_write_latency(
                        leader_idx,
                        write_quorum_idx,
                        node_idx,
                    );
                    let mut best_strategy = ReadStrategy::ReadAsWrite;
                    let mut best_strategy_latency = read_as_write_latency;

                    let read_quorum_latency = self.calculate_read_quorum_latency(
                        leader_idx,
                        read_quorum_idx,
                        write_quorum_idx,
                        node_idx,
                    );
                    if read_quorum_latency <= best_strategy_latency {
                        best_strategy = ReadStrategy::QuorumRead;
                        best_strategy_latency = read_quorum_latency;
                    }

                    let ballot_read_quorum_latency =
                        self.calculate_ballot_read_latency(leader_idx, node_idx, read_quorum_idx);
                    if ballot_read_quorum_latency < best_strategy_latency {
                        best_strategy = ReadStrategy::BallotRead;
                        best_strategy_latency = ballot_read_quorum_latency;
                    }

                    self.cached_strats[leader_idx][quorum_idx][node_idx] = NodeStrategy {
                        write_latency: read_as_write_latency,
                        read_strategy: best_strategy,
                        read_latency: best_strategy_latency,
                    };
                }
            }
        }
        return true;
    }

    fn calculate_read_as_write_latency(
        &self,
        leader_idx: usize,
        write_quorum_idx: usize,
        node_idx: usize,
    ) -> f64 {
        let to_leader_rtt = self.cached_latencies[node_idx][leader_idx];
        let leader_write_latency = self.cached_quorum_latencies[leader_idx][write_quorum_idx].1;
        return to_leader_rtt + leader_write_latency;
    }

    fn calculate_read_quorum_latency(
        &self,
        leader_idx: usize,
        read_quorum_idx: usize,
        write_quorum_idx: usize,
        node_idx: usize,
    ) -> f64 {
        let to_leader_rtt = self.cached_latencies[node_idx][leader_idx];
        let leader_write_latency = self.cached_quorum_latencies[leader_idx][write_quorum_idx].1;
        let read_quorum = &self.cached_quorum_latencies[node_idx][0..=read_quorum_idx];
        let (node_to_most_updated_latency, leader_to_most_updated_latency) = self
            .cached_quorum_latencies[leader_idx]
            .iter()
            .find_map(|(id, leader_to_most_updated_latency)| {
                read_quorum.iter().find(|(r_id, _)| r_id == id).map(
                    |(_, node_to_most_updated_latency)| {
                        (
                            node_to_most_updated_latency / 2.,
                            leader_to_most_updated_latency / 2.,
                        )
                    },
                )
            })
            .unwrap();
        let read_quorum_latency_no_wait = self.cached_quorum_latencies[node_idx][read_quorum_idx].1;
        let read_quorum_latency_with_wait = node_to_most_updated_latency + leader_write_latency
            - leader_to_most_updated_latency
            + (to_leader_rtt / 2.);
        return read_quorum_latency_no_wait.max(read_quorum_latency_with_wait);
    }

    fn calculate_ballot_read_latency(
        &self,
        leader_idx: usize,
        node_idx: usize,
        read_quorum_idx: usize,
    ) -> f64 {
        let to_leader_rtt = self.cached_latencies[node_idx][leader_idx];
        let read_quorum_latency_no_wait = self.cached_quorum_latencies[node_idx][read_quorum_idx].1;
        return to_leader_rtt.max(read_quorum_latency_no_wait);
    }

    pub fn calculate_optimal_strategy(&mut self, workload: &Vec<Load>) -> (ClusterStrategy, f64) {
        let mut best_latency = f64::MAX;
        let mut best_leader_idx = 0;
        let mut best_quorum_idx = 0;
        for leader_idx in 0..self.num_nodes {
            for quorum_idx in 0..self.quorum_indices.len() {
                let latency = self.get_optimal_latency(workload, leader_idx, quorum_idx);
                if latency < best_latency {
                    best_latency = latency;
                    best_leader_idx = leader_idx;
                    best_quorum_idx = quorum_idx;
                }
            }
        }
        let best_read_strats = self.cached_strats[best_leader_idx][best_quorum_idx]
            .iter()
            .map(|node_strat| node_strat.read_strategy)
            .collect();
        let best_strategy = ClusterStrategy {
            leader: best_leader_idx as NodeId + 1,
            read_quorum_size: self.quorum_indices[best_quorum_idx].0 + 1,
            write_quorum_size: self.quorum_indices[best_quorum_idx].1 + 1,
            read_strategies: best_read_strats,
        };
        (best_strategy, best_latency)
    }

    pub fn score_strategy(&self, workload: &Vec<Load>, strategy: &ClusterStrategy) -> f64 {
        let leader_idx = strategy.leader as usize - 1;
        let quorum_idx = self.read_quorum_size_to_idx(strategy.read_quorum_size);
        let read_quorum_idx = self.quorum_indices[quorum_idx].0;
        let write_quorum_idx = self.quorum_indices[quorum_idx].1;
        let mut latency = 0.;
        for node_idx in 0..self.num_nodes {
            latency += workload[node_idx].writes
                * self.cached_strats[leader_idx][quorum_idx][node_idx].write_latency;
            let read_latency = match strategy.read_strategies[node_idx] {
                ReadStrategy::QuorumRead => self.calculate_read_quorum_latency(
                    leader_idx,
                    read_quorum_idx,
                    write_quorum_idx,
                    node_idx,
                ),
                ReadStrategy::ReadAsWrite => {
                    self.calculate_read_as_write_latency(leader_idx, write_quorum_idx, node_idx)
                }
                ReadStrategy::BallotRead => {
                    self.calculate_ballot_read_latency(leader_idx, node_idx, read_quorum_idx)
                }
            };
            latency += workload[node_idx].reads * read_latency;
        }
        latency
    }

    pub fn get_optimal_read_strat(
        &self,
        leader: NodeId,
        read_quorum_size: usize,
        node: NodeId,
    ) -> ReadStrategy {
        let leader_idx = leader as usize - 1;
        let quorum_idx = self.read_quorum_size_to_idx(read_quorum_size);
        let node_idx = node as usize - 1;
        self.cached_strats[leader_idx][quorum_idx][node_idx].read_strategy
    }

    pub fn get_optimal_node_strat(
        &self,
        leader: NodeId,
        read_quorum_size: usize,
        node: NodeId,
    ) -> NodeStrategy {
        let leader_idx = leader as usize - 1;
        let quorum_idx = self.read_quorum_size_to_idx(read_quorum_size);
        let node_idx = node as usize - 1;
        self.cached_strats[leader_idx][quorum_idx][node_idx]
    }

    fn get_optimal_latency(
        &self,
        workload: &Vec<Load>,
        leader_idx: usize,
        quorum_idx: usize,
    ) -> f64 {
        let mut total_latency = 0.;
        for node in 0..self.num_nodes {
            total_latency += workload[node].writes
                * self.cached_strats[leader_idx][quorum_idx][node].write_latency;
            total_latency += workload[node].reads
                * self.cached_strats[leader_idx][quorum_idx][node].read_latency;
        }
        total_latency
    }

    fn validate_cached_latencies(&mut self, new_latencies: &Vec<Vec<f64>>) -> bool {
        for i in 0..self.num_nodes {
            for j in 0..self.num_nodes {
                if (new_latencies[i][j] - self.cached_latencies[i][j]).abs()
                    > self.invalidate_cache_threshold
                {
                    return false;
                }
            }
        }
        true
    }

    fn calculate_quorum_latencies(&self) -> Vec<Vec<(usize, f64)>> {
        let mut quorum_latencies = vec![];
        for node_latency in self.cached_latencies.iter() {
            let mut quorum_latency: Vec<(usize, f64)> =
                node_latency.iter().cloned().enumerate().collect();
            quorum_latency.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            quorum_latencies.push(quorum_latency);
        }
        quorum_latencies
    }

    fn create_quorum_indices(num_nodes: usize) -> Vec<(usize, usize)> {
        let read_quorum_indices = FAILURE_TOLERANCE..num_nodes - FAILURE_TOLERANCE;
        read_quorum_indices
            .map(|read_q_idx| (read_q_idx, num_nodes - read_q_idx - 1))
            .collect()
    }

    fn read_quorum_size_to_idx(&self, read_quorum_size: usize) -> usize {
        read_quorum_size - self.quorum_indices[0].0 - 1
    }
}
