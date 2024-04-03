use common::{kv::NodeId, messages::ReadStrategy};
use serde::Serialize;

use crate::metrics::{ClusterMetrics, Load};

// TODO: make this a config setting
const FAILURE_TOLERANCE: usize = 1;

#[derive(Debug, Serialize, Clone)]
pub struct ClusterStrategy {
    pub leader: NodeId,
    pub read_quorum_size: usize,
    pub write_quorum_size: usize,
    pub read_strategies: Vec<ReadStrategy>,
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
    // rows = leader, cols = read quorum_size, depth = node
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
            cached_strats: vec![
                vec![vec![NodeStrategy::default(); num_nodes]; num_quorum_options];
                num_nodes
            ],
            quorum_indices, 
            invalidate_cache_threshold: 5.,
        }
    }

    pub fn calculate_optimal_strategy(&mut self, metrics: &ClusterMetrics) -> (ClusterStrategy, f64, bool) {
        let cache_update = self.update_cache(&metrics.latencies);
        let mut best_latency = f64::MAX;
        let mut best_leader_idx = 0;
        let mut best_quorum_idx = 0;
        for leader_idx in 0..self.num_nodes {
            for quorum_idx in 0..self.quorum_indices.len() {
                let latency = self.get_latency(&metrics.workload, leader_idx, quorum_idx);
                if latency < best_latency {
                    best_latency = latency;
                    best_leader_idx = leader_idx;
                    best_quorum_idx = quorum_idx;
                }
            }
        }
        let best_read_strats = self.cached_strats[best_leader_idx][best_quorum_idx].iter().map(|node_strat| node_strat.read_strategy).collect();
        let best_strategy = ClusterStrategy {
            leader: best_leader_idx as NodeId + 1,
            read_quorum_size: self.quorum_indices[best_quorum_idx].0 + 1,
            write_quorum_size: self.quorum_indices[best_quorum_idx].1 + 1,
            read_strategies: best_read_strats,
        };
        (best_strategy, best_latency, cache_update)
    }

    pub fn score_strategy(&self, metrics: &ClusterMetrics, strategy: &ClusterStrategy) -> f64 {
        let leader_idx = strategy.leader as usize - 1;
        let quorum_idx = self.read_quorum_size_to_idx(strategy.read_quorum_size);
        let latency = self.get_latency(&metrics.workload, leader_idx, quorum_idx);
        latency
    }

    pub fn get_optimal_read_strat(&self, leader: NodeId, read_quorum_size: usize, node: NodeId) -> ReadStrategy {
        let leader_idx = leader as usize - 1;
        let quorum_idx = self.read_quorum_size_to_idx(read_quorum_size);
        let node_idx = node as usize - 1;
        self.cached_strats[leader_idx][quorum_idx][node_idx].read_strategy
    }

    pub fn get_optimal_node_strat(&self, leader: NodeId, read_quorum_size: usize, node: NodeId) -> NodeStrategy {
        let leader_idx = leader as usize - 1;
        let quorum_idx = self.read_quorum_size_to_idx(read_quorum_size);
        let node_idx = node as usize - 1;
        self.cached_strats[leader_idx][quorum_idx][node_idx]
    }

    fn get_latency(&self, workload: &Vec<Load>, leader_idx: usize, quorum_idx: usize) -> f64 {
        let mut total_latency = 0.;
        for node in 0..self.num_nodes {
            total_latency += workload[node].writes
                * self.cached_strats[leader_idx][quorum_idx][node].write_latency;
            total_latency += workload[node].reads
                * self.cached_strats[leader_idx][quorum_idx][node].read_latency;
        }
        total_latency
    }

    fn update_cache(&mut self, new_latencies: &Vec<Vec<f64>>) -> bool {
        if self.validate_cached_latencies(new_latencies) {
            return false;
        }
        log::debug!("UPDATING STRATS:");
        let quorum_latencies = self.calculate_quorum_latencies();
        for leader in 0..self.cached_strats.len() {
            for quorum_idx in 0..self.quorum_indices.len() {
                let read_quorum_idx = self.quorum_indices[quorum_idx].0;
                let write_quorum_idx = self.quorum_indices[quorum_idx].1;
                for node in 0..self.num_nodes {
                    log::debug!("======= LEADER {leader}, QUORUM {read_quorum_idx}, NODE {node} =======");
                    // Read as write
                    let to_leader_rtt = self.cached_latencies[node][leader];
                    let leader_write_latency = quorum_latencies[leader][write_quorum_idx].1;
                    let write_latency = to_leader_rtt + leader_write_latency;
                    let mut best_strategy = ReadStrategy::ReadAsWrite;
                    let mut best_strategy_latency = write_latency;

                    // Quorum read
                    let read_quorum = &quorum_latencies[node][0..=read_quorum_idx];
                    let (node_to_most_updated_latency, leader_to_most_updated_latency) = quorum_latencies[leader]
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
                    let read_quorum_latency_no_wait = quorum_latencies[node][read_quorum_idx].1;
                    let read_quorum_latency_with_wait = node_to_most_updated_latency
                        + leader_write_latency
                        - leader_to_most_updated_latency
                        + (to_leader_rtt / 2.);
                    let read_quorum_latency =
                        read_quorum_latency_no_wait.max(read_quorum_latency_with_wait);
                    if read_quorum_latency <= best_strategy_latency {
                        best_strategy = ReadStrategy::QuorumRead;
                        best_strategy_latency = read_quorum_latency;
                    }

                    log::debug!("to_leader_rtt={to_leader_rtt}");
                    log::debug!("leader_write_latency={leader_write_latency}");
                    log::debug!("read_quorum={read_quorum:?}");
                    log::debug!("bottleneck={read_quorum_latency_no_wait}");
                    log::debug!("node=>most_updated={node_to_most_updated_latency}");
                    log::debug!("leader=>most_updated={leader_to_most_updated_latency}");

                    // TODO: Leader reads
                    // // Leader read
                    // let leader_read_latency = quorum_latencies[leader][read_quorum_idx].1;
                    // let proxy_leader_read_latency = to_leader_rtt + leader_read_latency;
                    // if node == leader {
                    //     if leader_read_latency < best_strategy_latency {
                    //         best_strategy = ReadStrategy::LeaderRead;
                    //         best_strategy_latency = leader_read_latency;
                    //     }
                    // } else {
                    //     if proxy_leader_read_latency < best_strategy_latency {
                    //         best_strategy = ReadStrategy::ProxiedLeaderRead;
                    //         best_strategy_latency = proxy_leader_read_latency;
                    //     }
                    // }

                    self.cached_strats[leader][quorum_idx][node] = NodeStrategy {
                        write_latency,
                        read_strategy: best_strategy,
                        read_latency: best_strategy_latency,
                    };
                }
            }
        }
        return true;
    }

    fn validate_cached_latencies(&mut self, new_latencies: &Vec<Vec<f64>>) -> bool {
        let mut change_in_latency = 0.;
        for i in 0..self.num_nodes {
            for j in 0..self.num_nodes {
                change_in_latency += (new_latencies[i][j] - self.cached_latencies[i][j]).abs();
            }
        }
        if change_in_latency < self.invalidate_cache_threshold {
            return true;
        } else {
            self.cached_latencies = new_latencies.clone();
            return false;
        }
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
        read_quorum_indices.map(|read_q_idx| (read_q_idx, num_nodes - read_q_idx - 1)).collect()
    }

    fn read_quorum_size_to_idx(&self, read_quorum_size: usize) -> usize {
        read_quorum_size - self.quorum_indices[0].0 - 1
    }
}

// pub fn find_better_strategy(
//     metrics: &ClusterMetrics,
//     current_strategy: &mut ClusterStrategy,
// ) -> Option<ClusterStrategy> {
//     let mut quorum_latencies = vec![];
//     for node in 0..metrics.num_nodes {
//         quorum_latencies.push(metrics.quorum_latencies(node));
//     }
//
//     let current_leader_idx = current_strategy.leader as usize - 1;
//     let (current_strategy_latency, _) = get_latency(
//         metrics,
//         &quorum_latencies,
//         current_leader_idx,
//         current_strategy.read_quorum_size,
//     );
//     current_strategy.average_latency_estimate = current_strategy_latency / metrics.get_total_load();
//     let mut min_latency = current_strategy_latency;
//     let mut best_parameters = None;
//     for leader_node in 0..metrics.num_nodes {
//         for read_quorum_size in
//             FAILURE_TOLERANCE + 1..=metrics.num_nodes - FAILURE_TOLERANCE
//         {
//             let (latency, read_strats) =
//                 get_latency(metrics, &quorum_latencies, leader_node, read_quorum_size);
//             if latency < min_latency {
//                 min_latency = latency;
//                 best_parameters = Some((leader_node, read_quorum_size, read_strats));
//             }
//             // println!("Total latency: {total_latency}\n");
//         }
//     }
//     match best_parameters {
//         Some((leader, read_quorum_size, read_strat)) => {
//             if read_strat.contains(&ReadStrategy::QuorumRead) {
//                 log::error!("Found best strat with q-read: {} {read_quorum_size} {read_strat:?}", leader+1);
//             }
//             let absolute_latency_reduction = current_strategy_latency - min_latency;
//             if absolute_latency_reduction < 3. {
//                 return None;
//             }
//             let relative_latency_reduction = min_latency / current_strategy_latency;
//             if relative_latency_reduction > 0.97 {
//                 return None;
//             }
//             Some(ClusterStrategy {
//                 average_latency_estimate: min_latency
//                     / metrics.get_total_load(),
//                 leader: leader as NodeId + 1,
//                 read_quorum_size,
//                 read_strat,
//             })
//         }
//         None => None,
//     }
// }
//
// fn get_latency(
//     metrics: &ClusterMetrics,
//     quorum_latencies: &Vec<Vec<(usize, f64)>>,
//     leader: usize,
//     read_quorum_size: usize,
// ) -> (f64, Vec<ReadStrategy>) {
//     let write_quorum_size = metrics.num_nodes - read_quorum_size + 1;
//     // println!("Testing leader={leader_node}, read_quorum={read_quorum_size}");
//     let mut total_latency = 0.;
//     let mut read_strats = vec![ReadStrategy::default(); metrics.num_nodes];
//     for node in 0..metrics.num_nodes {
//         if metrics.workload[node].reads == 0. && metrics.workload[node].writes == 0. {
//             continue;
//         }
//         let to_leader_rtt = metrics.latencies[node][leader];
//         let leader_write_latency = quorum_latencies[leader][write_quorum_size - 1].1;
//         let write_latency = to_leader_rtt + leader_write_latency;
//
//         // TODO: leader shouldn't have to wait if reads look at ballots (need to add to add this to reading logic)
//         let (bottleneck_node, read_quorum_latency_no_wait) = quorum_latencies[node][read_quorum_size - 1];
//         let worst_case_dec_idx_delay = leader_write_latency - (metrics.latencies[leader][bottleneck_node] / 2.) + (to_leader_rtt / 2.);
//         let read_quorum_latency_with_wait = (read_quorum_latency_no_wait / 2.) + worst_case_dec_idx_delay;
//         let read_quorum_latency = read_quorum_latency_no_wait.max(read_quorum_latency_with_wait);
//
//         if write_latency < read_quorum_latency {
//             read_strats[node] = ReadStrategy::ReadAsWrite;
//             total_latency +=
//                 (metrics.workload[node].reads + metrics.workload[node].writes) * write_latency;
//             // println!("Node {node}: write_lat={write_latency}");
//         } else {
//             // println!("Node {node}: write_lat={write_latency} read_lat={quorum_read_latency}");
//             read_strats[node] = ReadStrategy::QuorumRead;
//             total_latency += (metrics.workload[node].reads * read_quorum_latency)
//                 + (metrics.workload[node].writes * write_latency);
//         }
//     }
//     (total_latency, read_strats)
// }
