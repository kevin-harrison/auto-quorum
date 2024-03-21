use common::messages::ReadStrategy;
use common::kv::NodeId;
use serde::Serialize;

use crate::metrics::ClusterMetrics;

const FAILURE_TOLERANCE: usize = 1;

#[derive(Debug, Serialize)]
pub struct ClusterStrategy {
    pub average_latency_estimate: f64,
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
            FAILURE_TOLERANCE + 1..=metrics.num_nodes - FAILURE_TOLERANCE
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
            let absolute_latency_reduction = current_strategy_latency - min_latency;
            if absolute_latency_reduction < 3. {
                return None;
            }
            let relative_latency_reduction = min_latency / current_strategy_latency;
            if relative_latency_reduction > 0.96 {
                return None;
            }
            // error!("Better strategy {current_strategy_latency} -> {min_latency}");
            // error!("with metrics {metrics:?}");
            Some(ClusterStrategy {
                average_latency_estimate: min_latency
                    / metrics.get_total_load(),
                leader: leader as NodeId + 1,
                read_quorum_size,
                read_strat,
            })
        }
        None => None,
    }
}

fn get_latency(
    metrics: &ClusterMetrics,
    quorum_latencies: &Vec<Vec<f64>>,
    leader: usize,
    read_quorum_size: usize,
) -> (f64, Vec<ReadStrategy>) {
    let write_quorum_size = metrics.num_nodes - read_quorum_size + 1;
    // println!("Testing leader={leader_node}, read_quorum={read_quorum_size}");
    let mut total_latency = 0.;
    let mut read_strats = vec![ReadStrategy::QuorumRead; metrics.num_nodes];
    for node in 0..metrics.num_nodes {
        if metrics.workload[node].reads == 0. && metrics.workload[node].writes == 0. {
            continue;
        }
        let travel_to_leader_latency = metrics.latencies[node][leader];
        let leader_write_latency = quorum_latencies[leader][write_quorum_size - 1];
        let write_latency = travel_to_leader_latency + leader_write_latency;
        let quorum_read_latency = 2. * quorum_latencies[node][read_quorum_size - 1];
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
