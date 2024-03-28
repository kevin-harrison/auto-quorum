use common::messages::ReadStrategy;
use common::kv::NodeId;
use serde::Serialize;

use crate::metrics::ClusterMetrics;

// TODO: make this a config setting
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
    current_strategy: &mut ClusterStrategy,
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
    current_strategy.average_latency_estimate = current_strategy_latency / metrics.get_total_load();
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
            if read_strat.contains(&ReadStrategy::QuorumRead) {
                log::error!("Found best strat with q-read: {} {read_quorum_size} {read_strat:?}", leader+1);
            }
            let absolute_latency_reduction = current_strategy_latency - min_latency;
            if absolute_latency_reduction < 3. {
                return None;
            }
            let relative_latency_reduction = min_latency / current_strategy_latency;
            if relative_latency_reduction > 0.97 {
                return None;
            }
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
    quorum_latencies: &Vec<Vec<(usize, f64)>>,
    leader: usize,
    read_quorum_size: usize,
) -> (f64, Vec<ReadStrategy>) {
    let write_quorum_size = metrics.num_nodes - read_quorum_size + 1;
    // println!("Testing leader={leader_node}, read_quorum={read_quorum_size}");
    let mut total_latency = 0.;
    let mut read_strats = vec![ReadStrategy::default(); metrics.num_nodes];
    for node in 0..metrics.num_nodes {
        if metrics.workload[node].reads == 0. && metrics.workload[node].writes == 0. {
            continue;
        }
        let to_leader_rtt = metrics.latencies[node][leader];
        let leader_write_latency = quorum_latencies[leader][write_quorum_size - 1].1;
        let write_latency = to_leader_rtt + leader_write_latency;
        
        // TODO: leader shouldn't have to wait if reads look at ballots (need to add to add this to reading logic)
        let (bottleneck_node, read_quorum_latency_no_wait) = quorum_latencies[node][read_quorum_size - 1];
        let worst_case_dec_idx_delay = leader_write_latency - (metrics.latencies[leader][bottleneck_node] / 2.) + (to_leader_rtt / 2.);
        let read_quorum_latency_with_wait = (read_quorum_latency_no_wait / 2.) + worst_case_dec_idx_delay;
        let read_quorum_latency = read_quorum_latency_no_wait.max(read_quorum_latency_with_wait);

        if write_latency < read_quorum_latency {
            read_strats[node] = ReadStrategy::ReadAsWrite;
            total_latency +=
                (metrics.workload[node].reads + metrics.workload[node].writes) * write_latency;
            // println!("Node {node}: write_lat={write_latency}");
        } else {
            // println!("Node {node}: write_lat={write_latency} read_lat={quorum_read_latency}");
            read_strats[node] = ReadStrategy::QuorumRead;
            total_latency += (metrics.workload[node].reads * read_quorum_latency)
                + (metrics.workload[node].writes * write_latency);
        }
    }
    (total_latency, read_strats)
}
