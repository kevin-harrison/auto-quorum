use auto_quorum::common::messages::ReadStrategy;
use omnipaxos::{
    util::{FlexibleQuorum, NodeId},
    ClusterConfig, OmniPaxosConfig, ServerConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AutoQuorumServerConfig {
    pub location: String,
    pub server_id: NodeId,
    pub num_clients: usize,
    pub output_filepath: String,
    // Cluster-wide settings
    pub local_deployment: Option<bool>,
    pub cluster_name: String,
    pub nodes: Vec<NodeId>,
    pub initial_leader: NodeId,
    pub initial_flexible_quorum: Option<FlexibleQuorum>,
    pub optimize: Option<bool>,
    pub optimize_threshold: Option<f64>,
    pub initial_read_strat: Option<Vec<ReadStrategy>>,
}

impl Into<OmniPaxosConfig> for AutoQuorumServerConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: self.nodes,
            flexible_quorum: self.initial_flexible_quorum,
            initial_leader: None,
        };
        let server_config = ServerConfig {
            pid: self.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}
