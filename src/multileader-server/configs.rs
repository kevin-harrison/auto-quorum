use std::env;

use auto_quorum::common::messages::ReadStrategy;
use config::{Config, ConfigError, Environment, File};
use omnipaxos::{
    util::{FlexibleQuorum, NodeId},
    ClusterConfig as OmnipaxosClusterConfig, OmniPaxosConfig,
    ServerConfig as OmnipaxosServerConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AutoQuorumConfig {
    #[serde(flatten)]
    pub server: ServerConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    pub initial_leader: NodeId,
    pub initial_flexible_quorum: Option<FlexibleQuorum>,
    pub optimize: Option<bool>,
    pub optimize_threshold: Option<f64>,
    pub initial_read_strat: Option<Vec<ReadStrategy>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub location: Option<String>,
    pub server_id: NodeId,
    pub listen_address: String,
    pub listen_port: u16,
    pub num_clients: usize,
    pub output_filepath: String,
}

impl Into<OmniPaxosConfig> for AutoQuorumConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = OmnipaxosClusterConfig {
            configuration_id: 1,
            nodes: self.cluster.nodes,
            flexible_quorum: self.cluster.initial_flexible_quorum,
            initial_leader: None,
        };
        let server_config = OmnipaxosServerConfig {
            pid: self.server.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl AutoQuorumConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("AUTOQUORUM")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        config.try_deserialize()
    }
}
