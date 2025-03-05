from __future__ import annotations

from dataclasses import asdict, dataclass, replace

import toml

from clusters.autoquorum_configs import ClientConfig, FlexibleQuorum, ServerConfig
from clusters.base_cluster import ClusterConfigProtocol


@dataclass(frozen=True)
class ClusterConfig(ClusterConfigProtocol):
    multileader_cluster_config: MultiLeaderClusterConfig
    server_configs: dict[int, ServerConfig]
    client_configs: dict[int, ClientConfig]
    client_image: str
    server_image: str

    @dataclass(frozen=True)
    class MultiLeaderClusterConfig:
        nodes: list[int]
        node_addrs: list[str]
        initial_flexible_quorum: FlexibleQuorum | None

    def __post_init__(self):
        self.validate()

    def validate(self):
        aq_config = self.multileader_cluster_config
        if aq_config.initial_flexible_quorum:
            read_quorum = aq_config.initial_flexible_quorum.read_quorum_size
            write_quorum = aq_config.initial_flexible_quorum.write_quorum_size
            if read_quorum < 2:
                raise ValueError("Read quorum must be greater than 2")
            if write_quorum < 2:
                raise ValueError("Write quorum must be greater than 2")
            if read_quorum + write_quorum <= len(aq_config.nodes):
                raise ValueError(
                    f"Flexible quorum {(read_quorum, write_quorum)} must guarantee overlap"
                )

        for client_id in self.client_configs.keys():
            if client_id not in self.server_configs.keys():
                raise ValueError(f"Client {client_id} has no server to connect to")

        for server_id, server_config in self.server_configs.items():
            client_configs = self.client_configs.values()
            server_id_matches = sum(
                1
                for _ in filter(
                    lambda c: c.autoquorum_client_config.server_id == server_id,
                    client_configs,
                )
            )
            total_matches = server_id_matches
            num_clients = server_config.autoquorum_server_config.num_clients
            if num_clients != total_matches:
                raise ValueError(
                    f"Server {server_id} has {num_clients} clients but found {total_matches} references among client configs"
                )

        server_ids = sorted(self.server_configs.keys())
        if aq_config.nodes != server_ids:
            raise ValueError(
                f"Cluster nodes {aq_config.nodes} must match defined server ids {server_ids}"
            )

    def update_config(self, **kwargs) -> ClusterConfig:
        new_op_config = replace(self.multileader_cluster_config, **kwargs)
        new_config = replace(self, multileader_cluster_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_cluster_toml(self) -> str:
        cluster_config_dict = asdict(self.multileader_cluster_config)
        cluster_toml_str = toml.dumps(cluster_config_dict)
        return cluster_toml_str
