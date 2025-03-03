from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from enum import Enum

import toml

from gcp_cluster import InstanceConfig


@dataclass(frozen=True)
class ClusterConfig:
    autoquorum_cluster_config: AutoQuorumClusterConfig
    server_configs: dict[int, ServerConfig]
    client_configs: dict[int, ClientConfig]
    client_image: str
    server_image: str
    multileader_server_image: str
    multileader: bool

    @dataclass(frozen=True)
    class AutoQuorumClusterConfig:
        nodes: list[int]
        node_addrs: list[str]
        initial_leader: int
        initial_flexible_quorum: FlexibleQuorum | None
        optimize: bool | None
        optimize_threshold: float | None
        initial_read_strat: list[ReadStrategy] | None

    def __post_init__(self):
        self.validate()

    def validate(self):
        aq_config = self.autoquorum_cluster_config
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

        if aq_config.optimize_threshold:
            if not 0 <= aq_config.optimize_threshold <= 1:
                raise ValueError(
                    f"Optimize threshold {aq_config.optimize_threshold} must be in range [0,1]"
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

        if aq_config.initial_leader not in self.server_configs.keys():
            raise ValueError(
                f"Initial leader {aq_config.initial_leader} must be one of the server nodes"
            )

        server_ids = sorted(self.server_configs.keys())
        if aq_config.nodes != server_ids:
            raise ValueError(
                f"Cluster nodes {aq_config.nodes} must match defined server ids {server_ids}"
            )

    def update_autoquorum_config(self, **kwargs) -> ClusterConfig:
        new_op_config = replace(self.autoquorum_cluster_config, **kwargs)
        new_config = replace(self, autoquorum_cluster_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_cluster_toml(self) -> str:
        cluster_config_dict = asdict(self.autoquorum_cluster_config)
        read_strat_enums = cluster_config_dict["initial_read_strat"]
        if read_strat_enums is not None:
            read_strat_strs = [enum.value for enum in read_strat_enums]
            cluster_config_dict["initial_read_strat"] = read_strat_strs
        cluster_toml_str = toml.dumps(cluster_config_dict)
        return cluster_toml_str


@dataclass(frozen=True)
class ServerConfig:
    instance_config: InstanceConfig
    autoquorum_server_config: AutoQuorumServerConfig
    run_script: str
    rust_log: str
    server_address: str

    @dataclass(frozen=True)
    class AutoQuorumServerConfig:
        location: str
        server_id: int
        listen_address: str
        listen_port: int
        num_clients: int
        output_filepath: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        aq_config = self.autoquorum_server_config
        if aq_config.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {aq_config.server_id}. It must be greater than 0."
            )

        if aq_config.num_clients < 0:
            raise ValueError(
                f"Invalid num_clients: {aq_config.num_clients}. It must be a positive number."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def update_autoquorum_config(self, **kwargs) -> ServerConfig:
        new_op_config = replace(self.autoquorum_server_config, **kwargs)
        new_config = replace(self, autoquorum_server_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_server_toml(self) -> str:
        server_toml_str = toml.dumps(asdict(self.autoquorum_server_config))
        return server_toml_str


@dataclass(frozen=True)
class ClientConfig:
    instance_config: InstanceConfig
    autoquorum_client_config: AutoQuorumClientConfig
    run_script: str
    rust_log: str = "info"

    @dataclass(frozen=True)
    class AutoQuorumClientConfig:
        location: str
        server_id: int
        server_address: str
        requests: list[RequestInterval]
        kill_signal_sec: int | None
        summary_filepath: str
        output_filepath: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        aq_config = self.autoquorum_client_config
        if aq_config.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {aq_config.server_id}. It must be greater than 0."
            )

        if aq_config.kill_signal_sec:
            if aq_config.kill_signal_sec < 0:
                raise ValueError(
                    f"Kill signal {aq_config.kill_signal_sec} must be non-negative"
                )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def update_autoquorum_config(self, **kwargs) -> ClientConfig:
        new_op_config = replace(self.autoquorum_client_config, **kwargs)
        new_config = replace(self, autoquorum_client_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_client_toml(self) -> str:
        client_toml_str = toml.dumps(asdict(self.autoquorum_client_config))
        return client_toml_str


class ReadStrategy(Enum):
    ReadAsWrite = "ReadAsWrite"
    QuorumRead = "QuorumRead"
    BallotRead = "BallotRead"


@dataclass(frozen=True)
class FlexibleQuorum:
    read_quorum_size: int
    write_quorum_size: int


@dataclass(frozen=True)
class RequestInterval:
    duration_sec: int
    requests_per_sec: int
    read_ratio: float
