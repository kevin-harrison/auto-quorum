from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace

import toml

from clusters.autoquorum_configs import RequestInterval
from clusters.base_cluster import ClusterConfigProtocol, ServerConfigProtocol

from .gcp_cluster import InstanceConfig


@dataclass(frozen=True)
class ClusterConfig(ClusterConfigProtocol):
    etcd_cluster_config: EtcdClusterConfig
    server_configs: dict[int, ServerConfig]
    client_configs: dict[int, ClientConfig]
    client_image: str

    @dataclass(frozen=True)
    class EtcdClusterConfig:
        initial_leader: int

    def __post_init__(self):
        self.validate()

    def validate(self):
        aq_config = self.etcd_cluster_config
        for client_id in self.client_configs.keys():
            if client_id not in self.server_configs.keys():
                raise ValueError(f"Client {client_id} has no server to connect to")

        if aq_config.initial_leader not in self.server_configs.keys():
            raise ValueError(
                f"Initial leader {aq_config.initial_leader} must be one of the server nodes"
            )

    def update_config(self, **kwargs) -> ClusterConfig:
        new_op_config = replace(self.etcd_cluster_config, **kwargs)
        new_config = replace(self, etcd_cluster_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_cluster_json(self) -> str:
        cluster_dict = asdict(self)
        for node in self.server_configs:
            cluster_dict["server_configs"][node]["experiment_output"] = None
        for node, config in self.client_configs.items():
            cluster_dict["client_configs"][node][
                "experiment_output"
            ] = config.etcd_client_config.output_filepath
        return json.dumps(cluster_dict, indent=4)


@dataclass(frozen=True)
class ServerConfig(ServerConfigProtocol):
    instance_config: InstanceConfig
    etcd_server_config: EtcdServerConfig
    output_dir: str
    kill_command: str
    server_address: str
    run_script: str

    @dataclass(frozen=True)
    class EtcdServerConfig:
        name: str
        advertise_client_urls: str
        initial_advertise_peer_urls: str
        initial_cluster: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        pass

    def update_config(self, **kwargs) -> ServerConfig:
        new_op_config = replace(self.etcd_server_config, **kwargs)
        new_config = replace(self, etcd_server_config=new_op_config)
        new_config.validate()
        return new_config


@dataclass(frozen=True)
class ClientConfig:
    instance_config: InstanceConfig
    etcd_client_config: EtcdClientConfig
    output_dir: str
    kill_command: str
    run_script: str
    rust_log: str = "info"

    @dataclass(frozen=True)
    class EtcdClientConfig:
        location: str
        server_id: int
        server_name: str
        server_address: str
        requests: list[RequestInterval]
        kill_signal_sec: int | None
        initial_leader: str
        summary_filepath: str
        output_filepath: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        etcd_config = self.etcd_client_config
        if etcd_config.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {etcd_config.server_id}. It must be greater than 0."
            )
        if etcd_config.kill_signal_sec:
            if etcd_config.kill_signal_sec < 0:
                raise ValueError(
                    f"Kill signal {etcd_config.kill_signal_sec} must be non-negative"
                )
        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def update_config(self, **kwargs) -> ClientConfig:
        new_op_config = replace(self.etcd_client_config, **kwargs)
        new_config = replace(self, etcd_client_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_client_toml(self) -> str:
        client_toml_str = toml.dumps(asdict(self.etcd_client_config))
        return client_toml_str
