import signal
import subprocess
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Generic, Protocol, TypeVar

from .gcp_cluster import GcpCluster, InstanceConfig
from .gcp_ssh_client import GcpClusterSSHClient


class ServerConfigProtocol(Protocol):
    instance_config: InstanceConfig
    output_dir: str
    kill_command: str

    def update_config(self, **kwargs) -> "ServerConfigProtocol": ...


TServerConfig = TypeVar("TServerConfig", bound=ServerConfigProtocol)


class ClientConfigProtocol(Protocol):
    instance_config: InstanceConfig
    output_dir: str
    kill_command: str

    def update_config(self, **kwargs) -> "ClientConfigProtocol": ...


TClientConfig = TypeVar("TClientConfig", bound=ServerConfigProtocol)


TClusterConfig = TypeVar("TClusterConfig", bound="ClusterConfigProtocol")


class ClusterConfigProtocol(Protocol[TServerConfig, TClientConfig]):
    server_configs: dict[int, TServerConfig]
    client_configs: dict[int, TClientConfig]

    def update_config(self: TClusterConfig, **kwargs) -> TClusterConfig: ...
    def generate_cluster_json(self) -> str: ...


class ClientServerCluster(ABC, Generic[TClusterConfig]):
    """
    Abstract base class for managing a client-server cluster on Google Cloud Platform (GCP).
    This class provides functionality for configuring, starting, stopping, and monitoring
    a distributed cluster of client and server instances. It also supports updating
    configurations dynamically and retrieving logs from remote instances.
    """

    _gcp_cluster: GcpCluster
    _gcp_ssh_client: GcpClusterSSHClient
    _cluster_config: TClusterConfig

    @abstractmethod
    def _start_client_command(self, client_id: int, pull_image: bool = False) -> str:
        """Generate the SSH command to start a client on the remote instance."""
        pass

    @abstractmethod
    def _start_server_command(self, server_id: int, pull_image: bool = False) -> str:
        """Generate the SSH command to start a server on the remote instance."""
        pass

    def __init__(self, project_id: str, cluster_config: TClusterConfig):
        self._cluster_config = cluster_config
        # Collect instance configurations from both server and client configs.
        instance_configs = [
            c.instance_config for c in cluster_config.server_configs.values()
        ]
        instance_configs.extend(
            [c.instance_config for c in cluster_config.client_configs.values()]
        )
        self._gcp_cluster = GcpCluster(project_id, instance_configs)
        self._gcp_ssh_client = GcpClusterSSHClient(self._gcp_cluster)
        signal.signal(signal.SIGINT, self._cleanup_handler)
        signal.signal(signal.SIGTERM, self._cleanup_handler)

    def run(self, logs_directory: Path, pull_images: bool = False):
        """
        Starts servers and clients but only waits for client processes to exit before
        killing remote processes and pulling logs.
        """
        _ = self._start_servers(pull_images=pull_images)
        client_process_ids = self._start_clients(pull_images=pull_images)
        clients_finished = self._gcp_ssh_client.await_processes_concurrent(
            client_process_ids
        )
        if clients_finished:
            self._stop_servers_and_clear_clients()
        else:
            self._stop_servers_and_clients()
        self._get_logs(logs_directory)
        self._create_experiment_file(logs_directory)

    def shutdown(self):
        instance_names = [
            c.instance_config.name for c in self._cluster_config.server_configs.values()
        ]
        client_names = [
            c.instance_config.name for c in self._cluster_config.client_configs.values()
        ]
        instance_names.extend(client_names)
        self._gcp_cluster.shutdown_instances(instance_names)

    def change_cluster_config(self, **kwargs):
        self._cluster_config = self._cluster_config.update_config(**kwargs)

    def change_server_config(self, server_id: int, **kwargs):
        server_config = self._cluster_config.server_configs[server_id]
        self._cluster_config.server_configs[server_id] = server_config.update_config(
            **kwargs
        )

    def change_client_config(self, client_id: int, **kwargs):
        client_config = self._cluster_config.client_configs[client_id]
        self._cluster_config.client_configs[client_id] = client_config.update_config(
            **kwargs
        )

    def _start_servers(self, pull_images: bool = False) -> list[str]:
        process_ids = []
        for id, config in self._cluster_config.server_configs.items():
            process_id = f"server-{id}"
            instance_name = config.instance_config.name
            ssh_command = self._start_server_command(id, pull_images)
            self._gcp_ssh_client.start_process(process_id, instance_name, ssh_command)
            process_ids.append(process_id)
        return process_ids

    def _start_clients(self, pull_images: bool = False) -> list[str]:
        process_ids = []
        for id, config in self._cluster_config.client_configs.items():
            process_id = f"client-{id}"
            instance_name = config.instance_config.name
            ssh_command = self._start_client_command(id, pull_images)
            self._gcp_ssh_client.start_process(process_id, instance_name, ssh_command)
            process_ids.append(process_id)
        return process_ids

    def _stop_servers_and_clear_clients(self):
        print("Shutting down servers...")
        kill_processes = []
        for id, config in self._cluster_config.server_configs.items():
            instance_name = config.instance_config.name
            self._gcp_ssh_client.stop_process(f"server-{id}")
            kill_process_id = f"kill-server-{id}"
            self._gcp_ssh_client.start_process(
                kill_process_id,
                instance_name,
                config.kill_command,
            )
            kill_processes.append(kill_process_id)
        for id in self._cluster_config.client_configs.keys():
            self._gcp_ssh_client.clear_process(f"client-{id}")
        self._gcp_ssh_client.await_processes(kill_processes)

    def _stop_servers_and_clients(self):
        print("Shutting down servers and clients...")
        kill_processes = []
        for id, config in self._cluster_config.server_configs.items():
            self._gcp_ssh_client.stop_process(f"server-{id}")
            kill_process_id = f"kill-server-{id}"
            self._gcp_ssh_client.start_process(
                kill_process_id,
                config.instance_config.name,
                config.kill_command,
            )
            kill_processes.append(kill_process_id)
        for id, config in self._cluster_config.client_configs.items():
            self._gcp_ssh_client.stop_process(f"client-{id}")
            kill_process_id = f"kill-client-{id}"
            self._gcp_ssh_client.start_process(
                kill_process_id,
                config.instance_config.name,
                config.kill_command,
            )
            kill_processes.append(kill_process_id)
        self._gcp_ssh_client.await_processes(kill_processes)

    def _get_logs(self, dest_directory: Path):
        subprocess.run(["mkdir", "-p", str(dest_directory)])
        processes = []
        for config in self._cluster_config.server_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, config.output_dir, dest_directory
            )
            processes.append(scp_process)
        for config in self._cluster_config.client_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, config.output_dir, dest_directory
            )
            processes.append(scp_process)
        successes = 0
        for process in processes:
            process.wait()
            if process.returncode == 0:
                successes += 1
        print(f"Collected logs from {successes} instances")

    def _create_experiment_file(self, dest_directory: Path):
        run_json = self._cluster_config.generate_cluster_json()
        with open(dest_directory / "experiment-summary.json", "w") as experiment_file:
            experiment_file.write(run_json)

    def _cleanup_handler(self, signum, frame):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        self._stop_servers_and_clients()
        sys.exit(0)
