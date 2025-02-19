import subprocess
import time
from itertools import chain
from pathlib import Path
from typing import KeysView

from autoquorum_configs import *
from gcp_cluster import GcpCluster, InstanceConfig


class AutoQuorumCluster:
    """
    Orchestration class for managing a AutoQuorum cluster on GCP.

    This class automates the setup, deployment, and management of AutoQuorum servers and clients
    on Google Cloud Platform (GCP) instances. It abstracts the steps required to push Docker images,
    start instances, configure and launch AutoQuorum containers, and manage logs and shutdown operations.

    Deployment Steps:
    1. Push AutoQuorum server and client Docker images to GCR (Google Cloud Registry).
       See `../build_scripts/push-server-image.sh` and `../build_scripts/push-client-image.sh` for details.
    2. `__init__` Initializes the cluster by creating GCP instances (using the GcpCluster class) for AutoQuorum servers and clients.
       The instances will run startup scripts (passed via ClusterConfig) to configure Docker for the gcloud OS login user.
    3. Use `start_servers()` and `start_clients()` to SSH into instances, pass configuration files,
       and run Docker containers from the artifact registry.
    4. Use `await_cluster()` to wait for the SSH processes to finish.
    5. Use `get_logs()` to pull logs from the GCP instances.
    6. Use `shutdown()` to shut down the GCP instances (or leave them running for reuse).
    """

    _server_processes: dict[int, subprocess.Popen]
    _client_processes: dict[int, subprocess.Popen]
    _cluster_config: ClusterConfig
    _gcp_cluster: GcpCluster

    def __init__(self, project_id: str, cluster_config: ClusterConfig):
        self._server_processes = {}
        self._client_processes = {}
        self._cluster_config = cluster_config
        instance_configs = [
            c.instance_config for c in cluster_config.server_configs.values()
        ]
        instance_configs.extend(
            [c.instance_config for c in cluster_config.client_configs.values()]
        )
        self._gcp_cluster = GcpCluster(project_id, instance_configs)

    def start_servers(
        self, servers: list[int] | None = None, pull_images: bool = False
    ):
        if servers is None:
            print("Starting all servers")
            for server_id in self._cluster_config.server_configs.keys():
                self.start_server(server_id, pull_images)
        else:
            print(f"Starting servers {servers}")
            for server_id in servers:
                self.start_server(server_id, pull_images)

    def start_server(self, server_id: int, pull_image: bool = False):
        server_config = self._get_server_config(server_id)
        current_server_process = self._server_processes.get(server_id)
        if current_server_process is not None:
            current_server_process.terminate()
        start_command = self._start_server_command(server_id, pull_image=pull_image)
        server_process = self._gcp_cluster.ssh_command(
            server_config.instance_config.name, start_command
        )
        self._server_processes[server_id] = server_process

    def stop_server(self, server_id: int) -> int | None:
        _ = self._get_server_config(server_id)
        current_server_process = self._server_processes.pop(server_id, None)
        if current_server_process is not None:
            current_server_process.terminate()
            return server_id
        return None

    def stop_servers(self) -> list[int]:
        stopped_processes = []
        for server_id in self._cluster_config.server_configs.keys():
            if self.stop_server(server_id) is not None:
                stopped_processes.append(server_id)
        return stopped_processes

    def await_servers(self):
        print(f"Awaiting servers...")
        for server_process in self._server_processes.values():
            server_process.wait(timeout=600)
        self._server_processes.clear()

    def start_clients(
        self, clients: list[int] | None = None, pull_images: bool = False
    ):
        if clients is None:
            print("Starting all clients")
            for client_id in self._cluster_config.client_configs.keys():
                self.start_client(client_id, pull_images)
        else:
            print(f"Starting clients {clients}")
            for client_id in clients:
                self.start_client(client_id, pull_images)

    def start_client(self, client_id: int, pull_image: bool = False):
        client_config = self._get_client_config(client_id)
        current_client_process = self._client_processes.pop(client_id, None)
        if current_client_process is not None:
            current_client_process.terminate()
        start_command = self._start_client_command(client_id, pull_image=pull_image)
        client_process = self._gcp_cluster.ssh_command(
            client_config.instance_config.name, start_command
        )
        self._client_processes[client_id] = client_process

    def stop_client(self, client_id: int) -> int | None:
        _ = self._get_client_config(client_id)
        current_client_process = self._client_processes.pop(client_id, None)
        if current_client_process is not None:
            current_client_process.terminate()
            return client_id
        return None

    def stop_clients(self) -> list[int]:
        stopped_processes = []
        for client_id in self._cluster_config.client_configs.keys():
            if self.stop_client(client_id) is not None:
                stopped_processes.append(client_id)
        return stopped_processes

    def await_cluster(self, timeout: int | None = None, partitioned_node: int | None = None):
        """
        Waits for client and server processes to finish, retrying if SSH connection fails. Aborts processes if timeout (in seconds)
        is reached.

        This method retries client and server SSH connections up to 3 times if an instance SSH connection fails. This is necessary
        because there is an undefined delay between when an instance is created and when it becomes SSH-able. If any instance fails
        to connect, all the connections are retried.
        """
        print(f"Awaiting cluster...")
        retries = 0
        ticks = 0
        while True:
            a_process_failed = False
            a_process_is_running = False
            for client_id, client_process in self._client_processes.items():
                return_code = client_process.poll()
                if return_code is None:
                    a_process_is_running = True
                elif return_code == 255:
                    a_process_failed = True
            for server_id, server_process in self._server_processes.items():
                return_code = server_process.poll()
                if return_code is None:
                    # TODO: A server sometimes misses a kill message from the client
                    # and thus never terminates
                    if partitioned_node == server_id:
                        continue
                    a_process_is_running = True
                elif return_code == 255:
                    a_process_failed = True

            # all_processes = chain(
            #     self._client_processes.values(), self._server_processes.values()
            # )
            # for process in all_processes:
            #     return_code = process.poll()
            #     if return_code is None:
            #         a_process_is_running = True
            #     elif return_code == 255:
            #         a_process_failed = True

            if a_process_failed:
                stopped_clients = self.stop_clients()
                stopped_servers = self.stop_servers()
                if retries < 3:
                    print(f"Retrying client and server SSH connections...")
                    time.sleep(10)
                    retries += 1
                    self.start_servers(stopped_servers)
                    self.start_clients(stopped_clients)
                else:
                    print("Failed SSH 3 times, aborting cluster.")
                    break
            elif a_process_is_running:
                time.sleep(1)
                ticks += 1
                if timeout is not None and ticks > timeout:
                    print("Timeout reached, stopping all processes.")
                    self.stop_clients()
                    self.stop_servers()
                    break
            else:
                print("Cluster finished successfully.")
                break
        self._client_processes.clear()
        self._server_processes.clear()

    def run(self, logs_directory: Path, pull_images: bool = False, partitioned_node: int | None = None):
        self.start_servers(pull_images=pull_images)
        self.start_clients(pull_images=pull_images)
        self.await_cluster(partitioned_node=partitioned_node)
        self.get_logs(logs_directory)

    def shutdown(self):
        instance_names = [
            c.instance_config.name for c in self._cluster_config.server_configs.values()
        ]
        client_names = [
            c.instance_config.name for c in self._cluster_config.client_configs.values()
        ]
        instance_names.extend(client_names)
        self.stop_servers()
        self._gcp_cluster.shutdown_instances(instance_names)

    def get_logs(self, dest_directory: Path):
        # Make sure destination directory exists
        subprocess.run(["mkdir", "-p", dest_directory])
        instance_results_dir = "./results"
        processes = []
        for config in self._cluster_config.server_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, instance_results_dir, dest_directory
            )
            processes.append(scp_process)
        for config in self._cluster_config.client_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, instance_results_dir, dest_directory
            )
            processes.append(scp_process)
        successes = 0
        for process in processes:
            process.wait()
            if process.returncode == 0:
                successes += 1
        print(f"Collected logs from {successes} instances")

    def change_cluster_config(self, **kwargs):
        self._cluster_config = self._cluster_config.with_updated(**kwargs)

    def change_server_config(self, server_id: int, **kwargs):
        server_config = self._get_server_config(server_id)
        self._cluster_config.server_configs[server_id] = server_config.with_updated(
            **kwargs
        )

    def change_client_config(self, client_id: int, **kwargs):
        client_config = self._get_client_config(client_id)
        self._cluster_config.client_configs[client_id] = client_config.with_updated(
            **kwargs
        )

    def get_nodes(self) -> KeysView[int]:
        return self._cluster_config.server_configs.keys()

    def _get_server_config(self, server_id: int) -> ServerConfig:
        server_config = self._cluster_config.server_configs.get(server_id)
        if server_config is None:
            raise ValueError(f"Server {server_id} doesn't exist")
        return server_config

    def _get_client_config(self, client_id: int) -> ClientConfig:
        client_config = self._cluster_config.client_configs.get(client_id)
        if client_config is None:
            raise ValueError(f"Client {client_id} doesn't exist")
        return client_config

    def _start_server_command(self, server_id: int, pull_image: bool = False) -> str:
        config = self._get_server_config(server_id)
        container_name = "server"
        image_path = self._cluster_config.multileader_server_image if self._cluster_config.multileader else self._cluster_config.server_image
        instance_config_location = "~/server-config.toml"
        container_config_location = "/home/$(whoami)/server-config.toml"
        instance_output_dir = "./results"
        container_output_dir = "/app"
        stderr_pipe = f"{instance_output_dir}/xerr-server-{config.server_id}.log"
        server_config_toml = config.generate_server_toml(self._cluster_config)

        # pull_command = f"docker pull {container_image_location} > /dev/null"
        pull_command = f"docker pull {image_path}"
        kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
        gen_config_command = f"mkdir -p {instance_output_dir} && echo -e '{server_config_toml}' > {instance_config_location}"
        docker_command = f"""docker run \\
            --name {container_name} \\
            -p 800{config.server_id}:800{config.server_id} \\
            --env RUST_LOG={config.rust_log} \\
            --env CONFIG_FILE="{container_config_location}" \\
            -v {instance_config_location}:{container_config_location} \\
            -v {instance_output_dir}:{container_output_dir} \\
            --rm \\
            "{image_path}" \\
            2> {stderr_pipe}"""
        if pull_image:
            full_command = f"{kill_prev_container_command}; {pull_command}; {gen_config_command} && {docker_command}"
        else:
            # Add a sleep to help avoid connecting to any currently shutting down servers.
            full_command = f"{kill_prev_container_command}; sleep 1; {gen_config_command} && {docker_command}"
        return full_command

    def _start_client_command(self, client_id: int, pull_image: bool = False) -> str:
        config = self._get_client_config(client_id)
        container_name = "client"
        image_path = self._cluster_config.client_image
        instance_config_location = "~/client-config.toml"
        container_config_location = f"/home/$(whoami)/client-config.toml"
        instance_output_dir = "./results"
        container_output_dir = "/app"
        client_config_toml = config.generate_client_toml(self._cluster_config)

        # pull_command = f"docker pull gcr.io/{container_image_location} > /dev/null"
        # kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
        pull_command = f"docker pull {image_path}"
        kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
        gen_config_command = f"mkdir -p {instance_output_dir} && echo -e '{client_config_toml}' > {instance_config_location}"
        docker_command = f"""docker run \\
        --name={container_name} \\
        --rm \\
        --env RUST_LOG={config.rust_log} \\
        --env CONFIG_FILE={container_config_location} \\
        -v {instance_config_location}:{container_config_location} \\
        -v {instance_output_dir}:{container_output_dir} \\
        {image_path}"""
        if pull_image:
            full_command = f"{kill_prev_container_command}; {pull_command}; {gen_config_command} && {docker_command}"
        else:
            # Add a sleep to help avoid connecting to any currently shutting down servers.
            full_command = f"{kill_prev_container_command}; sleep 1; {gen_config_command} && {docker_command}"
        return full_command


class AutoQuorumClusterBuilder:
    """
    Builder class for defining and validating configurations to start a AutoQuorumCluster.
    """

    def __init__(
        self, cluster_name: str, project_id: str = "my-project-1499979282244"
    ) -> None:
        self.cluster_name = cluster_name
        self._project_id = project_id
        self._service_account = f"deployment@{project_id}.iam.gserviceaccount.com"
        self._gcloud_ssh_user = GcpCluster.get_oslogin_username()
        self._server_docker_image_path = f"gcr.io/{project_id}/autoquorum_server"
        self._multileader_server_docker_image_path = f"gcr.io/{project_id}/autoquorum_multileader-server"
        self._client_docker_image_path = f"gcr.io/{project_id}/autoquorum_client"
        self._server_configs: dict[int, ServerConfig] = {}
        self._client_configs: dict[int, ClientConfig] = {}
        # Cluster-wide settings
        self._initial_leader: int | None = None
        self._initial_quorum: FlexibleQuorum | None = None
        self._optimize_setting: bool | None = None
        self._optimize_threshold: float | None = None
        self._initial_read_strategy: list[ReadStrategy] | None = None
        self._multileader: bool = False

    def server(
        self,
        server_id: int,
        zone: str,
        machine_type: str = "e2-standard-8",
        rust_log: str = "info",
    ):
        if server_id in self._server_configs.keys():
            raise ValueError(f"Server {server_id} already exists")
        instance_config = InstanceConfig(
            f"{self.cluster_name}-server-{server_id}",
            zone,
            machine_type,
            self._docker_startup_script(self._server_docker_image_path, self._multileader_server_docker_image_path),
            dns_name=f"{self.cluster_name}-server-{server_id}",
            service_account=self._service_account,
        )
        server_config = ServerConfig(
            instance_config=instance_config,
            server_id=server_id,
            num_clients=0,
            output_filepath=f"server-{server_id}.json",
            rust_log=rust_log,
        )
        self._server_configs[server_id] = server_config
        return self

    def client(
        self,
        server_id: int,
        zone: str,
        requests: list[RequestInterval] = [],
        kill_signal_sec: int | None = None,
        next_server: int | None = None,
        machine_type: str = "e2-standard-2",
        rust_log: str = "info",
    ):
        if server_id in self._client_configs.keys():
            raise ValueError(f"Client {server_id} already exists")
        instance_config = InstanceConfig(
            f"{self.cluster_name}-client-{server_id}",
            zone,
            machine_type,
            self._docker_startup_script(self._client_docker_image_path),
            service_account=self._service_account,
        )
        client_config = ClientConfig(
            instance_config=instance_config,
            server_id=server_id,
            requests=requests,
            kill_signal_sec=kill_signal_sec,
            next_server=next_server,
            summary_filepath=f"client-{server_id}.json",
            output_filepath=f"client-{server_id}.csv",
            rust_log=rust_log,
        )
        self._client_configs[server_id] = client_config
        return self

    def initial_leader(self, initial_leader: int):
        self._initial_leader = initial_leader
        return self

    def initial_quorum(self, flex_quorum: FlexibleQuorum):
        self._initial_quorum = flex_quorum
        return self

    def optimize_setting(self, optimize: bool):
        self._optimize_setting = optimize
        return self

    def initial_read_strategy(self, initial_read_strategy: list[ReadStrategy]):
        self._initial_read_strategy = initial_read_strategy
        return self

    def optimize_threshold(self, threshold: float):
        self._optimize_threshold = threshold
        return self
    
    def multileader(self, multileader: bool):
        self._multileader = multileader
        return self

    def build(self) -> AutoQuorumCluster:
        # Edit server configs based on cluster-wide settings
        for server_id, server_config in self._server_configs.items():
            client_configs = self._client_configs.values()
            server_id_matches = sum(
                1 for _ in filter(lambda c: c.server_id == server_id, client_configs)
            )
            next_id_matches = sum(
                1 for _ in filter(lambda c: c.next_server == server_id, client_configs)
            )
            total_matches = server_id_matches + next_id_matches
            self._server_configs[server_id] = replace(server_config, num_clients=total_matches)

        if self._initial_leader is None:
            raise ValueError("Need to set cluster's initial leader")

        cluster_config = ClusterConfig(
            cluster_name=self.cluster_name,
            nodes=sorted(self._server_configs.keys()),
            initial_leader=self._initial_leader,
            initial_flexible_quorum=self._initial_quorum,
            optimize=self._optimize_setting,
            optimize_threshold=self._optimize_threshold,
            initial_read_strat=self._initial_read_strategy,
            server_configs=self._server_configs,
            client_configs=self._client_configs,
            multileader=self._multileader,
            client_image=self._client_docker_image_path,
            server_image=self._server_docker_image_path,
            multileader_server_image=self._multileader_server_docker_image_path,
        )
        return AutoQuorumCluster(self._project_id, cluster_config)

    def _docker_startup_script(self,
        image_path: str,
        alternative_image_path: str | None = None
    ) -> str:
        """
        Generates the startup script for a AutoQuorum client on a GCP instance.

        This script is executed during instance creation and configures Docker to use GCR.
        For debugging, SSH into the instance and run `sudo journalctl -u google-startup-scripts.service`.
        """
        user = self._gcloud_ssh_user
        pull_alt_image_command = f"sudo -u {user} docker pull \"{alternative_image_path}\"" if alternative_image_path else ""
        return f"""#! /bin/bash
# Ensure OS login user is setup
useradd -m {user}
mkdir -p /home/{user}
chown {user}:{user} /home/{user}

# Configure Docker credentials for the user
sudo -u {user} docker-credential-gcr configure-docker --registries=gcr.io
sudo -u {user} echo "https://gcr.io" | docker-credential-gcr get
sudo groupadd docker
sudo usermod -aG docker {user}

# Pull the container as user
sudo -u {user} docker pull "{image_path}"
{pull_alt_image_command}
"""
